"""
FINEP NLP Topic Modelling — Core Pipeline Logic

Called by finep_nlp_topic DAG:
  Task 1 — embed_and_cluster : fetch, embed, BERTopic, write to PG, save .npy
  Task 2 — load_neo4j_nodes  : MERGE ProjetoFinep with topic/UMAP properties
  Task 3 — load_neo4j_edges  : CREATE SIMILAR_TO from k-NN graph, clean up .npy

Artifact files (ephemeral, shared between tasks via shared filesystem):
  {ARTIFACTS_DIR}/embeddings_{run_id}.npy    — float32 (n, embedding_dim)
  {ARTIFACTS_DIR}/nn_indices_{run_id}.npy    — int64   (n, k+1)
  {ARTIFACTS_DIR}/nn_distances_{run_id}.npy  — float32 (n, k+1)
  {ARTIFACTS_DIR}/proj_ids_{run_id}.npy      — int64   (n,)
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path

import numpy as np
import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)

# ── Hyperparameter defaults ───────────────────────────────────────────────────
EMBEDDING_MODEL           = "paraphrase-multilingual-mpnet-base-v2"
UMAP_N_NEIGHBORS          = 15
UMAP_N_COMPONENTS_CLUSTER = 5
SIM_THRESHOLD             = 0.70
K_NEIGHBORS               = 5

ARTIFACTS_DIR = Path(os.getenv("AIRFLOW_HOME", "/opt/airflow")) / "data" / "finep" / "nlp"

# Portuguese stopwords — filters prepositions/articles/conjunctions from c-TF-IDF
PT_STOPWORDS = [
    "de", "da", "do", "das", "dos", "em", "no", "na", "nos", "nas",
    "a", "o", "as", "os", "um", "uma", "uns", "umas",
    "para", "por", "pelo", "pela", "pelos", "pelas",
    "com", "sem", "sob", "sobre", "entre", "até", "após",
    "e", "ou", "mas", "que", "se", "como", "mais", "ao", "à",
    "aos", "às", "num", "numa", "nuns", "numas",
    "este", "esta", "estes", "estas", "esse", "essa", "esses", "essas",
    "aquele", "aquela", "aqueles", "aquelas", "ele", "ela", "eles", "elas",
    "eu", "tu", "nós", "vós", "me", "te", "nos", "vos",
    "meu", "minha", "meus", "minhas", "teu", "tua", "teus", "tuas",
    "seu", "sua", "seus", "suas", "nosso", "nossa", "nossos", "nossas",
    "não", "também", "já", "ainda", "quando", "onde", "porque", "pois",
    "foi", "são", "ser", "ter", "tem", "teve", "há", "via",
    # domain-ubiquitous terms appearing in virtually every FINEP topic
    "projeto", "projetos", "desenvolvimento", "nacional",
]

NEO4J_BATCH_NODES = 500
NEO4J_BATCH_EDGES = 1000


# ── Connections ───────────────────────────────────────────────────────────────

def _get_pg_conn():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "osint_metadata"),
        user=os.getenv("POSTGRES_USER", "osint_admin"),
        password=os.getenv("POSTGRES_PASSWORD", ""),
    )


def _get_neo4j_driver():
    from neo4j import GraphDatabase
    return GraphDatabase.driver(
        os.getenv("NEO4J_URI", "bolt://neo4j:7687"),
        auth=(os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASSWORD", "")),
    )


# ── Task 1: embed_and_cluster ─────────────────────────────────────────────────

def embed_and_cluster(
    dag_run_id: str,
    embedding_model: str = EMBEDDING_MODEL,
    umap_n_neighbors: int = UMAP_N_NEIGHBORS,
    umap_n_components_cluster: int = UMAP_N_COMPONENTS_CLUSTER,
    sim_threshold: float = SIM_THRESHOLD,
    k_neighbors: int = K_NEIGHBORS,
) -> dict:
    """
    1. Fetch rows from finep.projetos_operacao_direta
       (resumo_publicavel with fallback to titulo)
    2. Embed with SentenceTransformer
    3. Run BERTopic (CountVectorizer + ClassTfidfTransformer + KeyBERTInspired)
    4. Compute 2D UMAP projection and k-NN graph
    5. Persist to PG: nlp_model_run, nlp_topic, nlp_document_topic
    6. Save .npy artifact files for downstream Neo4j tasks

    Returns a dict pushed to XCom with run_id and artifact_dir.
    Idempotent: deletes any existing run for this dag_run_id before re-inserting.
    """
    import matplotlib
    matplotlib.use("Agg")

    from sentence_transformers import SentenceTransformer
    from bertopic import BERTopic
    from bertopic.vectorizers import ClassTfidfTransformer
    from bertopic.representation import KeyBERTInspired
    from umap import UMAP
    from hdbscan import HDBSCAN
    from sklearn.feature_extraction.text import CountVectorizer
    from sklearn.neighbors import NearestNeighbors

    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)

    # ── 1. Fetch ──────────────────────────────────────────────────────────────
    logger.info("Fetching rows from finep.projetos_operacao_direta...")
    conn = _get_pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, titulo, resumo_publicavel
                FROM finep.projetos_operacao_direta
                WHERE titulo IS NOT NULL AND titulo <> ''
                ORDER BY id
            """)
            rows = cur.fetchall()
    finally:
        conn.close()

    if not rows:
        raise ValueError("No rows found in finep.projetos_operacao_direta")

    proj_ids    = np.array([r[0] for r in rows], dtype=np.int64)
    titulos_raw = [r[1] for r in rows]
    resumos_raw = [r[2] for r in rows]

    # resumo_publicavel when non-empty, else titulo
    texts_raw = [
        (r.strip() if r and r.strip() else t)
        for r, t in zip(resumos_raw, titulos_raw)
    ]
    texts = [t.title() for t in texts_raw]
    n = len(texts)
    n_resumo = sum(1 for r in resumos_raw if r and r.strip())
    logger.info(
        f"Fetched {n} rows  "
        f"({n_resumo} with resumo_publicavel, {n - n_resumo} fallback to titulo)"
    )

    # ── 2. Embed ──────────────────────────────────────────────────────────────
    logger.info(f"Embedding with {embedding_model}...")
    st_model   = SentenceTransformer(embedding_model)
    embeddings = st_model.encode(texts, show_progress_bar=True, batch_size=32)
    embeddings = embeddings.astype(np.float32)

    # ── 3. BERTopic ───────────────────────────────────────────────────────────
    logger.info("Running BERTopic...")
    hdbscan_min_cluster_size = max(5, n // 100)

    vectorizer = CountVectorizer(
        stop_words=PT_STOPWORDS,
        ngram_range=(1, 2),
        min_df=3,
    )
    ctfidf         = ClassTfidfTransformer(reduce_frequent_words=True)
    representation = KeyBERTInspired()

    umap_topic = UMAP(
        n_components=umap_n_components_cluster,
        random_state=42, min_dist=0.0,
        n_neighbors=umap_n_neighbors,
    )
    umap_viz = UMAP(
        n_components=2,
        random_state=42, min_dist=0.1,
        n_neighbors=umap_n_neighbors,
    )
    hdbscan_model = HDBSCAN(
        min_cluster_size=hdbscan_min_cluster_size,
        metric="euclidean",
        prediction_data=True,
    )
    topic_model = BERTopic(
        embedding_model=st_model,
        umap_model=umap_topic,
        hdbscan_model=hdbscan_model,
        vectorizer_model=vectorizer,
        ctfidf_model=ctfidf,
        representation_model=representation,
        calculate_probabilities=False,
        verbose=True,
    )
    topics, _ = topic_model.fit_transform(texts, embeddings=embeddings)
    topics_arr = np.array(topics, dtype=np.int16)

    logger.info("Computing 2D UMAP for visualization...")
    pos2d = np.array(umap_viz.fit_transform(embeddings), dtype=np.float32)  # type: ignore[arg-type]

    topic_info = topic_model.get_topic_info()
    n_topics   = int(len(topic_info[topic_info["Topic"] >= 0]))
    n_outliers = int((topics_arr == -1).sum())
    logger.info(f"BERTopic done: {n_topics} topics, {n_outliers} outliers")

    # ── 4. k-NN graph ─────────────────────────────────────────────────────────
    logger.info(f"Building k-NN graph (k={k_neighbors}, threshold={sim_threshold})...")
    nn_model = NearestNeighbors(n_neighbors=k_neighbors + 1, metric="cosine", algorithm="brute")
    nn_model.fit(embeddings)
    nn_distances, nn_indices = nn_model.kneighbors(embeddings)

    # ── 5. Write to PostgreSQL ────────────────────────────────────────────────
    logger.info("Writing results to PostgreSQL...")
    conn = _get_pg_conn()
    try:
        with conn.cursor() as cur:
            # Idempotency: clear any existing run for this dag_run_id
            cur.execute(
                "DELETE FROM finep.nlp_model_run WHERE dag_run_id = %s",
                (dag_run_id,),
            )

            cur.execute(
                """
                INSERT INTO finep.nlp_model_run (
                    dag_run_id, embedding_model,
                    umap_n_neighbors, umap_n_components_cluster,
                    hdbscan_min_cluster_size,
                    sim_threshold, k_neighbors,
                    n_documents, n_topics, n_outliers
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    dag_run_id, embedding_model,
                    umap_n_neighbors, umap_n_components_cluster,
                    hdbscan_min_cluster_size,
                    sim_threshold, k_neighbors,
                    n, n_topics, n_outliers,
                ),
            )
            row = cur.fetchone()
            if row is None:
                raise RuntimeError("INSERT INTO nlp_model_run returned no row")
            run_id: int = row[0]
            logger.info(f"Created nlp_model_run id={run_id}")

            # Topics (skip outliers: topic_id == -1)
            topic_rows = []
            for _, row in topic_info.iterrows():
                t = int(row["Topic"])
                if t == -1:
                    continue
                kws: list[tuple[str, float]] = topic_model.get_topic(t) or []  # type: ignore[assignment]
                topic_rows.append((
                    run_id, t, int(row["Count"]),
                    json.dumps([[w, round(float(s), 6)] for w, s in kws]),
                ))
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO finep.nlp_topic (run_id, topic_id, document_count, keywords)
                VALUES %s
                """,
                topic_rows,
            )
            logger.info(f"Inserted {len(topic_rows)} topics")

            # Document-topic assignments + UMAP coords
            doc_rows = [
                (run_id, int(proj_ids[i]), int(topics_arr[i]),
                 float(pos2d[i, 0]), float(pos2d[i, 1]))
                for i in range(n)
            ]
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO finep.nlp_document_topic
                    (run_id, projeto_id, topic_id, umap_x, umap_y)
                VALUES %s
                """,
                doc_rows,
                page_size=2000,
            )
            logger.info(f"Inserted {n} document-topic assignments")

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    # ── 6. Save .npy artifacts ────────────────────────────────────────────────
    logger.info(f"Saving .npy artifacts to {ARTIFACTS_DIR}...")
    np.save(ARTIFACTS_DIR / f"embeddings_{run_id}.npy",   embeddings)
    np.save(ARTIFACTS_DIR / f"nn_indices_{run_id}.npy",   nn_indices.astype(np.int64))
    np.save(ARTIFACTS_DIR / f"nn_distances_{run_id}.npy", nn_distances.astype(np.float32))
    np.save(ARTIFACTS_DIR / f"proj_ids_{run_id}.npy",     proj_ids)
    logger.info("Artifacts saved")

    return {
        "run_id":        run_id,
        "artifact_dir":  str(ARTIFACTS_DIR),
        "n_documents":   n,
        "n_topics":      n_topics,
        "n_outliers":    n_outliers,
        "sim_threshold": sim_threshold,
        "k_neighbors":   k_neighbors,
    }


# ── Task 2: load_neo4j_nodes ──────────────────────────────────────────────────

def load_neo4j_nodes(run_id: int) -> dict:
    """
    Read nlp_document_topic JOIN projetos_operacao_direta from PG.
    MERGE ProjetoFinep nodes in Neo4j — sets all project properties plus
    topic_id, umap_x, umap_y, and nlp_run_id from the current run.
    Also creates PROPOSTO_POR and EXECUTADO_POR edges to existing Empresa nodes.
    """
    logger.info(f"Loading ProjetoFinep nodes for run_id={run_id}...")
    conn = _get_pg_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    p.id, p.titulo, p.contrato, p.status,
                    p.cnpj_proponente_norm, p.cnpj_executor_norm,
                    p.proponente, p.executor, p.municipio, p.uf,
                    p.instrumento,
                    CAST(p.valor_finep AS FLOAT) AS valor_finep,
                    dt.topic_id,
                    CAST(dt.umap_x AS FLOAT) AS umap_x,
                    CAST(dt.umap_y AS FLOAT) AS umap_y
                FROM finep.nlp_document_topic dt
                JOIN finep.projetos_operacao_direta p ON p.id = dt.projeto_id
                WHERE dt.run_id = %s
                ORDER BY p.id
                """,
                (run_id,),
            )
            rows = [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()

    logger.info(f"  {len(rows)} projects to MERGE into Neo4j")

    driver = _get_neo4j_driver()
    nodes_merged       = 0
    proposto_created   = 0
    executado_created  = 0

    try:
        # Batch MERGE ProjetoFinep nodes and set properties
        for i in range(0, len(rows), NEO4J_BATCH_NODES):
            batch = [{**r, "nlp_run_id": run_id} for r in rows[i : i + NEO4J_BATCH_NODES]]
            with driver.session() as session:
                result = session.run(
                    """
                    UNWIND $rows AS row
                    MERGE (p:ProjetoFinep {id: row.id})
                    SET
                        p.titulo               = row.titulo,
                        p.contrato             = row.contrato,
                        p.status               = row.status,
                        p.cnpj_proponente_norm = row.cnpj_proponente_norm,
                        p.cnpj_executor_norm   = row.cnpj_executor_norm,
                        p.proponente           = row.proponente,
                        p.executor             = row.executor,
                        p.municipio            = row.municipio,
                        p.uf                   = row.uf,
                        p.instrumento          = row.instrumento,
                        p.valor_finep          = row.valor_finep,
                        p.topic_id             = row.topic_id,
                        p.umap_x               = row.umap_x,
                        p.umap_y               = row.umap_y,
                        p.nlp_run_id           = row.nlp_run_id
                    RETURN count(p) AS merged
                    """,
                    rows=batch,
                )
                record = result.single()
                nodes_merged += int(record["merged"]) if record is not None else 0
        logger.info(f"  MERGEd {nodes_merged} ProjetoFinep nodes")

        # PROPOSTO_POR — link to existing Empresa by cnpj_basico (first 8 digits)
        for i in range(0, len(rows), NEO4J_BATCH_NODES):
            batch = [r for r in rows[i : i + NEO4J_BATCH_NODES] if r["cnpj_proponente_norm"]]
            if not batch:
                continue
            with driver.session() as session:
                result = session.run(
                    """
                    UNWIND $rows AS row
                    MATCH (p:ProjetoFinep {id: row.id})
                    OPTIONAL MATCH (e:Empresa {cnpj_basico: left(row.cnpj_proponente_norm, 8)})
                    WITH p, e WHERE e IS NOT NULL
                    MERGE (p)-[:PROPOSTO_POR]->(e)
                    RETURN count(*) AS created
                    """,
                    rows=batch,
                )
                record = result.single()
                proposto_created += int(record["created"]) if record is not None else 0

        # EXECUTADO_POR — skip when executor == proponente (same company)
        for i in range(0, len(rows), NEO4J_BATCH_NODES):
            batch = [
                r for r in rows[i : i + NEO4J_BATCH_NODES]
                if r["cnpj_executor_norm"]
                and r["cnpj_executor_norm"] != r["cnpj_proponente_norm"]
            ]
            if not batch:
                continue
            with driver.session() as session:
                result = session.run(
                    """
                    UNWIND $rows AS row
                    MATCH (p:ProjetoFinep {id: row.id})
                    OPTIONAL MATCH (e:Empresa {cnpj_basico: left(row.cnpj_executor_norm, 8)})
                    WITH p, e WHERE e IS NOT NULL
                    MERGE (p)-[:EXECUTADO_POR]->(e)
                    RETURN count(*) AS created
                    """,
                    rows=batch,
                )
                record = result.single()
                executado_created += int(record["created"]) if record is not None else 0

        logger.info(
            f"  PROPOSTO_POR: {proposto_created}  |  EXECUTADO_POR: {executado_created}"
        )
    finally:
        driver.close()

    return {
        "run_id":           run_id,
        "nodes_merged":     nodes_merged,
        "proposto_created": proposto_created,
        "executado_created":executado_created,
    }


# ── Task 3: load_neo4j_edges ──────────────────────────────────────────────────

def load_neo4j_edges(
    run_id: int,
    artifact_dir: str,
    sim_threshold: float = SIM_THRESHOLD,
    k_neighbors: int = K_NEIGHBORS,
) -> dict:
    """
    Load saved .npy arrays, build the filtered k-NN edge list, delete any
    pre-existing SIMILAR_TO relationships for this run (idempotency), then
    batch-CREATE all edges in Neo4j.  Cleans up artifact files on success.
    """
    artifact_path = Path(artifact_dir)

    nn_indices   = np.load(artifact_path / f"nn_indices_{run_id}.npy")
    nn_distances = np.load(artifact_path / f"nn_distances_{run_id}.npy")
    proj_ids     = np.load(artifact_path / f"proj_ids_{run_id}.npy")

    logger.info(f"Building edge list (threshold={sim_threshold}, k={k_neighbors})...")
    edges: list[dict] = []
    seen:  set[tuple] = set()

    for i, (nbrs, dists) in enumerate(zip(nn_indices, nn_distances)):
        for j, d in zip(nbrs[1:k_neighbors + 1], dists[1:k_neighbors + 1]):
            sim = float(1.0 - d)
            if sim < sim_threshold:
                continue
            # always store with smaller id as source for consistent direction
            a = int(min(proj_ids[i], proj_ids[j]))
            b = int(max(proj_ids[i], proj_ids[j]))
            key = (a, b)
            if key not in seen:
                seen.add(key)
                edges.append({"src_id": a, "dst_id": b, "cosine_similarity": round(sim, 4)})

    logger.info(f"  {len(edges)} edges above threshold")

    driver = _get_neo4j_driver()
    edges_created = 0
    try:
        # Delete old SIMILAR_TO for this run_id (safe for task retries)
        logger.info(f"  Deleting old SIMILAR_TO for run_id={run_id}...")
        with driver.session() as session:
            session.run(
                "MATCH ()-[r:SIMILAR_TO {nlp_run_id: $run_id}]->() DELETE r",
                run_id=run_id,
            )

        # Batch CREATE
        for i in range(0, len(edges), NEO4J_BATCH_EDGES):
            batch = edges[i : i + NEO4J_BATCH_EDGES]
            with driver.session() as session:
                result = session.run(
                    """
                    UNWIND $edges AS edge
                    MATCH (a:ProjetoFinep {id: edge.src_id})
                    MATCH (b:ProjetoFinep {id: edge.dst_id})
                    CREATE (a)-[:SIMILAR_TO {
                        nlp_run_id:        $run_id,
                        cosine_similarity: edge.cosine_similarity
                    }]->(b)
                    RETURN count(*) AS created
                    """,
                    edges=batch,
                    run_id=run_id,
                )
                record = result.single()
                edges_created += int(record["created"]) if record is not None else 0

        logger.info(f"  Created {edges_created} SIMILAR_TO relationships")
    finally:
        driver.close()

    # Clean up artifact files
    for fname in [
        f"embeddings_{run_id}.npy",
        f"nn_indices_{run_id}.npy",
        f"nn_distances_{run_id}.npy",
        f"proj_ids_{run_id}.npy",
    ]:
        fpath = artifact_path / fname
        if fpath.exists():
            fpath.unlink()
            logger.info(f"  Removed artifact: {fpath.name}")

    return {"run_id": run_id, "edges_created": edges_created}
