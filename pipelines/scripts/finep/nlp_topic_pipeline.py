"""
FINEP NLP Topic Modelling — Core Pipeline Logic

Called by the finep_nlp_topic DAG (single task embed_and_cluster): fetch, embed, BERTopic,
k-NN graph, and write everything to PostgreSQL in one transaction.
"""

from __future__ import annotations

import json
import logging
import os
import sys

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


# ── Connections ───────────────────────────────────────────────────────────────

def _get_pg_conn():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "osint_metadata"),
        user=os.getenv("POSTGRES_USER", "osint_admin"),
        password=os.getenv("POSTGRES_PASSWORD", ""),
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
    5. Persist to PG: nlp_model_run, nlp_topic, nlp_document_topic, nlp_project_similarity

    Returns a dict pushed to XCom with run_id and counts.
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
    embeddings = st_model.encode(texts, show_progress_bar=sys.stderr.isatty(), batch_size=32)
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

    # Each pair once, smaller project id first; column 0 of the k-NN result is the point itself.
    similar_pairs: dict[tuple[int, int], float] = {}
    for i, (nbrs, dists) in enumerate(zip(nn_indices, nn_distances)):
        for j, d in zip(nbrs[1:k_neighbors + 1], dists[1:k_neighbors + 1]):
            sim = float(1.0 - d)
            if sim < sim_threshold or j == i:
                continue
            a, b = sorted((int(proj_ids[i]), int(proj_ids[j])))
            similar_pairs.setdefault((a, b), round(sim, 4))
    logger.info(f"{len(similar_pairs)} similar pairs above threshold")

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

            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO finep.nlp_project_similarity
                    (run_id, projeto_id, similar_projeto_id, similarity)
                VALUES %s
                """,
                [(run_id, a, b, sim) for (a, b), sim in similar_pairs.items()],
                page_size=2000,
            )
            logger.info(f"Inserted {len(similar_pairs)} similarity pairs")

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return {
        "run_id":        run_id,
        "n_documents":   n,
        "n_topics":      n_topics,
        "n_outliers":    n_outliers,
        "n_similar_pairs": len(similar_pairs),
        "sim_threshold": sim_threshold,
        "k_neighbors":   k_neighbors,
    }

