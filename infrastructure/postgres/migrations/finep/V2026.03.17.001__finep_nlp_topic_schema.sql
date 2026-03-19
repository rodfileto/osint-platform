-- ============================================================
-- FINEP NLP — Topic Modelling Metadata
--
-- Stores structured output of the BERTopic pipeline over
-- finep.projetos_operacao_direta.titulo.
-- Similarity edges (k-NN graph) are stored in Neo4j as
-- (ProjetoFinep)-[:SIMILAR_TO]->(ProjetoFinep) relationships.
--
-- Tables:
--   finep.nlp_model_run        — one row per DAG run (hyperparameters)
--   finep.nlp_topic            — topic keyword definitions per run
--   finep.nlp_document_topic   — topic assignment + UMAP 2D coords per project
-- ============================================================


-- ── Model Run ────────────────────────────────────────────────────────────────
-- One row per pipeline execution. Captures all hyperparameters needed to
-- reproduce or compare runs.

CREATE TABLE IF NOT EXISTS finep.nlp_model_run (
    id                          SERIAL          PRIMARY KEY,

    -- Airflow provenance
    dag_run_id                  VARCHAR(250),

    -- Embedding
    embedding_model             VARCHAR(200)    NOT NULL
                                    DEFAULT 'paraphrase-multilingual-mpnet-base-v2',

    -- UMAP (cluster=5D, viz=2D — separate instances)
    umap_n_neighbors            SMALLINT        NOT NULL DEFAULT 15,
    umap_n_components_cluster   SMALLINT        NOT NULL DEFAULT 5,

    -- HDBSCAN
    hdbscan_min_cluster_size    SMALLINT        NOT NULL DEFAULT 10,

    -- k-NN graph parameters (edges stored in Neo4j)
    sim_threshold               NUMERIC(4,3)    NOT NULL DEFAULT 0.70,
    k_neighbors                 SMALLINT        NOT NULL DEFAULT 5,

    -- Results summary
    n_documents                 INTEGER,
    n_topics                    SMALLINT,
    n_outliers                  INTEGER,

    created_at                  TIMESTAMPTZ     NOT NULL DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE finep.nlp_model_run IS
    'One row per BERTopic pipeline execution over finep.projetos_operacao_direta.titulo. '
    'Tracks embedding model, UMAP/HDBSCAN hyperparameters and Airflow dag_run_id. '
    'Similarity edges (k-NN graph) are persisted in Neo4j as SIMILAR_TO relationships.';

COMMENT ON COLUMN finep.nlp_model_run.hdbscan_min_cluster_size IS
    'Computed at runtime as max(5, n_documents // 100); stored here for reproducibility.';

COMMENT ON COLUMN finep.nlp_model_run.sim_threshold IS
    'Minimum cosine similarity to create a SIMILAR_TO edge in Neo4j.';


-- ── Topics ───────────────────────────────────────────────────────────────────
-- One row per discovered topic per run.
-- Topic -1 (outliers) is NOT stored here; outlier documents are identified
-- by topic_id = -1 in nlp_document_topic.

CREATE TABLE IF NOT EXISTS finep.nlp_topic (
    id                  SERIAL          PRIMARY KEY,
    run_id              INTEGER         NOT NULL
                            REFERENCES finep.nlp_model_run(id) ON DELETE CASCADE,

    topic_id            SMALLINT        NOT NULL,       -- BERTopic internal id (≥ 0)
    document_count      INTEGER,
    keywords            JSONB,                          -- [[term, weight], ...] top-10 c-TF-IDF
    label               TEXT,                          -- NULL initially; set by reviewer or LLM task

    UNIQUE (run_id, topic_id)
);

COMMENT ON TABLE finep.nlp_topic IS
    'BERTopic topic definitions per run. '
    'keywords: JSONB array of [term, c-TF-IDF weight] ordered by weight desc. '
    'topic_id >= 0 only; outlier documents (topic_id = -1) have no row here.';

COMMENT ON COLUMN finep.nlp_topic.label IS
    'Human-readable topic label. Initially NULL; can be updated by a reviewer '
    'or a downstream LLM-labelling task.';

CREATE INDEX IF NOT EXISTS idx_finep_nlp_topic_run
    ON finep.nlp_topic(run_id);


-- ── Document → Topic Assignment ───────────────────────────────────────────────
-- One row per project per run.
-- Stores topic assignment and 2-D UMAP coordinates for frontend scatter plot.

CREATE TABLE IF NOT EXISTS finep.nlp_document_topic (
    id                  BIGSERIAL       PRIMARY KEY,
    run_id              INTEGER         NOT NULL
                            REFERENCES finep.nlp_model_run(id) ON DELETE CASCADE,
    projeto_id          INTEGER         NOT NULL
                            REFERENCES finep.projetos_operacao_direta(id) ON DELETE CASCADE,

    topic_id            SMALLINT        NOT NULL,       -- -1 for outliers
    umap_x              REAL,                           -- 2-D UMAP x (viz projection)
    umap_y              REAL,                           -- 2-D UMAP y

    UNIQUE (run_id, projeto_id)
);

COMMENT ON TABLE finep.nlp_document_topic IS
    'Per-document topic assignment and 2-D UMAP coordinates for each pipeline run. '
    'topic_id = -1 means the document was not assigned to any cluster (outlier). '
    'JOIN with finep.nlp_topic via (run_id, topic_id) to retrieve keywords and label. '
    'ProjetoFinep nodes in Neo4j carry topic_id and umap_x/y as properties, '
    'sourced from this table during the Neo4j load task.';

CREATE INDEX IF NOT EXISTS idx_finep_nlp_dt_run
    ON finep.nlp_document_topic(run_id);

CREATE INDEX IF NOT EXISTS idx_finep_nlp_dt_projeto
    ON finep.nlp_document_topic(projeto_id);

CREATE INDEX IF NOT EXISTS idx_finep_nlp_dt_run_topic
    ON finep.nlp_document_topic(run_id, topic_id);
