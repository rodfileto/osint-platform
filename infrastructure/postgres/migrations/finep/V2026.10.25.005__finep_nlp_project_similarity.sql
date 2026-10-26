-- k-NN similarity between FINEP projects, one row per unordered pair per NLP run (it used to be
-- written to Neo4j as SIMILAR_TO). Rows go away with their run or with either project, like
-- finep.nlp_document_topic.
CREATE TABLE IF NOT EXISTS finep.nlp_project_similarity (
    run_id              INTEGER NOT NULL REFERENCES finep.nlp_model_run(id) ON DELETE CASCADE,
    projeto_id          INTEGER NOT NULL REFERENCES finep.projetos_operacao_direta(id) ON DELETE CASCADE,
    similar_projeto_id  INTEGER NOT NULL REFERENCES finep.projetos_operacao_direta(id) ON DELETE CASCADE,
    similarity          REAL    NOT NULL,
    PRIMARY KEY (run_id, projeto_id, similar_projeto_id),
    CHECK (projeto_id < similar_projeto_id)
);

-- The primary key serves lookups by projeto_id; this one serves the other side of the pair.
CREATE INDEX IF NOT EXISTS idx_finep_nlp_similarity_other
    ON finep.nlp_project_similarity (run_id, similar_projeto_id);

COMMENT ON TABLE finep.nlp_project_similarity IS
    'Cosine similarity between project embeddings for each pair among each project''s k nearest '
    'neighbors with similarity >= nlp_model_run.sim_threshold. Each pair is stored once, with '
    'projeto_id < similar_projeto_id; query both columns to find the neighbors of a project.';

COMMENT ON TABLE finep.nlp_model_run IS
    'One row per BERTopic pipeline execution over finep.projetos_operacao_direta '
    '(resumo_publicavel, falling back to titulo). Tracks embedding model, UMAP/HDBSCAN '
    'hyperparameters and Airflow dag_run_id. Similarity pairs are in finep.nlp_project_similarity.';

COMMENT ON COLUMN finep.nlp_model_run.sim_threshold IS
    'Minimum cosine similarity for a pair to be stored in finep.nlp_project_similarity.';

COMMENT ON TABLE finep.nlp_document_topic IS
    'Per-document topic assignment and 2-D UMAP coordinates for each pipeline run. '
    'topic_id = -1 means the document was not assigned to any cluster (outlier). '
    'JOIN with finep.nlp_topic via (run_id, topic_id) to retrieve keywords and label.';
