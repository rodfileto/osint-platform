-- ============================================================
-- INOVALINK Relationship Graph
-- Explicit organization-to-organization links derived from
-- linkable_type + linkable_id in the upstream payload.
-- ============================================================

CREATE TABLE IF NOT EXISTS inovalink.organization_relationship (
    id                          BIGSERIAL PRIMARY KEY,
    manifest_id                 INTEGER         NOT NULL REFERENCES inovalink.sync_manifest(id) ON DELETE CASCADE,
    child_organization_id       BIGINT          NOT NULL REFERENCES inovalink.organization_snapshot(id) ON DELETE CASCADE,
    relationship_type           VARCHAR(50)     NOT NULL,
    parent_model_class          TEXT            NOT NULL,
    parent_source_record_id     BIGINT          NOT NULL,
    parent_organization_id      BIGINT          REFERENCES inovalink.organization_snapshot(id) ON DELETE SET NULL,
    relationship_metadata       JSONB,
    created_at                  TIMESTAMPTZ     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at                  TIMESTAMPTZ     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_inovalink_org_relationship
        UNIQUE (manifest_id, child_organization_id, relationship_type, parent_model_class, parent_source_record_id),
    CONSTRAINT chk_inovalink_relationship_type
        CHECK (
            relationship_type IN (
                'incubated_by',
                'accelerated_by',
                'resident_in',
                'graduated_from',
                'linked_to'
            )
        )
);

COMMENT ON TABLE inovalink.organization_relationship IS
    'Explicit edges between Inovalink organizations derived from linkable_type/linkable_id in the source payload.';
COMMENT ON COLUMN inovalink.organization_relationship.child_organization_id IS
    'Normalized child actor row, e.g. an incubated company.';
COMMENT ON COLUMN inovalink.organization_relationship.parent_model_class IS
    'Upstream Laravel model class from linkable_type, e.g. App\\Models\\Incubator.';
COMMENT ON COLUMN inovalink.organization_relationship.parent_source_record_id IS
    'Upstream parent record id from linkable_id, e.g. company 112 linked to incubator 61.';
COMMENT ON COLUMN inovalink.organization_relationship.parent_organization_id IS
    'Resolved normalized parent actor row inside the same collected manifest when available.';
COMMENT ON COLUMN inovalink.organization_relationship.relationship_metadata IS
    'Optional raw relationship details preserved from the source payload or loader resolution step.';

CREATE OR REPLACE FUNCTION inovalink.update_relationship_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS update_inovalink_organization_relationship_updated_at ON inovalink.organization_relationship;
CREATE TRIGGER update_inovalink_organization_relationship_updated_at
    BEFORE UPDATE ON inovalink.organization_relationship
    FOR EACH ROW
    EXECUTE FUNCTION inovalink.update_relationship_updated_at_column();

CREATE INDEX IF NOT EXISTS idx_inovalink_org_relationship_manifest
    ON inovalink.organization_relationship (manifest_id);

CREATE INDEX IF NOT EXISTS idx_inovalink_org_relationship_child
    ON inovalink.organization_relationship (child_organization_id);

CREATE INDEX IF NOT EXISTS idx_inovalink_org_relationship_parent_source
    ON inovalink.organization_relationship (parent_model_class, parent_source_record_id);

CREATE INDEX IF NOT EXISTS idx_inovalink_org_relationship_parent_org
    ON inovalink.organization_relationship (parent_organization_id)
    WHERE parent_organization_id IS NOT NULL;

CREATE OR REPLACE VIEW inovalink.organization_relationship_resolved AS
SELECT
    rel.id,
    rel.manifest_id,
    rel.relationship_type,
    rel.parent_model_class,
    rel.parent_source_record_id,
    child.source_collection      AS child_source_collection,
    child.source_record_id       AS child_source_record_id,
    child.entity_kind            AS child_entity_kind,
    child.name                   AS child_name,
    child.cnpj                   AS child_cnpj,
    parent.source_collection     AS parent_source_collection,
    parent.source_record_id      AS parent_source_record_id_resolved,
    parent.entity_kind           AS parent_entity_kind,
    parent.name                  AS parent_name,
    parent.cnpj                  AS parent_cnpj,
    rel.relationship_metadata,
    rel.created_at,
    rel.updated_at
FROM inovalink.organization_relationship AS rel
JOIN inovalink.organization_snapshot AS child
    ON child.id = rel.child_organization_id
LEFT JOIN inovalink.organization_snapshot AS parent
    ON parent.id = rel.parent_organization_id;

COMMENT ON VIEW inovalink.organization_relationship_resolved IS
    'Resolved organization-to-organization edges for the latest Inovalink snapshots loaded into PostgreSQL.';

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA inovalink TO osint_admin;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA inovalink TO osint_admin;
GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA inovalink TO osint_admin;