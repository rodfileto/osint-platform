export interface FinepMunicipioMapFeatureProperties {
  municipality_ibge_code: string;
  municipio_nome: string;
  uf: string | null;
  latitude: number | null;
  longitude: number | null;
  total_empresas: number;
  total_projetos: number;
  total_aprovado_finep: number;
  total_liberado: number;
  fontes: string[];
}

export interface FinepMunicipioMapFeature {
  type: "Feature";
  id: string;
  geometry: {
    type: string;
    coordinates: unknown;
  };
  properties: FinepMunicipioMapFeatureProperties;
}

export interface FinepMunicipioMapFeatureCollection {
  type: "FeatureCollection";
  features: FinepMunicipioMapFeature[];
  metadata: {
    feature_count: number;
    municipalities_with_value: number;
    max_total_aprovado_finep: number;
    simplify_tolerance: number;
  };
}