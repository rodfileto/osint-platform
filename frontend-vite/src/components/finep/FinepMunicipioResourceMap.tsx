import ComponentCard from "@/components/common/ComponentCard";
import Badge from "@/components/ui/badge/Badge";
import { GeoJSON, MapContainer, useMap } from "react-leaflet";
import { useEffect, useMemo, useState } from "react";
import L from "leaflet";
import type {
  FinepMunicipioMapFeature,
  FinepMunicipioMapFeatureCollection,
} from "@/components/finep/finepMunicipioMapTypes";

function formatMoney(value: number | null | undefined): string {
  const amount = Number(value ?? 0);
  return new Intl.NumberFormat("pt-BR", {
    style: "currency",
    currency: "BRL",
    maximumFractionDigits: 0,
  }).format(amount);
}

function formatCompactMoney(value: number | null | undefined): string {
  const amount = Number(value ?? 0);
  return new Intl.NumberFormat("pt-BR", {
    style: "currency",
    currency: "BRL",
    notation: "compact",
    maximumFractionDigits: 1,
  }).format(amount);
}

function sourceLabel(source: string): string {
  const labels: Record<string, string> = {
    ancine: "Ancine",
    credito_descentralizado: "Credito Descentralizado",
    investimento: "Investimento",
    operacao_direta: "Operacao Direta",
  };

  return labels[source] ?? source;
}

function fillColor(value: number, maxValue: number): string {
  if (value <= 0 || maxValue <= 0) {
    return "#F3F4F6";
  }

  const intensity = value / maxValue;
  if (intensity >= 0.8) return "#9A3412";
  if (intensity >= 0.55) return "#C2410C";
  if (intensity >= 0.3) return "#EA580C";
  if (intensity >= 0.12) return "#F59E0B";
  return "#FCD34D";
}

function MapBoundsController({ featureCollection }: { featureCollection: FinepMunicipioMapFeatureCollection }) {
  const map = useMap();

  useEffect(() => {
    if (!featureCollection.features.length) {
      return;
    }

    const bounds = L.geoJSON(featureCollection as never).getBounds();
    if (bounds.isValid()) {
      map.fitBounds(bounds.pad(0.04), { animate: false });
    }
  }, [featureCollection, map]);

  return null;
}

type FinepMunicipioResourceMapProps = {
  featureCollection: FinepMunicipioMapFeatureCollection | null;
  loading: boolean;
};

export type { FinepMunicipioMapFeatureCollection } from "@/components/finep/finepMunicipioMapTypes";

export default function FinepMunicipioResourceMap({ featureCollection, loading }: FinepMunicipioResourceMapProps) {
  const [selectedCode, setSelectedCode] = useState<string | null>(null);

  const features = useMemo(() => featureCollection?.features ?? [], [featureCollection]);
  const featureCount = featureCollection?.metadata.feature_count ?? features.length;
  const municipalitiesWithValue =
    featureCollection?.metadata.municipalities_with_value ??
    features.filter((feature) => feature.properties.total_aprovado_finep > 0).length;
  const maxApproved = featureCollection?.metadata.max_total_aprovado_finep ?? 0;

  const topFeatures = useMemo(
    () =>
      [...features]
        .filter((feature) => feature.properties.total_aprovado_finep > 0)
        .sort((left, right) => right.properties.total_aprovado_finep - left.properties.total_aprovado_finep)
        .slice(0, 5),
    [features]
  );

  const effectiveSelectedCode = useMemo(() => {
    if (!features.length) {
      return null;
    }

    const firstFeatureWithValue = features.find((feature) => feature.properties.total_aprovado_finep > 0);
    if (selectedCode && features.some((feature) => feature.id === selectedCode)) {
      return selectedCode;
    }

    return firstFeatureWithValue?.id ?? features[0]?.id ?? null;
  }, [features, selectedCode]);

  const selectedFeature = useMemo(
    () => features.find((feature) => feature.id === effectiveSelectedCode) ?? null,
    [effectiveSelectedCode, features]
  );

  return (
    <ComponentCard
      title="Mapa de Recursos por Municipio"
      desc="Poligonos municipais do schema geo coloridos pelo valor aprovado FINEP no recorte atual."
      className="xl:col-span-12"
    >
      <div className="grid grid-cols-1 gap-6 xl:grid-cols-[minmax(0,1.75fr)_minmax(320px,0.85fr)]">
        <div className="overflow-hidden rounded-3xl border border-gray-200 bg-[radial-gradient(circle_at_top,#fff7ed_0%,#ffffff_44%,#eff6ff_100%)] p-3 dark:border-gray-800 dark:bg-[radial-gradient(circle_at_top,#3b1d12_0%,#111827_42%,#0f172a_100%)]">
          <div className="mb-3 flex flex-wrap items-center justify-between gap-2 px-2 pt-1">
            <div>
              <p className="text-sm font-medium text-gray-800 dark:text-white/90">Cobertura geografica</p>
              <p className="text-xs text-gray-500 dark:text-gray-400">
                {loading
                  ? "Carregando poligonos municipais..."
                  : `${featureCount.toLocaleString("pt-BR")} municipios com geometria, ${municipalitiesWithValue.toLocaleString("pt-BR")} com valor no recorte`}
              </p>
            </div>
            <div className="flex flex-wrap items-center gap-2">
              <Badge color="warning" size="sm">Cor = valor aprovado</Badge>
              <Badge color="light" size="sm">Cinza = sem valor</Badge>
              <Badge color="light" size="sm">Geo simplificado {featureCollection?.metadata.simplify_tolerance ?? 0}</Badge>
            </div>
          </div>

          <div className="finep-municipio-map h-[460px] overflow-hidden rounded-2xl border border-white/70 shadow-[0_24px_60px_-28px_rgba(15,23,42,0.35)] dark:border-gray-800">
            {features.length ? (
              <MapContainer
                center={[-14.235, -51.9253]}
                zoom={4}
                minZoom={3}
                maxZoom={11}
                zoomControl={false}
                className="h-full w-full bg-[#FFF8EF] dark:bg-[#111827]"
                attributionControl={false}
              >
                <MapBoundsController featureCollection={featureCollection as FinepMunicipioMapFeatureCollection} />
                <GeoJSON
                  data={featureCollection as never}
                  style={(feature) => {
                    const municipalityFeature = feature as FinepMunicipioMapFeature | undefined;
                    const approvedValue = municipalityFeature?.properties.total_aprovado_finep ?? 0;
                    const isSelected = municipalityFeature?.id === effectiveSelectedCode;

                    return {
                      color: isSelected ? "#111827" : "rgba(148, 163, 184, 0.55)",
                      weight: isSelected ? 2.2 : 0.7,
                      opacity: isSelected ? 0.9 : 0.55,
                      fillOpacity: approvedValue > 0 ? (isSelected ? 0.92 : 0.78) : isSelected ? 0.78 : 0.52,
                      fillColor: fillColor(approvedValue, maxApproved),
                    };
                  }}
                  onEachFeature={(feature, layer) => {
                    const municipalityFeature = feature as FinepMunicipioMapFeature;
                    const approvedValue = municipalityFeature.properties.total_aprovado_finep;
                    const releasedValue = municipalityFeature.properties.total_liberado;

                    layer.bindTooltip(
                      `<div style="padding:8px 10px;min-width:220px">` +
                        `<strong>${municipalityFeature.properties.municipio_nome} · ${municipalityFeature.properties.uf ?? "Sem UF"}</strong><br/>` +
                        `Aprovado: ${formatMoney(approvedValue)}<br/>` +
                        `Liberado: ${formatMoney(releasedValue)}<br/>` +
                        `Projetos: ${municipalityFeature.properties.total_projetos.toLocaleString("pt-BR")}<br/>` +
                        `Empresas: ${municipalityFeature.properties.total_empresas.toLocaleString("pt-BR")}<br/>` +
                        `Status: ${approvedValue > 0 ? "Com recursos" : "Sem recursos no recorte"}` +
                        `</div>`,
                      { sticky: true }
                    );

                    layer.on({
                      click: () => setSelectedCode(municipalityFeature.id),
                    });
                  }}
                />
              </MapContainer>
            ) : (
              <div className="flex h-full items-center justify-center px-6 text-center text-sm text-gray-500 dark:text-gray-400">
                {loading
                  ? "Carregando mapa municipal..."
                  : "Nenhum municipio com geometria correspondente foi encontrado para o filtro atual."}
              </div>
            )}
          </div>
        </div>

        <div className="space-y-4">
          <div className="rounded-2xl border border-gray-100 bg-gray-50 p-4 dark:border-gray-800 dark:bg-gray-900/70">
            <p className="text-xs font-medium uppercase tracking-wide text-gray-500 dark:text-gray-400">Destaques territoriais</p>
            <div className="mt-3 space-y-3">
              {topFeatures.map((feature) => (
                <button
                  key={feature.id}
                  type="button"
                  onClick={() => setSelectedCode(feature.id)}
                  className="flex w-full items-start justify-between gap-3 rounded-xl border border-gray-100 bg-white px-4 py-3 text-left transition hover:border-warning-300 hover:bg-warning-50 dark:border-gray-800 dark:bg-gray-950 dark:hover:border-warning-500/30 dark:hover:bg-warning-500/10"
                >
                  <div>
                    <p className="text-sm font-medium text-gray-800 dark:text-white/90">
                      {feature.properties.municipio_nome} · {feature.properties.uf ?? "Sem UF"}
                    </p>
                    <p className="text-xs text-gray-500 dark:text-gray-400">
                      {feature.properties.total_projetos.toLocaleString("pt-BR")} projetos • {feature.properties.total_empresas.toLocaleString("pt-BR")} empresas
                    </p>
                  </div>
                  <Badge color="warning" size="sm">{formatCompactMoney(feature.properties.total_aprovado_finep)}</Badge>
                </button>
              ))}
              {!topFeatures.length && !loading ? (
                <p className="text-sm text-gray-500 dark:text-gray-400">Nenhum municipio com valor no filtro atual.</p>
              ) : null}
            </div>
          </div>

          <div className="rounded-2xl border border-gray-100 bg-white p-4 dark:border-gray-800 dark:bg-gray-900/70">
            <div className="flex items-center justify-between gap-3">
              <p className="text-sm font-medium text-gray-800 dark:text-white/90">Municipio em foco</p>
              {selectedFeature ? <Badge color="success" size="sm">Selecionado</Badge> : null}
            </div>

            {selectedFeature ? (
              <div className="mt-4 space-y-3">
                <div>
                  <p className="text-lg font-semibold text-gray-900 dark:text-white">
                    {selectedFeature.properties.municipio_nome} · {selectedFeature.properties.uf ?? "Sem UF"}
                  </p>
                  <p className="text-xs text-gray-500 dark:text-gray-400">IBGE {selectedFeature.properties.municipality_ibge_code}</p>
                  <p className="mt-1 text-xs text-gray-500 dark:text-gray-400">
                    {selectedFeature.properties.total_aprovado_finep > 0 ? "Municipio com recursos no recorte atual." : "Municipio sem recursos no recorte atual."}
                  </p>
                </div>

                <div className="grid grid-cols-2 gap-3">
                  <div className="rounded-xl bg-gray-50 p-3 dark:bg-gray-800/60">
                    <p className="text-xs uppercase tracking-wide text-gray-500 dark:text-gray-400">Aprovado</p>
                    <p className="mt-1 text-sm font-semibold text-gray-800 dark:text-white/90">
                      {formatMoney(selectedFeature.properties.total_aprovado_finep)}
                    </p>
                  </div>
                  <div className="rounded-xl bg-gray-50 p-3 dark:bg-gray-800/60">
                    <p className="text-xs uppercase tracking-wide text-gray-500 dark:text-gray-400">Liberado</p>
                    <p className="mt-1 text-sm font-semibold text-gray-800 dark:text-white/90">
                      {formatMoney(selectedFeature.properties.total_liberado)}
                    </p>
                  </div>
                  <div className="rounded-xl bg-gray-50 p-3 dark:bg-gray-800/60">
                    <p className="text-xs uppercase tracking-wide text-gray-500 dark:text-gray-400">Projetos</p>
                    <p className="mt-1 text-sm font-semibold text-gray-800 dark:text-white/90">
                      {selectedFeature.properties.total_projetos.toLocaleString("pt-BR")}
                    </p>
                  </div>
                  <div className="rounded-xl bg-gray-50 p-3 dark:bg-gray-800/60">
                    <p className="text-xs uppercase tracking-wide text-gray-500 dark:text-gray-400">Empresas</p>
                    <p className="mt-1 text-sm font-semibold text-gray-800 dark:text-white/90">
                      {selectedFeature.properties.total_empresas.toLocaleString("pt-BR")}
                    </p>
                  </div>
                </div>

                <div className="flex flex-wrap gap-2">
                  {selectedFeature.properties.fontes.map((source) => (
                    <Badge key={source} color="info" size="sm">{sourceLabel(source)}</Badge>
                  ))}
                </div>
              </div>
            ) : (
              <p className="mt-4 text-sm text-gray-500 dark:text-gray-400">
                Selecione um municipio no mapa para inspecionar o volume de recursos e a cobertura empresarial.
              </p>
            )}
          </div>
        </div>
      </div>
    </ComponentCard>
  );
}