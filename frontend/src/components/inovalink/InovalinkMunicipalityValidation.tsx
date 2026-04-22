import { useEffect, useMemo, useState } from "react";
import Chart from "react-apexcharts";
import type { ApexOptions } from "apexcharts";
import { isAxiosError } from "axios";
import ComponentCard from "@/components/common/ComponentCard";
import InputField from "@/components/form/input/InputField";
import Pagination from "@/components/tables/Pagination";
import Badge from "@/components/ui/badge/Badge";
import Button from "@/components/ui/button/Button";
import { Table, TableBody, TableCell, TableHeader, TableRow } from "@/components/ui/table";
import apiClient from "@/lib/apiClient";

interface InovalinkMunicipalityValidationRecord {
  id: number;
  source_collection: string;
  source_record_id: number;
  entity_kind: string;
  category_label: string | null;
  name: string;
  city_name: string | null;
  state_name: string | null;
  state_abbrev: string | null;
  latitude: number | null;
  longitude: number | null;
  geobr_municipality_ibge_code: string | null;
  geobr_city_name: string | null;
  geobr_state_abbrev: string | null;
  validation_status: string;
  municipality_matches: boolean | null;
}

interface PaginatedResponse<T> {
  count: number;
  next: string | null;
  previous: string | null;
  results: T[];
}

interface InovalinkMunicipalityCityCount {
  city_name: string;
  state_abbrev: string | null;
  total_records: number;
}

interface InovalinkMunicipalityValidationSummary {
  total_records: number;
  issue_records: number;
  top_declared_cities: InovalinkMunicipalityCityCount[];
  top_geobr_cities: InovalinkMunicipalityCityCount[];
}

function MetricCard({ title, value, note, accentClass }: { title: string; value: string; note?: string; accentClass: string }) {
  return (
    <div className="rounded-2xl border border-gray-200 bg-white p-5 dark:border-gray-800 dark:bg-white/[0.03]">
      <div className={`flex h-12 w-12 items-center justify-center rounded-xl text-sm font-semibold ${accentClass}`}>{title.slice(0, 3).toUpperCase()}</div>
      <div className="mt-5">
        <p className="text-sm text-gray-500 dark:text-gray-400">{title}</p>
        <h3 className="mt-2 text-2xl font-semibold text-gray-800 dark:text-white/90">{value}</h3>
        {note ? <p className="mt-2 text-xs text-gray-500 dark:text-gray-400">{note}</p> : null}
      </div>
    </div>
  );
}

const PAGE_SIZE = 20;

const ENTITY_KIND_OPTIONS = [
  { value: "", label: "Todos os tipos" },
  { value: "company", label: "Empresa" },
  { value: "incubator", label: "Incubadora" },
  { value: "accelerator", label: "Aceleradora" },
  { value: "park", label: "Parque" },
];

function statusLabel(status: string): string {
  switch (status) {
    case "ok":
      return "OK";
    case "municipality_mismatch":
      return "Municipio divergente";
    case "state_mismatch":
      return "UF divergente";
    case "polygon_not_found":
      return "Sem poligono";
    case "coordinates_missing":
      return "Sem coordenadas";
    default:
      return status;
  }
}

function statusColor(status: string): "success" | "warning" | "error" | "info" | "light" {
  switch (status) {
    case "ok":
      return "success";
    case "municipality_mismatch":
      return "warning";
    case "state_mismatch":
      return "error";
    case "polygon_not_found":
      return "info";
    default:
      return "light";
  }
}

function formatCoordinate(value: number | null): string {
  if (value === null || Number.isNaN(value)) return "—";
  return value.toFixed(6);
}

function formatCityLabel(cityName: string, stateAbbrev: string | null): string {
  return stateAbbrev ? `${cityName} · ${stateAbbrev}` : cityName;
}

function buildRankingChartOptions(categories: string[], color: string): ApexOptions {
  return {
    colors: [color],
    chart: {
      fontFamily: "Outfit, sans-serif",
      type: "bar",
      toolbar: {
        show: false,
      },
    },
    plotOptions: {
      bar: {
        horizontal: true,
        borderRadius: 6,
        borderRadiusApplication: "end",
        barHeight: "48%",
      },
    },
    dataLabels: {
      enabled: false,
    },
    legend: {
      show: false,
    },
    grid: {
      xaxis: {
        lines: {
          show: true,
        },
      },
      yaxis: {
        lines: {
          show: false,
        },
      },
    },
    xaxis: {
      categories,
      axisBorder: {
        show: false,
      },
      axisTicks: {
        show: false,
      },
      labels: {
        style: {
          fontSize: "12px",
          colors: Array(Math.max(categories.length, 1)).fill("#6B7280"),
        },
        formatter: (value: string) => Number(value).toLocaleString("pt-BR"),
      },
    },
    yaxis: {
      labels: {
        maxWidth: 220,
        style: {
          fontSize: "12px",
          colors: Array(Math.max(categories.length, 1)).fill("#1D2939"),
        },
      },
    },
    tooltip: {
      x: {
        show: true,
      },
      y: {
        formatter: (value: number) => `${value.toLocaleString("pt-BR")} registro(s)`,
      },
    },
  };
}

function CityRankingCard({
  title,
  desc,
  items,
  chartColor,
  loading,
}: {
  title: string;
  desc: string;
  items: InovalinkMunicipalityCityCount[];
  chartColor: string;
  loading: boolean;
}) {
  const categories = items.map((item) => formatCityLabel(item.city_name, item.state_abbrev));
  const series = [
    {
      name: "Registros",
      data: items.map((item) => item.total_records),
    },
  ];
  const options = useMemo(() => buildRankingChartOptions(categories, chartColor), [categories, chartColor]);

  return (
    <ComponentCard title={title} desc={desc}>
      <div className="max-w-full overflow-x-auto custom-scrollbar">
        {items.length ? (
          <div className="min-w-[320px]">
            <Chart options={options} series={series} type="bar" height={Math.max(260, items.length * 46)} />
          </div>
        ) : null}
        {!items.length && !loading ? <p className="text-sm text-gray-500 dark:text-gray-400">Nenhum municipio disponivel para o filtro atual.</p> : null}
        {loading && !items.length ? <p className="text-sm text-gray-400 dark:text-gray-600">Carregando ranking...</p> : null}
      </div>
    </ComponentCard>
  );
}

export default function InovalinkMunicipalityValidation() {
  const [query, setQuery] = useState("");
  const [appliedQuery, setAppliedQuery] = useState("");
  const [entityKind, setEntityKind] = useState("");
  const [appliedEntityKind, setAppliedEntityKind] = useState("");
  const [stateAbbrev, setStateAbbrev] = useState("");
  const [appliedStateAbbrev, setAppliedStateAbbrev] = useState("");
  const [onlyIssues, setOnlyIssues] = useState(true);
  const [appliedOnlyIssues, setAppliedOnlyIssues] = useState(true);
  const [currentPage, setCurrentPage] = useState(1);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [data, setData] = useState<PaginatedResponse<InovalinkMunicipalityValidationRecord> | null>(null);
  const [summary, setSummary] = useState<InovalinkMunicipalityValidationSummary | null>(null);

  useEffect(() => {
    const fetchData = async () => {
      setLoading(true);
      setError(null);

      const params: Record<string, string | number | boolean> = {
        page: currentPage,
        page_size: PAGE_SIZE,
        only_issues: appliedOnlyIssues,
      };
      if (appliedQuery.trim()) params.q = appliedQuery.trim();
      if (appliedEntityKind) params.entity_kind = appliedEntityKind;
      if (appliedStateAbbrev.trim()) params.state_abbrev = appliedStateAbbrev.trim().toUpperCase();

      try {
        const [pageResponse, summaryResponse] = await Promise.all([
          apiClient.get<PaginatedResponse<InovalinkMunicipalityValidationRecord>>("/api/inovalink/municipality-validation", { params }),
          apiClient.get<InovalinkMunicipalityValidationSummary>("/api/inovalink/municipality-validation/summary", {
            params: {
              ...(appliedQuery.trim() ? { q: appliedQuery.trim() } : {}),
              ...(appliedEntityKind ? { entity_kind: appliedEntityKind } : {}),
              ...(appliedStateAbbrev.trim() ? { state_abbrev: appliedStateAbbrev.trim().toUpperCase() } : {}),
              only_issues: appliedOnlyIssues,
            },
          }),
        ]);
        setData(pageResponse.data);
        setSummary(summaryResponse.data);
      } catch (fetchError) {
        const message =
          isAxiosError(fetchError) && fetchError.response
            ? `Erro ${fetchError.response.status}: ${fetchError.response.statusText}`
            : "Erro ao carregar a validacao municipal do Inovalink.";
        setError(message);
        setData(null);
        setSummary(null);
      } finally {
        setLoading(false);
      }
    };

    void fetchData();
  }, [appliedEntityKind, appliedOnlyIssues, appliedQuery, appliedStateAbbrev, currentPage]);

  const totalPages = useMemo(() => {
    const total = data?.count ?? 0;
    return Math.max(1, Math.ceil(total / PAGE_SIZE));
  }, [data]);

  const currentPageIssueCount = useMemo(
    () => (data?.results ?? []).filter((item) => item.validation_status !== "ok").length,
    [data],
  );

  const divergencePercentage = useMemo(() => {
    if (!summary?.total_records) return 0;
    return (summary.issue_records / summary.total_records) * 100;
  }, [summary]);

  const handleSubmit = (event: React.FormEvent) => {
    event.preventDefault();
    setCurrentPage(1);
    setAppliedQuery(query);
    setAppliedEntityKind(entityKind);
    setAppliedStateAbbrev(stateAbbrev);
    setAppliedOnlyIssues(onlyIssues);
  };

  const handleReset = () => {
    setQuery("");
    setEntityKind("");
    setStateAbbrev("");
    setOnlyIssues(true);
    setCurrentPage(1);
    setAppliedQuery("");
    setAppliedEntityKind("");
    setAppliedStateAbbrev("");
    setAppliedOnlyIssues(true);
  };

  return (
    <div className="space-y-6">
      <ComponentCard
        title="Validacao Municipal Inovalink"
        desc="Compara o municipio declarado no snapshot com o municipio derivado do ponto latitude/longitude dentro do poligono GeoBR em Postgres."
      >
        <form className="flex flex-col gap-3 lg:flex-row lg:flex-wrap lg:items-end" onSubmit={handleSubmit}>
          <div className="min-w-0 flex-1 lg:min-w-[280px]">
            <InputField
              id="inovalink-validation-query"
              type="text"
              placeholder="Buscar por nome, municipio, categoria ou collection"
              value={query}
              onChange={(event) => setQuery(event.target.value)}
            />
          </div>
          <div className="w-full lg:w-52">
            <label className="mb-1 block text-xs font-medium uppercase tracking-wide text-gray-500 dark:text-gray-400">Tipo</label>
            <select
              className="h-11 w-full rounded-lg border border-gray-300 bg-white px-4 text-sm text-gray-800 outline-none transition focus:border-brand-500 dark:border-gray-700 dark:bg-gray-900 dark:text-white/90"
              value={entityKind}
              onChange={(event) => setEntityKind(event.target.value)}
            >
              {ENTITY_KIND_OPTIONS.map((option) => (
                <option key={option.label} value={option.value}>{option.label}</option>
              ))}
            </select>
          </div>
          <div className="w-full lg:w-28">
            <label className="mb-1 block text-xs font-medium uppercase tracking-wide text-gray-500 dark:text-gray-400">UF</label>
            <InputField
              id="inovalink-validation-uf"
              type="text"
              placeholder="RS"
              value={stateAbbrev}
              onChange={(event) => setStateAbbrev(event.target.value)}
            />
          </div>
          <label className="flex h-11 items-center gap-2 rounded-lg border border-gray-200 px-4 text-sm text-gray-700 dark:border-gray-800 dark:text-gray-300">
            <input
              type="checkbox"
              checked={onlyIssues}
              onChange={(event) => setOnlyIssues(event.target.checked)}
              className="h-4 w-4 rounded border-gray-300 text-brand-500 focus:ring-brand-500"
            />
            Somente divergencias
          </label>
          <div className="flex gap-3">
            <Button type="submit" disabled={loading}>{loading ? "Atualizando..." : "Aplicar"}</Button>
            <Button type="button" variant="outline" onClick={handleReset}>Limpar</Button>
          </div>
        </form>

        <div className="flex flex-wrap items-center gap-3 text-sm text-gray-500 dark:text-gray-400">
          <span>Total: <span className="font-semibold text-gray-800 dark:text-white/90">{loading ? "..." : (data?.count ?? 0).toLocaleString("pt-BR")}</span></span>
          <span>Pagina: <span className="font-semibold text-gray-800 dark:text-white/90">{currentPage}</span> / {totalPages}</span>
          <span>Divergencias nesta pagina: <span className="font-semibold text-gray-800 dark:text-white/90">{loading ? "..." : currentPageIssueCount}</span></span>
        </div>
      </ComponentCard>

      {error ? (
        <div className="rounded-2xl border border-red-200 bg-red-50 px-5 py-4 text-sm text-red-700 dark:border-red-500/20 dark:bg-red-500/10 dark:text-red-400">
          {error}
        </div>
      ) : null}

      <div className="grid grid-cols-1 gap-4 md:grid-cols-2 xl:grid-cols-3">
        <MetricCard
          title="Registros analisados"
          value={loading ? "..." : (summary?.total_records ?? 0).toLocaleString("pt-BR")}
          note="Total dentro do filtro atual, independentemente da pagina exibida."
          accentClass="bg-brand-50 text-brand-500 dark:bg-brand-500/15 dark:text-brand-400"
        />
        <MetricCard
          title="Divergencias"
          value={loading ? "..." : (summary?.issue_records ?? 0).toLocaleString("pt-BR")}
          note="Linhas com municipio, UF, poligono ou coordenadas inconsistentes."
          accentClass="bg-amber-50 text-amber-600 dark:bg-amber-500/15 dark:text-amber-400"
        />
        <MetricCard
          title="Percentual divergente"
          value={loading ? "..." : `${divergencePercentage.toFixed(1).replace('.', ',')}%`}
          note="Percentual de divergencias sobre o total do conjunto filtrado."
          accentClass="bg-red-50 text-red-600 dark:bg-red-500/15 dark:text-red-400"
        />
      </div>

      <div className="grid grid-cols-1 gap-6 xl:grid-cols-2">
        <CityRankingCard
          title="Top 10 Municipios Declarados"
          desc={appliedOnlyIssues ? "Ranking do municipio declarado nas linhas divergentes do filtro atual." : "Ranking do municipio declarado no conjunto filtrado atual."}
          items={summary?.top_declared_cities ?? []}
          chartColor="#F59E0B"
          loading={loading}
        />
        <CityRankingCard
          title="Top 10 Municipios GeoBR"
          desc={appliedOnlyIssues ? "Ranking do municipio identificado pelo GeoBR nas linhas divergentes do filtro atual." : "Ranking do municipio identificado pelo GeoBR no conjunto filtrado atual."}
          items={summary?.top_geobr_cities ?? []}
          chartColor="#0EA5E9"
          loading={loading}
        />
      </div>

      <ComponentCard
        title="Comparacao Municipio x GeoBR"
        desc={data ? `${data.count.toLocaleString("pt-BR")} registro(s) compatíveis com o filtro atual.` : "Aguardando dados..."}
      >
        <div className="overflow-x-auto">
          <Table className="table-auto">
            <TableHeader>
              <TableRow className="border-b border-gray-100 dark:border-gray-800">
                {[
                  "Status",
                  "Entidade",
                  "Declarado",
                  "GeoBR",
                  "Coordenadas",
                  "Origem",
                ].map((header) => (
                  <TableCell key={header} isHeader className="px-4 py-3 text-left text-xs font-semibold uppercase tracking-wide text-gray-500 dark:text-gray-400">
                    {header}
                  </TableCell>
                ))}
              </TableRow>
            </TableHeader>
            <TableBody>
              {(data?.results ?? []).map((item) => (
                <TableRow key={`${item.source_collection}-${item.source_record_id}`} className="border-b border-gray-100 last:border-0 dark:border-gray-800">
                  <TableCell className="px-4 py-3 align-top">
                    <Badge color={statusColor(item.validation_status)} size="sm">{statusLabel(item.validation_status)}</Badge>
                  </TableCell>
                  <TableCell className="px-4 py-3 align-top">
                    <div>
                      <p className="max-w-[240px] truncate text-sm font-medium text-gray-800 dark:text-white/90">{item.name}</p>
                      <p className="mt-1 text-xs text-gray-500 dark:text-gray-400">
                        {item.entity_kind}
                        {item.category_label ? ` · ${item.category_label}` : ""}
                      </p>
                    </div>
                  </TableCell>
                  <TableCell className="px-4 py-3 align-top text-sm text-gray-700 dark:text-gray-300">
                    <div>
                      <p>{item.city_name ?? "—"}</p>
                      <p className="text-xs text-gray-500 dark:text-gray-400">{item.state_abbrev ?? item.state_name ?? "—"}</p>
                    </div>
                  </TableCell>
                  <TableCell className="px-4 py-3 align-top text-sm text-gray-700 dark:text-gray-300">
                    <div>
                      <p>{item.geobr_city_name ?? "—"}</p>
                      <p className="text-xs text-gray-500 dark:text-gray-400">
                        {item.geobr_state_abbrev ?? "—"}
                        {item.geobr_municipality_ibge_code ? ` · ${item.geobr_municipality_ibge_code}` : ""}
                      </p>
                    </div>
                  </TableCell>
                  <TableCell className="px-4 py-3 align-top">
                    <div className="font-mono text-xs text-gray-600 dark:text-gray-400">
                      <p>lat {formatCoordinate(item.latitude)}</p>
                      <p>lon {formatCoordinate(item.longitude)}</p>
                    </div>
                  </TableCell>
                  <TableCell className="px-4 py-3 align-top">
                    <div className="text-sm text-gray-700 dark:text-gray-300">
                      <p>{item.source_collection}</p>
                      <p className="font-mono text-xs text-gray-500 dark:text-gray-400">#{item.source_record_id}</p>
                    </div>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>

        {!data?.results.length && !loading ? (
          <p className="px-4 pb-4 text-sm text-gray-500 dark:text-gray-400">Nenhum registro encontrado para o filtro atual.</p>
        ) : null}
        {loading ? <p className="px-4 pb-4 text-sm text-gray-400 dark:text-gray-600">Carregando...</p> : null}

        <div className="flex justify-end">
          <Pagination currentPage={currentPage} totalPages={totalPages} onPageChange={setCurrentPage} />
        </div>
      </ComponentCard>
    </div>
  );
}