import { useEffect, useMemo, useState } from "react";
import { isAxiosError } from "axios";
import ComponentCard from "@/components/common/ComponentCard";
import InputField from "@/components/form/input/InputField";
import Badge from "@/components/ui/badge/Badge";
import Button from "@/components/ui/button/Button";
import { Table, TableBody, TableCell, TableHeader, TableRow } from "@/components/ui/table";
import apiClient from "@/lib/apiClient";

interface InpiResumoGeral {
  total_patentes: number;
  por_tipo: { tipo_patente: string | null; total: number }[];
  por_secao_ipc: { ipc_secao_classe: string | null; total: number; titulo_en: string | null }[];
  por_ano_deposito: { ano: number | null; total: number }[];
  snapshot_date: string | null;
}

interface DepositanteEmpresa {
  cnpj_14: string;
  cnpj_basico: string;
  razao_social: string | null;
  nome_fantasia: string | null;
  situacao_cadastral: string | null;
  uf: string | null;
  municipio_nome: string | null;
  cnae_fiscal_principal: string | null;
  cnae_descricao: string | null;
  porte_empresa: string | null;
  capital_social: string | null;
  match_source: string | null;
}

interface MvPatenteSearch {
  codigo_interno: number;
  numero_inpi: string;
  tipo_patente: string | null;
  data_deposito: string | null;
  data_publicacao: string | null;
  sigilo: boolean;
  snapshot_date: string;
  titulo: string | null;
  inventor_principal: string | null;
  inventor_pais: string | null;
  depositante_principal: string | null;
  depositante_tipo: string | null;
  depositante_cnpj_basico: string | null;
  ipc_principal: string | null;
  ipc_secao_classe: string | null;
  depositante_empresa: DepositanteEmpresa | null;
}

interface PaginatedResponse<T> {
  count: number;
  next: string | null;
  previous: string | null;
  results: T[];
}

const TIPO_PATENTE_OPTIONS = [
  { value: "", label: "Todos os tipos" },
  { value: "PI", label: "PI - Patente de Invencao" },
  { value: "MU", label: "MU - Modelo de Utilidade" },
  { value: "PP", label: "PP - Protecao de Planta" },
  { value: "MI", label: "MI - Modelo de Invencao (legado)" },
];

function tipoLabel(tipo: string | null): string {
  const labels: Record<string, string> = {
    PI: "Invencao",
    MU: "Modelo de Utilidade",
    PP: "Protecao de Planta",
    MI: "Modelo de Invencao",
    DI: "Desenho Industrial",
  };

  return labels[tipo ?? ""] ?? tipo ?? "Desconhecido";
}

function tipoColor(tipo: string | null): "primary" | "success" | "warning" | "error" | "info" | "light" {
  const colors: Record<string, "primary" | "success" | "warning" | "error" | "info" | "light"> = {
    PI: "primary",
    MU: "success",
    PP: "info",
    MI: "warning",
    DI: "light",
  };

  return colors[tipo ?? ""] ?? "light";
}

function formatDate(value: string | null): string {
  if (!value) return "—";
  return new Intl.DateTimeFormat("pt-BR").format(new Date(value));
}

function barWidth(value: number, maxValue: number): string {
  if (!maxValue) return "0%";
  return `${Math.max(8, (value / maxValue) * 100)}%`;
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

export default function InpiOverview() {
  const [query, setQuery] = useState("");
  const [appliedQuery, setAppliedQuery] = useState("");
  const [selectedTipo, setSelectedTipo] = useState("");
  const [appliedTipo, setAppliedTipo] = useState("");
  const [ipcSecao, setIpcSecao] = useState("");
  const [appliedIpc, setAppliedIpc] = useState("");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [resumo, setResumo] = useState<InpiResumoGeral | null>(null);
  const [patentes, setPatentes] = useState<PaginatedResponse<MvPatenteSearch> | null>(null);

  useEffect(() => {
    const fetchData = async () => {
      setLoading(true);
      setError(null);

      const params: Record<string, string> = {};
      if (appliedQuery.trim()) params.q = appliedQuery.trim();
      if (appliedTipo) params.tipo_patente = appliedTipo;
      if (appliedIpc.trim()) params.ipc_secao = appliedIpc.trim().toUpperCase();

      try {
        const [resumoResponse, patentesResponse] = await Promise.all([
          apiClient.get<InpiResumoGeral>("/api/inpi/resumo/", { params }),
          apiClient.get<PaginatedResponse<MvPatenteSearch>>("/api/inpi/patentes/", { params }),
        ]);

        setResumo(resumoResponse.data);
        setPatentes(patentesResponse.data);
      } catch (fetchError) {
        const message =
          isAxiosError(fetchError) && fetchError.response
            ? `Erro ${fetchError.response.status}: ${fetchError.response.statusText}`
            : "Erro ao carregar dados do INPI.";
        setError(message);
      } finally {
        setLoading(false);
      }
    };

    void fetchData();
  }, [appliedQuery, appliedTipo, appliedIpc]);

  const topIpcMax = useMemo(() => Math.max(...(resumo?.por_secao_ipc.map((item) => item.total) ?? [0])), [resumo]);
  const topTipoMax = useMemo(() => Math.max(...(resumo?.por_tipo.map((item) => item.total) ?? [0])), [resumo]);

  const handleSubmit = (event: React.FormEvent) => {
    event.preventDefault();
    setAppliedQuery(query);
    setAppliedTipo(selectedTipo);
    setAppliedIpc(ipcSecao);
  };

  const handleReset = () => {
    setQuery("");
    setSelectedTipo("");
    setIpcSecao("");
    setAppliedQuery("");
    setAppliedTipo("");
    setAppliedIpc("");
  };

  return (
    <div className="space-y-6">
      <ComponentCard title="Radar INPI - Patentes" desc="Exploracao do acervo de patentes do Instituto Nacional da Propriedade Industrial.">
        <form className="flex flex-col gap-3 lg:flex-row lg:items-end" onSubmit={handleSubmit}>
          <div className="flex-1">
            <InputField
              id="inpi-query"
              type="text"
              placeholder="Buscar por titulo, depositante ou inventor"
              value={query}
              onChange={(event) => setQuery(event.target.value)}
            />
          </div>
          <div className="w-full lg:w-56">
            <label className="mb-1 block text-xs font-medium uppercase tracking-wide text-gray-500 dark:text-gray-400">Tipo de Patente</label>
            <select
              className="h-11 w-full rounded-lg border border-gray-300 bg-white px-4 text-sm text-gray-800 outline-none transition focus:border-brand-500 dark:border-gray-700 dark:bg-gray-900 dark:text-white/90"
              value={selectedTipo}
              onChange={(event) => setSelectedTipo(event.target.value)}
            >
              {TIPO_PATENTE_OPTIONS.map((option) => (
                <option key={option.value || "all"} value={option.value}>{option.label}</option>
              ))}
            </select>
          </div>
          <div className="w-full lg:w-40">
            <label className="mb-1 block text-xs font-medium uppercase tracking-wide text-gray-500 dark:text-gray-400">Secao IPC</label>
            <InputField
              id="inpi-ipc"
              type="text"
              placeholder="Ex: F41A"
              value={ipcSecao}
              onChange={(event) => setIpcSecao(event.target.value)}
            />
          </div>
          <div className="flex gap-3">
            <Button type="submit" disabled={loading}>{loading ? "Atualizando..." : "Aplicar"}</Button>
            <Button type="button" variant="outline" onClick={handleReset}>Limpar</Button>
          </div>
        </form>

        {resumo ? (
          <div className="mt-4 flex flex-wrap items-center gap-2">
            <span className="text-xs text-gray-400 dark:text-gray-500">
              Snapshot: <span className="font-medium text-gray-600 dark:text-gray-300">{resumo.snapshot_date ?? "—"}</span>
            </span>
          </div>
        ) : null}
      </ComponentCard>

      {error ? (
        <div className="rounded-2xl border border-red-200 bg-red-50 px-5 py-4 text-sm text-red-700 dark:border-red-500/20 dark:bg-red-500/10 dark:text-red-400">
          {error}
        </div>
      ) : null}

      <div className="grid grid-cols-1 gap-4 md:grid-cols-2 xl:grid-cols-3">
        <MetricCard
          title="Total de Patentes"
          value={loading || !resumo ? "..." : resumo.total_patentes.toLocaleString("pt-BR")}
          accentClass="bg-brand-50 text-brand-500 dark:bg-brand-500/15 dark:text-brand-400"
        />
        <MetricCard
          title="Tipos distintos"
          value={loading || !resumo ? "..." : String(resumo.por_tipo.length)}
          note={resumo?.por_tipo.map((item) => `${item.tipo_patente ?? "?"}: ${item.total.toLocaleString("pt-BR")}`).join(" · ")}
          accentClass="bg-emerald-50 text-emerald-600 dark:bg-emerald-500/15 dark:text-emerald-500"
        />
        <MetricCard
          title="Resultados encontrados"
          value={loading || !patentes ? "..." : patentes.count.toLocaleString("pt-BR")}
          note={appliedQuery || appliedTipo || appliedIpc ? "Com filtros ativos" : "Sem filtros"}
          accentClass="bg-amber-50 text-amber-600 dark:bg-amber-500/15 dark:text-amber-400"
        />
      </div>

      <div className="grid grid-cols-1 gap-6 xl:grid-cols-12">
        <ComponentCard title="Por Tipo de Patente" desc="Distribuicao do acervo por modalidade de protecao." className="xl:col-span-5">
          <div className="space-y-4">
            {(resumo?.por_tipo ?? []).map((item) => (
              <div key={item.tipo_patente ?? "null"} className="space-y-2">
                <div className="flex items-center justify-between gap-3">
                  <div className="flex items-center gap-2">
                    <Badge color={tipoColor(item.tipo_patente)} size="sm">{item.tipo_patente ?? "?"}</Badge>
                    <span className="text-sm font-medium text-gray-700 dark:text-white/80">{tipoLabel(item.tipo_patente)}</span>
                  </div>
                  <span className="text-sm font-semibold text-gray-800 dark:text-white/90">{item.total.toLocaleString("pt-BR")}</span>
                </div>
                <div className="h-2 rounded-full bg-gray-100 dark:bg-gray-800">
                  <div className="h-2 rounded-full bg-brand-500" style={{ width: barWidth(item.total, topTipoMax) }} />
                </div>
              </div>
            ))}
            {!resumo?.por_tipo.length && !loading ? <p className="text-sm text-gray-500 dark:text-gray-400">Nenhum dado disponível.</p> : null}
          </div>
        </ComponentCard>

        <ComponentCard title="Principais Secoes IPC" desc="Areas tecnicas mais representadas (top 10 por secao/classe IPC)." className="xl:col-span-7">
          <div className="space-y-4">
            {(resumo?.por_secao_ipc ?? []).slice(0, 10).map((item) => (
              <div key={item.ipc_secao_classe ?? "null"} className="space-y-2">
                <div className="flex items-center justify-between gap-3">
                  <div>
                    <span className="font-mono text-sm font-medium text-gray-800 dark:text-white/90">{item.ipc_secao_classe ?? "Sem IPC"}</span>
                    {item.titulo_en ? <p className="mt-0.5 max-w-xs truncate text-xs text-gray-500 dark:text-gray-400">{item.titulo_en}</p> : null}
                  </div>
                  <span className="shrink-0 text-sm text-gray-600 dark:text-gray-400">{item.total.toLocaleString("pt-BR")}</span>
                </div>
                <div className="h-2 rounded-full bg-gray-100 dark:bg-gray-800">
                  <div className="h-2 rounded-full bg-emerald-500" style={{ width: barWidth(item.total, topIpcMax) }} />
                </div>
              </div>
            ))}
            {!resumo?.por_secao_ipc.length && !loading ? <p className="text-sm text-gray-500 dark:text-gray-400">Nenhuma classificacao IPC encontrada.</p> : null}
          </div>
        </ComponentCard>
      </div>

      <ComponentCard title="Depositos por Ano" desc="Volume de pedidos de patente por ano de deposito.">
        <div className="flex min-h-20 items-end gap-1 overflow-x-auto pb-2">
          {(resumo?.por_ano_deposito ?? []).map((item) => {
            const maxValue = Math.max(...(resumo?.por_ano_deposito.map((entry) => entry.total) ?? [1]), 1);
            const height = Math.max(8, (item.total / maxValue) * 72);

            return (
              <div key={item.ano ?? "null"} className="group relative flex min-w-7 flex-col items-center gap-1">
                <div className="w-5 rounded-sm bg-brand-400 transition-all group-hover:bg-brand-600 dark:bg-brand-500 dark:group-hover:bg-brand-400" style={{ height }} />
                <span className="text-[10px] text-gray-400 dark:text-gray-600">{item.ano ?? "?"}</span>
                <div className="pointer-events-none absolute bottom-full mb-1 hidden rounded bg-gray-800 px-2 py-1 text-xs text-white group-hover:block">
                  {item.ano}: {item.total.toLocaleString("pt-BR")}
                </div>
              </div>
            );
          })}
          {!resumo?.por_ano_deposito.length && !loading ? <p className="text-sm text-gray-500 dark:text-gray-400">Nenhum dado disponivel.</p> : null}
        </div>
      </ComponentCard>

      <ComponentCard
        title="Patentes Encontradas"
        desc={patentes ? `${patentes.count.toLocaleString("pt-BR")} resultado(s) - exibindo os primeiros 20` : "Aguardando dados..."}
      >
        <div className="overflow-x-auto">
          <Table className="table-auto">
            <TableHeader>
              <TableRow className="border-b border-gray-100 dark:border-gray-800">
                {["Numero INPI", "Titulo", "Tipo", "IPC", "Depositante", "Deposito"].map((header) => (
                  <TableCell key={header} isHeader className="px-4 py-3 text-left text-xs font-semibold uppercase tracking-wide text-gray-500 dark:text-gray-400">
                    {header}
                  </TableCell>
                ))}
              </TableRow>
            </TableHeader>
            <TableBody>
              {(patentes?.results ?? []).map((patente) => (
                <TableRow key={patente.codigo_interno} className="border-b border-gray-100 last:border-0 dark:border-gray-800">
                  <TableCell className="px-4 py-3">
                    <span className="font-mono text-sm font-medium text-gray-800 dark:text-white/90">{patente.numero_inpi}</span>
                    {patente.sigilo ? (
                      <span className="ml-2 inline-flex"><Badge color="error" size="sm">Sigiloso</Badge></span>
                    ) : null}
                  </TableCell>
                  <TableCell className="px-4 py-3">
                    <p className="max-w-xs truncate text-sm text-gray-700 dark:text-white/80">{patente.titulo ?? "—"}</p>
                    {patente.inventor_principal ? (
                      <p className="mt-0.5 text-xs text-gray-400 dark:text-gray-600">
                        Inventor: {patente.inventor_principal}{patente.inventor_pais ? ` (${patente.inventor_pais})` : ""}
                      </p>
                    ) : null}
                  </TableCell>
                  <TableCell className="px-4 py-3">
                    {patente.tipo_patente ? <Badge color={tipoColor(patente.tipo_patente)} size="sm">{patente.tipo_patente}</Badge> : <span className="text-sm text-gray-400">—</span>}
                  </TableCell>
                  <TableCell className="px-4 py-3"><span className="font-mono text-xs text-gray-600 dark:text-gray-400">{patente.ipc_principal ?? "—"}</span></TableCell>
                  <TableCell className="px-4 py-3">
                    <div>
                      <p className="max-w-[200px] truncate text-sm text-gray-700 dark:text-white/80">{patente.depositante_empresa?.razao_social ?? patente.depositante_principal ?? "—"}</p>
                      {patente.depositante_empresa ? (
                        <p className="text-xs text-gray-400 dark:text-gray-600">
                          {patente.depositante_empresa.uf ?? ""}{patente.depositante_empresa.cnae_descricao ? ` · ${patente.depositante_empresa.cnae_descricao}` : ""}
                        </p>
                      ) : !patente.depositante_empresa && patente.depositante_tipo ? (
                        <p className="text-xs text-gray-400 dark:text-gray-600">{patente.depositante_tipo}</p>
                      ) : null}
                    </div>
                  </TableCell>
                  <TableCell className="px-4 py-3 text-sm text-gray-600 dark:text-gray-400">{formatDate(patente.data_deposito)}</TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
        {!patentes?.results.length && !loading ? <p className="px-4 pb-4 text-sm text-gray-500 dark:text-gray-400">Nenhuma patente encontrada para o filtro atual.</p> : null}
        {loading ? <p className="px-4 pb-4 text-sm text-gray-400 dark:text-gray-600">Carregando...</p> : null}
      </ComponentCard>
    </div>
  );
}