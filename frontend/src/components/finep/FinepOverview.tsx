"use client";

import React, { useEffect, useMemo, useState } from "react";
import axios from "axios";

import ComponentCard from "@/components/common/ComponentCard";
import FinepMunicipioResourceMap from "@/components/finep/FinepMunicipioResourceMap";
import type { FinepMunicipioMapFeatureCollection } from "@/components/finep/FinepMunicipioResourceMap";
import Input from "@/components/form/input/InputField";
import Badge from "@/components/ui/badge/Badge";
import Button from "@/components/ui/button/Button";
import {
  Table,
  TableBody,
  TableCell,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import apiClient from "@/lib/apiClient";
import {
  ArrowDownIcon,
  ArrowUpIcon,
  BoxCubeIcon,
  DollarLineIcon,
  GroupIcon,
  PieChartIcon,
} from "@/icons";

interface FinepResumoGeral {
  total_empresas: number;
  total_municipios: number;
  total_ufs: number;
  total_projetos: number;
  total_aprovado_finep: string;
  total_liberado: string;
  ticket_medio_projeto: string;
  gini_liberado: number;
  gini_por_uf: FinepGiniUf[];
  fontes: string[];
  projetos_por_fonte: Record<string, number>;
}

interface FinepGiniUf {
  uf: string;
  total_municipios: number;
  gini_liberado: number;
}

interface FinepResumoEmpresa {
  cnpj_14: string;
  razao_social: string | null;
  nome_fantasia: string | null;
  uf: string | null;
  municipio_nome: string | null;
  cnae_fiscal_principal: string | null;
  cnae_descricao: string | null;
  porte_empresa: string | null;
  natureza_juridica: number | null;
  natureza_juridica_descricao: string | null;
  capital_social: string | null;
  match_source: string | null;
  total_projetos: number;
  total_aprovado_finep: string;
  total_liberado: string;
  fontes: string[];
}

interface FinepResumoUf {
  uf: string;
  total_empresas: number;
  total_projetos: number;
  total_aprovado_finep: string;
  total_liberado: string;
  fontes: string[];
}

interface FinepResumoCnae {
  cnae_fiscal_principal: string | null;
  cnae_descricao: string | null;
  total_empresas: number;
  total_projetos: number;
  total_aprovado_finep: string;
  total_liberado: string;
  fontes: string[];
}

interface FinepResumoMunicipio {
  municipio_nome: string;
  uf: string | null;
  municipality_ibge_code: string | null;
  latitude: number | null;
  longitude: number | null;
  total_empresas: number;
  total_projetos: number;
  total_aprovado_finep: string;
  total_liberado: string;
  fontes: string[];
}

interface PaginatedResponse<T> {
  count: number;
  next: string | null;
  previous: string | null;
  results: T[];
}

const UF_OPTIONS = [
  "",
  "AC",
  "AL",
  "AM",
  "AP",
  "BA",
  "CE",
  "DF",
  "ES",
  "GO",
  "MA",
  "MG",
  "MS",
  "MT",
  "PA",
  "PB",
  "PE",
  "PI",
  "PR",
  "RJ",
  "RN",
  "RO",
  "RR",
  "RS",
  "SC",
  "SE",
  "SP",
  "TO",
  "NA",
];

function formatMoney(value: string | number | null | undefined): string {
  const amount = Number(value ?? 0);
  return new Intl.NumberFormat("pt-BR", {
    style: "currency",
    currency: "BRL",
    maximumFractionDigits: 0,
  }).format(amount);
}

function formatCompactMoney(value: string | number | null | undefined): string {
  const amount = Number(value ?? 0);
  return new Intl.NumberFormat("pt-BR", {
    style: "currency",
    currency: "BRL",
    notation: "compact",
    maximumFractionDigits: 1,
  }).format(amount);
}

function formatCnpj(cnpj: string): string {
  return cnpj.replace(
    /^(\d{2})(\d{3})(\d{3})(\d{4})(\d{2})$/,
    "$1.$2.$3/$4-$5"
  );
}

function sourceLabel(source: string): string {
  const labels: Record<string, string> = {
    ancine: "Ancine",
    credito_descentralizado: "Crédito Descentralizado",
    investimento: "Investimento",
    operacao_direta: "Operação Direta",
  };
  return labels[source] ?? source;
}

function formatCnaeLabel(
  cnaeDescricao: string | null | undefined,
  cnaeCodigo: string | null | undefined
): string {
  if (cnaeDescricao) {
    return cnaeDescricao;
  }

  if (cnaeCodigo) {
    return `CNAE ${cnaeCodigo}`;
  }

  return "CNAE não identificado";
}

function barWidth(value: string | number, maxValue: number): string {
  const current = Number(value);
  if (!maxValue || !Number.isFinite(current)) {
    return "0%";
  }

  return `${Math.max(8, (current / maxValue) * 100)}%`;
}

function formatGini(value: number | null | undefined): string {
  return new Intl.NumberFormat("pt-BR", {
    minimumFractionDigits: 3,
    maximumFractionDigits: 3,
  }).format(value ?? 0);
}

export default function FinepOverview() {
  const [query, setQuery] = useState("");
  const [appliedQuery, setAppliedQuery] = useState("");
  const [selectedUf, setSelectedUf] = useState("");
  const [appliedUf, setAppliedUf] = useState("");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [overview, setOverview] = useState<FinepResumoGeral | null>(null);
  const [companies, setCompanies] = useState<PaginatedResponse<FinepResumoEmpresa> | null>(null);
  const [ufs, setUfs] = useState<PaginatedResponse<FinepResumoUf> | null>(null);
  const [cnaes, setCnaes] = useState<PaginatedResponse<FinepResumoCnae> | null>(null);
  const [municipios, setMunicipios] = useState<PaginatedResponse<FinepResumoMunicipio> | null>(null);
  const [municipioMapData, setMunicipioMapData] = useState<FinepMunicipioMapFeatureCollection | null>(null);

  useEffect(() => {
    const fetchOverview = async () => {
      setLoading(true);
      setError(null);

      const params: Record<string, string> = {};
      if (appliedQuery.trim()) {
        params.q = appliedQuery.trim();
      }
      if (appliedUf) {
        params.uf = appliedUf;
      }

      try {
        const [overviewResponse, companiesResponse, ufResponse, cnaeResponse, municipioResponse, municipioMapResponse] = await Promise.all([
          apiClient.get<FinepResumoGeral>("/api/finep/resumo-geral/", { params }),
          apiClient.get<PaginatedResponse<FinepResumoEmpresa>>("/api/finep/resumo-empresa/", { params }),
          apiClient.get<PaginatedResponse<FinepResumoUf>>("/api/finep/resumo-uf/", { params }),
          apiClient.get<PaginatedResponse<FinepResumoCnae>>("/api/finep/resumo-cnae/", { params }),
          apiClient.get<PaginatedResponse<FinepResumoMunicipio>>("/api/finep/resumo-municipio/", { params }),
          apiClient.get<FinepMunicipioMapFeatureCollection>("/api/finep/mapa-municipio/", {
            params: { ...params, simplify_tolerance: "0.01" },
          }),
        ]);

        setOverview(overviewResponse.data);
        setCompanies(companiesResponse.data);
        setUfs(ufResponse.data);
        setCnaes(cnaeResponse.data);
        setMunicipios(municipioResponse.data);
        setMunicipioMapData(municipioMapResponse.data);
      } catch (fetchError) {
        const message =
          axios.isAxiosError(fetchError) && fetchError.response
            ? `Erro ${fetchError.response.status}: ${fetchError.response.statusText}`
            : "Erro ao carregar o panorama da FINEP.";
        setError(message);
      } finally {
        setLoading(false);
      }
    };

    fetchOverview();
  }, [appliedQuery, appliedUf]);

  const topUfMax = useMemo(() => {
    return Math.max(...(ufs?.results.map((item) => Number(item.total_aprovado_finep)) ?? [0]));
  }, [ufs]);

  const topCnaeMax = useMemo(() => {
    return Math.max(...(cnaes?.results.map((item) => Number(item.total_aprovado_finep)) ?? [0]));
  }, [cnaes]);

  const sourceBreakdown = useMemo(() => {
    if (!overview) {
      return [];
    }

    return Object.entries(overview.projetos_por_fonte).sort((left, right) => right[1] - left[1]);
  }, [overview]);

  const topMunicipioMax = useMemo(() => {
    return Math.max(...(municipios?.results.map((item) => Number(item.total_aprovado_finep)) ?? [0]));
  }, [municipios]);

  const handleSubmit = (event: React.FormEvent) => {
    event.preventDefault();
    setAppliedQuery(query);
    setAppliedUf(selectedUf);
  };

  const handleReset = () => {
    setQuery("");
    setSelectedUf("");
    setAppliedQuery("");
    setAppliedUf("");
  };

  return (
    <div className="space-y-6">
      <ComponentCard
        title="Radar FINEP"
        desc="Visão geral dos projetos apoiados, com recortes por empresa, UF e atividade econômica."
      >
        <form className="flex flex-col gap-3 lg:flex-row lg:items-end" onSubmit={handleSubmit}>
          <div className="flex-1">
            <Input
              id="finep-query"
              type="text"
              placeholder="Buscar por empresa, CNAE ou instrumento"
              value={query}
              onChange={(event) => setQuery(event.target.value)}
            />
          </div>
          <div className="w-full lg:w-40">
            <label className="mb-1 block text-xs font-medium uppercase tracking-wide text-gray-500 dark:text-gray-400">
              UF
            </label>
            <select
              className="h-11 w-full rounded-lg border border-gray-300 bg-white px-4 text-sm text-gray-800 outline-none transition focus:border-brand-500 dark:border-gray-700 dark:bg-gray-900 dark:text-white/90"
              value={selectedUf}
              onChange={(event) => setSelectedUf(event.target.value)}
            >
              {UF_OPTIONS.map((uf) => (
                <option key={uf || "all"} value={uf}>
                  {uf || "Todas as UFs"}
                </option>
              ))}
            </select>
          </div>
          <div className="flex gap-3">
            <Button type="submit" disabled={loading}>
              {loading ? "Atualizando..." : "Aplicar"}
            </Button>
            <Button type="button" variant="outline" onClick={handleReset}>
              Limpar
            </Button>
          </div>
        </form>

        {overview && (
          <div className="mt-4 flex flex-wrap gap-2">
            {overview.fontes.map((source) => (
              <Badge key={source} color="info" size="sm">
                {sourceLabel(source)}
              </Badge>
            ))}
          </div>
        )}
      </ComponentCard>

      {error && (
        <div className="rounded-2xl border border-error-200 bg-error-50 px-5 py-4 text-sm text-error-700 dark:border-error-500/20 dark:bg-error-500/10 dark:text-error-400">
          {error}
        </div>
      )}

      <div className="grid grid-cols-1 gap-4 md:grid-cols-2 xl:grid-cols-4">
        <div className="rounded-2xl border border-gray-200 bg-white p-5 dark:border-gray-800 dark:bg-white/[0.03]">
          <div className="flex h-12 w-12 items-center justify-center rounded-xl bg-brand-50 text-brand-500 dark:bg-brand-500/15 dark:text-brand-400">
            <GroupIcon className="size-6" />
          </div>
          <div className="mt-5">
            <p className="text-sm text-gray-500 dark:text-gray-400">Empresas apoiadas</p>
            <h3 className="mt-2 text-2xl font-semibold text-gray-800 dark:text-white/90">
              {loading || !overview ? "..." : overview.total_empresas.toLocaleString("pt-BR")}
            </h3>
          </div>
        </div>

        <div className="rounded-2xl border border-gray-200 bg-white p-5 dark:border-gray-800 dark:bg-white/[0.03]">
          <div className="flex h-12 w-12 items-center justify-center rounded-xl bg-success-50 text-success-600 dark:bg-success-500/15 dark:text-success-500">
            <BoxCubeIcon className="size-6" />
          </div>
          <div className="mt-5">
            <p className="text-sm text-gray-500 dark:text-gray-400">Projetos mapeados</p>
            <h3 className="mt-2 text-2xl font-semibold text-gray-800 dark:text-white/90">
              {loading || !overview ? "..." : overview.total_projetos.toLocaleString("pt-BR")}
            </h3>
          </div>
        </div>

        <div className="rounded-2xl border border-gray-200 bg-white p-5 dark:border-gray-800 dark:bg-white/[0.03]">
          <div className="flex h-12 w-12 items-center justify-center rounded-xl bg-warning-50 text-warning-600 dark:bg-warning-500/15 dark:text-warning-400">
            <DollarLineIcon className="size-6" />
          </div>
          <div className="mt-5">
            <p className="text-sm text-gray-500 dark:text-gray-400">Valor aprovado FINEP</p>
            <h3 className="mt-2 text-2xl font-semibold text-gray-800 dark:text-white/90">
              {loading || !overview ? "..." : formatCompactMoney(overview.total_aprovado_finep)}
            </h3>
            <p className="mt-2 text-xs text-gray-500 dark:text-gray-400">
              Ticket médio {loading || !overview ? "..." : formatCompactMoney(overview.ticket_medio_projeto)}
            </p>
          </div>
        </div>

        <div className="rounded-2xl border border-gray-200 bg-white p-5 dark:border-gray-800 dark:bg-white/[0.03]">
          <div className="flex h-12 w-12 items-center justify-center rounded-xl bg-blue-light-50 text-blue-light-500 dark:bg-blue-light-500/15 dark:text-blue-light-400">
            <PieChartIcon className="size-6" />
          </div>
          <div className="mt-5">
            <p className="text-sm text-gray-500 dark:text-gray-400">Valor liberado</p>
            <h3 className="mt-2 text-2xl font-semibold text-gray-800 dark:text-white/90">
              {loading || !overview ? "..." : formatCompactMoney(overview.total_liberado)}
            </h3>
            <p className="mt-2 text-xs text-gray-500 dark:text-gray-400">
              Cobertura territorial {loading || !overview ? "..." : `${overview.total_municipios} municípios em ${overview.total_ufs} UFs`}
            </p>
          </div>
        </div>
      </div>

      <div className="grid grid-cols-1 gap-6 xl:grid-cols-12">
        <ComponentCard
          title="Distribuição por Fonte"
          desc="Participação das linhas de apoio no total de projetos filtrados."
          className="xl:col-span-4"
        >
          <div className="space-y-4">
            {sourceBreakdown.map(([source, total]) => (
              <div key={source} className="rounded-xl border border-gray-100 p-4 dark:border-gray-800">
                <div className="flex items-center justify-between gap-3">
                  <span className="text-sm font-medium text-gray-800 dark:text-white/90">
                    {sourceLabel(source)}
                  </span>
                  <Badge color="light" size="sm">
                    {total.toLocaleString("pt-BR")} projetos
                  </Badge>
                </div>
              </div>
            ))}
            {!sourceBreakdown.length && (
              <p className="text-sm text-gray-500 dark:text-gray-400">Nenhum dado disponível.</p>
            )}
          </div>
        </ComponentCard>

        <ComponentCard
          title="Top UFs"
          desc="Estados com maior volume aprovado nos filtros atuais."
          className="xl:col-span-8"
        >
          <div className="space-y-4">
            {(ufs?.results ?? []).slice(0, 6).map((item) => (
              <div key={item.uf} className="space-y-2">
                <div className="flex items-center justify-between gap-3">
                  <div>
                    <p className="text-sm font-medium text-gray-800 dark:text-white/90">{item.uf}</p>
                    <p className="text-xs text-gray-500 dark:text-gray-400">
                      {item.total_empresas.toLocaleString("pt-BR")} empresas • {item.total_projetos.toLocaleString("pt-BR")} projetos
                    </p>
                  </div>
                  <div className="text-right">
                    <p className="text-sm font-semibold text-gray-800 dark:text-white/90">
                      {formatMoney(item.total_aprovado_finep)}
                    </p>
                    <p className="text-xs text-gray-500 dark:text-gray-400">
                      Liberado {formatCompactMoney(item.total_liberado)}
                    </p>
                  </div>
                </div>
                <div className="h-2 rounded-full bg-gray-100 dark:bg-gray-800">
                  <div
                    className="h-2 rounded-full bg-brand-500"
                    style={{ width: barWidth(item.total_aprovado_finep, topUfMax) }}
                  />
                </div>
              </div>
            ))}
            {!ufs?.results.length && !loading && (
              <p className="text-sm text-gray-500 dark:text-gray-400">Nenhuma UF encontrada.</p>
            )}
          </div>
        </ComponentCard>
      </div>

      <div className="grid grid-cols-1 gap-6 xl:grid-cols-12">
        <FinepMunicipioResourceMap featureCollection={municipioMapData} loading={loading} />

        <ComponentCard
          title="Concentração Territorial"
          desc="Coeficiente de Gini sobre a distribuição municipal dos valores liberados no recorte atual."
          className="xl:col-span-5"
        >
          <div className="rounded-xl border border-success-100 bg-success-50/60 p-4 dark:border-success-500/20 dark:bg-success-500/10">
            <p className="text-xs uppercase tracking-wide text-gray-500 dark:text-gray-400">Brasil · Liberado</p>
            <p className="mt-2 text-3xl font-semibold text-gray-800 dark:text-white/90">
              {loading || !overview ? "..." : formatGini(overview.gini_liberado)}
            </p>
            <p className="mt-2 text-xs text-gray-500 dark:text-gray-400">
              Quanto mais perto de 1, maior a concentração entre municípios que receberam desembolso.
            </p>
          </div>

          <div className="mt-5 space-y-3">
            <p className="text-xs font-medium uppercase tracking-wide text-gray-500 dark:text-gray-400">
              Gini por UF
            </p>
            {(overview?.gini_por_uf ?? []).slice(0, 6).map((item) => (
              <div key={item.uf} className="flex items-center justify-between gap-3 rounded-xl border border-gray-100 px-4 py-3 dark:border-gray-800">
                <div>
                  <p className="text-sm font-medium text-gray-800 dark:text-white/90">{item.uf}</p>
                  <p className="text-xs text-gray-500 dark:text-gray-400">
                    {item.total_municipios.toLocaleString("pt-BR")} municípios com registros
                  </p>
                </div>
                <div className="flex flex-col items-end gap-1 text-right">
                  <Badge color="success" size="sm">Liberado {formatGini(item.gini_liberado)}</Badge>
                </div>
              </div>
            ))}
          </div>
        </ComponentCard>

        <ComponentCard
          title="Municípios com Maior Volume"
          desc="Ranking municipal com leitura simultânea de valores aprovados e liberados."
          className="xl:col-span-7"
        >
          <div className="space-y-4">
            {(municipios?.results ?? []).slice(0, 8).map((item) => (
              <div key={`${item.uf ?? "na"}-${item.municipio_nome}`} className="space-y-2">
                <div className="flex items-center justify-between gap-3">
                  <div>
                    <p className="text-sm font-medium text-gray-800 dark:text-white/90">
                      {item.municipio_nome}
                    </p>
                    <p className="text-xs text-gray-500 dark:text-gray-400">
                      {item.uf ?? "Sem UF"} • {item.total_empresas.toLocaleString("pt-BR")} empresas • {item.total_projetos.toLocaleString("pt-BR")} projetos
                    </p>
                  </div>
                  <div className="text-right">
                    <p className="text-sm font-semibold text-gray-800 dark:text-white/90">
                      {formatMoney(item.total_aprovado_finep)}
                    </p>
                    <div className="mt-1 flex flex-col items-end gap-1">
                      <Badge color="primary" size="sm">Aprovado {formatCompactMoney(item.total_aprovado_finep)}</Badge>
                      <Badge color="success" size="sm">Liberado {formatCompactMoney(item.total_liberado)}</Badge>
                    </div>
                  </div>
                </div>
                <div className="h-2 rounded-full bg-gray-100 dark:bg-gray-800">
                  <div
                    className="h-2 rounded-full bg-warning-500"
                    style={{ width: barWidth(item.total_aprovado_finep, topMunicipioMax) }}
                  />
                </div>
              </div>
            ))}
            {!municipios?.results.length && !loading && (
              <p className="text-sm text-gray-500 dark:text-gray-400">Nenhum município encontrado.</p>
            )}
          </div>
        </ComponentCard>
      </div>

      <div className="grid grid-cols-1 gap-6 xl:grid-cols-12">
        <ComponentCard
          title="Atividades Econômicas"
          desc="CNAEs com maior volume aprovado nos dados FINEP filtrados."
          className="xl:col-span-5"
        >
          <div className="space-y-4">
            {(cnaes?.results ?? []).slice(0, 6).map((item) => (
              <div key={`${item.cnae_fiscal_principal ?? "na"}-${item.cnae_descricao ?? "sem-cnae"}`} className="space-y-2">
                <div className="flex items-start justify-between gap-3">
                  <div>
                    <p className="text-sm font-medium text-gray-800 dark:text-white/90">
                      {formatCnaeLabel(item.cnae_descricao, item.cnae_fiscal_principal)}
                    </p>
                    <p className="text-xs text-gray-500 dark:text-gray-400">
                      {item.cnae_fiscal_principal ?? "Sem código"} • {item.total_empresas.toLocaleString("pt-BR")} empresas
                    </p>
                  </div>
                  <span className="text-sm font-semibold text-gray-800 dark:text-white/90">
                    {formatCompactMoney(item.total_aprovado_finep)}
                  </span>
                </div>
                <div className="h-2 rounded-full bg-gray-100 dark:bg-gray-800">
                  <div
                    className="h-2 rounded-full bg-success-500"
                    style={{ width: barWidth(item.total_aprovado_finep, topCnaeMax) }}
                  />
                </div>
              </div>
            ))}
            {!cnaes?.results.length && !loading && (
              <p className="text-sm text-gray-500 dark:text-gray-400">Nenhum CNAE encontrado.</p>
            )}
          </div>
        </ComponentCard>

        <ComponentCard
          title="Empresas com Maior Valor Aprovado"
          desc="Ranking inicial para navegação e priorização analítica."
          className="xl:col-span-7"
        >
          <div className="overflow-x-auto">
            <Table className="table-auto">
              <TableHeader>
                <TableRow className="border-b border-gray-100 dark:border-gray-800">
                  {['Empresa', 'UF', 'Projetos', 'Aprovado', 'Sinal'].map((header) => (
                    <TableCell
                      key={header}
                      isHeader
                      className="px-4 py-3 text-left text-xs font-semibold uppercase tracking-wide text-gray-500 dark:text-gray-400"
                    >
                      {header}
                    </TableCell>
                  ))}
                </TableRow>
              </TableHeader>
              <TableBody>
                {(companies?.results ?? []).slice(0, 8).map((company) => {
                  const liberado = Number(company.total_liberado);
                  const aprovado = Number(company.total_aprovado_finep);
                  const adimplencia = aprovado > 0 ? liberado / aprovado : 0;

                  return (
                    <TableRow
                      key={company.cnpj_14}
                      className="border-b border-gray-100 last:border-0 dark:border-gray-800"
                    >
                      <TableCell className="px-4 py-3">
                        <div>
                          <p className="text-sm font-medium text-gray-800 dark:text-white/90">
                            {company.razao_social ?? formatCnpj(company.cnpj_14)}
                          </p>
                          <p className="text-xs text-gray-500 dark:text-gray-400">
                            {formatCnpj(company.cnpj_14)}
                            {` • ${formatCnaeLabel(company.cnae_descricao, company.cnae_fiscal_principal)}`}
                          </p>
                        </div>
                      </TableCell>
                      <TableCell className="px-4 py-3 text-sm text-gray-600 dark:text-gray-400">
                        {company.uf ?? "—"}
                      </TableCell>
                      <TableCell className="px-4 py-3 text-sm text-gray-600 dark:text-gray-400">
                        {company.total_projetos.toLocaleString("pt-BR")}
                      </TableCell>
                      <TableCell className="px-4 py-3 text-sm font-semibold text-gray-800 dark:text-white/90">
                        {formatMoney(company.total_aprovado_finep)}
                      </TableCell>
                      <TableCell className="px-4 py-3">
                        <Badge
                          color={adimplencia >= 0.7 ? "success" : adimplencia >= 0.4 ? "warning" : "error"}
                          size="sm"
                          startIcon={adimplencia >= 0.7 ? <ArrowUpIcon /> : <ArrowDownIcon />}
                        >
                          {`${Math.round(adimplencia * 100)}% liberado`}
                        </Badge>
                      </TableCell>
                    </TableRow>
                  );
                })}
              </TableBody>
            </Table>
          </div>

          {!companies?.results.length && !loading && (
            <p className="px-6 pb-6 text-sm text-gray-500 dark:text-gray-400">Nenhuma empresa encontrada para o filtro atual.</p>
          )}
        </ComponentCard>
      </div>
    </div>
  );
}