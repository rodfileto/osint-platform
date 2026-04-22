import { Suspense, lazy, useEffect, useState } from "react";
import { isAxiosError } from "axios";
import ComponentCard from "@/components/common/ComponentCard";
import Badge from "@/components/ui/badge/Badge";
import { Table, TableBody, TableCell, TableHeader, TableRow } from "@/components/ui/table";
import Pagination from "@/components/tables/Pagination";
import apiClient from "@/lib/apiClient";

const CompanyNetworkGraph = lazy(() => import("@/components/cnpj/CompanyNetworkGraph"));

interface EstabelecimentoDetail {
  cnpj_completo: string;
  cnpj_ordem: string;
  cnpj_dv: string;
  identificador_matriz_filial: number;
  tipo_estabelecimento: string;
  nome_fantasia: string | null;
  situacao_cadastral: string | null;
  situacao_cadastral_display: string | null;
  data_situacao_cadastral: string | null;
  motivo_situacao_cadastral: string | null;
  motivo_situacao_cadastral_descricao: string | null;
  data_inicio_atividade: string | null;
  cnae_fiscal_principal: string | null;
  cnae_fiscal_principal_descricao: string | null;
  cnae_fiscal_secundaria: string | null;
  pais: string | null;
  pais_nome: string | null;
  endereco: {
    tipo_logradouro: string | null;
    logradouro: string | null;
    numero: string | null;
    complemento: string | null;
    bairro: string | null;
    cep: string | null;
    uf: string | null;
    municipio: string | null;
    municipio_nome: string | null;
  };
  contato: {
    ddd_telefone_1: string | null;
    ddd_telefone_2: string | null;
    ddd_fax: string | null;
    email: string | null;
  };
}

interface EmpresaDetailResponse {
  cnpj_basico: string;
  razao_social: string;
  natureza_juridica: number | null;
  natureza_juridica_descricao: string | null;
  qualificacao_responsavel: string | null;
  qualificacao_responsavel_descricao: string | null;
  capital_social: string | null;
  porte_empresa: string | null;
  porte_empresa_display: string | null;
  ente_federativo_responsavel: string | null;
  total_estabelecimentos: number;
  estabelecimentos_ativos: number;
  estabelecimentos: EstabelecimentoDetail[];
}

type EmpresaDetailProps = {
  cnpjBasico: string | null;
};

const PAGE_SIZE = 20;

function formatMoney(value: string | null): string {
  if (!value) {
    return "—";
  }

  const numericValue = Number(value);

  if (Number.isNaN(numericValue)) {
    return value;
  }

  return new Intl.NumberFormat("pt-BR", {
    style: "currency",
    currency: "BRL",
    minimumFractionDigits: 2,
  }).format(numericValue);
}

function InfoItem({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-xl border border-gray-200 px-3 py-2 dark:border-gray-800">
      <p className="text-xs uppercase tracking-wide text-gray-500 dark:text-gray-400">{label}</p>
      <p className="mt-1 text-sm font-medium text-gray-800 dark:text-white/90">{value}</p>
    </div>
  );
}

function GraphFallback() {
  return (
    <ComponentCard title="Rede Societária" desc="Grafo Empresa ↔ Sócios (Neo4j)">
      <p className="text-sm text-gray-500 dark:text-gray-400">Carregando grafo...</p>
    </ComponentCard>
  );
}

export default function EmpresaDetail({ cnpjBasico }: EmpresaDetailProps) {
  const [data, setData] = useState<EmpresaDetailResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [currentPage, setCurrentPage] = useState(1);

  useEffect(() => {
    const fetchDetail = async () => {
      if (!cnpjBasico) {
        setData(null);
        setError(null);
        setCurrentPage(1);
        return;
      }

      setLoading(true);
      setError(null);

      try {
        const response = await apiClient.get<EmpresaDetailResponse>(`/api/cnpj/empresa/${cnpjBasico}/`);
        setData(response.data);
        setCurrentPage(1);
      } catch (errorValue) {
        const message =
          isAxiosError(errorValue) && errorValue.response
            ? `Erro ${errorValue.response.status}: ${errorValue.response.statusText}`
            : "Erro de conexão ao carregar detalhes da empresa";

        setError(message);
        setData(null);
      } finally {
        setLoading(false);
      }
    };

    void fetchDetail();
  }, [cnpjBasico]);

  if (!cnpjBasico) {
    return null;
  }

  const totalPages = data ? Math.max(1, Math.ceil(data.estabelecimentos.length / PAGE_SIZE)) : 1;
  const safeCurrentPage = Math.min(currentPage, totalPages);
  const paginatedEstabelecimentos = data
    ? data.estabelecimentos.slice((safeCurrentPage - 1) * PAGE_SIZE, safeCurrentPage * PAGE_SIZE)
    : [];

  return (
    <div className="space-y-5">
      {loading ? <p className="text-sm text-gray-500 dark:text-gray-400">Carregando detalhes...</p> : null}

      {error ? (
        <div className="rounded-2xl border border-red-200 bg-red-50 px-5 py-4 text-sm text-red-700 dark:border-red-500/20 dark:bg-red-500/10 dark:text-red-400">
          {error}
        </div>
      ) : null}

      {!loading && !error && data ? (
        <>
          <ComponentCard title="Detalhes da Empresa" desc={`CNPJ básico: ${cnpjBasico}`}>
            <div className="space-y-5">
              <div className="grid grid-cols-1 gap-3 md:grid-cols-2 xl:grid-cols-3">
                <InfoItem label="Razão social" value={data.razao_social} />
                <InfoItem
                  label="Natureza jurídica"
                  value={data.natureza_juridica_descricao || String(data.natureza_juridica ?? "—")}
                />
                <InfoItem
                  label="Qualificação responsável"
                  value={data.qualificacao_responsavel_descricao || data.qualificacao_responsavel || "—"}
                />
                <InfoItem label="Porte" value={data.porte_empresa_display || data.porte_empresa || "—"} />
                <InfoItem label="Capital social" value={formatMoney(data.capital_social)} />
                <InfoItem label="Ente federativo" value={data.ente_federativo_responsavel || "—"} />
              </div>

              <div className="flex flex-wrap items-center gap-2">
                <Badge variant="light" color="info" size="sm">
                  Total estabelecimentos: {data.total_estabelecimentos}
                </Badge>
                <Badge variant="light" color="success" size="sm">
                  Ativos: {data.estabelecimentos_ativos}
                </Badge>
              </div>
            </div>
          </ComponentCard>

          <ComponentCard
            title="Estabelecimentos"
            desc={`${data.total_estabelecimentos.toLocaleString("pt-BR")} registro${data.total_estabelecimentos !== 1 ? "s" : ""}`}
          >
            <div className="space-y-4">
              <div className="overflow-x-auto">
                <Table className="table-auto">
                  <TableHeader>
                    <TableRow className="border-b border-gray-100 dark:border-gray-800">
                      {["CNPJ", "Tipo", "Nome Fantasia", "Situação", "UF", "Município", "CNAE"].map((header) => (
                        <TableCell
                          key={header}
                          isHeader
                          className="whitespace-nowrap px-4 py-3 text-left text-xs font-semibold uppercase tracking-wide text-gray-500 dark:text-gray-400"
                        >
                          {header}
                        </TableCell>
                      ))}
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {paginatedEstabelecimentos.map((estabelecimento) => (
                      <TableRow
                        key={`${estabelecimento.cnpj_ordem}${estabelecimento.cnpj_dv}`}
                        className="border-b border-gray-100 last:border-0 dark:border-gray-800"
                      >
                        <TableCell className="whitespace-nowrap px-4 py-3 font-mono text-sm text-gray-700 dark:text-gray-300">
                          {estabelecimento.cnpj_completo}
                        </TableCell>
                        <TableCell className="whitespace-nowrap px-4 py-3 text-sm text-gray-700 dark:text-gray-300">
                          {estabelecimento.tipo_estabelecimento}
                        </TableCell>
                        <TableCell className="px-4 py-3 text-sm text-gray-700 dark:text-gray-300">
                          {estabelecimento.nome_fantasia || "—"}
                        </TableCell>
                        <TableCell className="whitespace-nowrap px-4 py-3 text-sm text-gray-700 dark:text-gray-300">
                          {estabelecimento.situacao_cadastral_display || estabelecimento.situacao_cadastral || "—"}
                        </TableCell>
                        <TableCell className="whitespace-nowrap px-4 py-3 text-sm text-gray-700 dark:text-gray-300">
                          {estabelecimento.endereco.uf || "—"}
                        </TableCell>
                        <TableCell className="px-4 py-3 text-sm text-gray-700 dark:text-gray-300">
                          {estabelecimento.endereco.municipio_nome || estabelecimento.endereco.municipio || "—"}
                        </TableCell>
                        <TableCell className="px-4 py-3 text-sm text-gray-700 dark:text-gray-300">
                          {estabelecimento.cnae_fiscal_principal_descricao || estabelecimento.cnae_fiscal_principal || "—"}
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </div>

              {totalPages > 1 ? (
                <div className="flex justify-end border-t border-gray-100 pt-4 dark:border-gray-800">
                  <Pagination
                    currentPage={safeCurrentPage}
                    totalPages={totalPages}
                    onPageChange={(page) => setCurrentPage(page)}
                  />
                </div>
              ) : null}
            </div>
          </ComponentCard>
          <Suspense fallback={<GraphFallback />}>
            <CompanyNetworkGraph cnpjBasico={cnpjBasico} />
          </Suspense>
        </>
      ) : null}
    </div>
  );
}