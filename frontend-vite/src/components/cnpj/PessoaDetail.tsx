import { Suspense, lazy, useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { isAxiosError } from "axios";
import ComponentCard from "@/components/common/ComponentCard";
import Badge from "@/components/ui/badge/Badge";
import { Table, TableBody, TableCell, TableHeader, TableRow } from "@/components/ui/table";
import Pagination from "@/components/tables/Pagination";
import apiClient from "@/lib/apiClient";

const PersonNetworkGraph = lazy(() => import("@/components/cnpj/PersonNetworkGraph"));

interface EmpresaSocio {
  cnpj_basico: string;
  razao_social: string;
  qualificacao_socio: string | null;
  qualificacao_socio_descricao: string | null;
  data_entrada_sociedade: string | null;
  situacao_cadastral: string | null;
  uf: string | null;
  municipio_nome: string | null;
  reference_month: string;
}

interface PessoaDetailResponse {
  cpf_mascarado: string;
  nome: string | null;
  faixa_etaria: number | null;
  total_empresas: number;
  empresas: EmpresaSocio[];
}

type PessoaDetailProps = {
  cpfMascarado: string;
  nome: string;
};

const FAIXA_ETARIA_LABEL: Record<number, string> = {
  0: "Não informado",
  1: "até 20 anos",
  2: "21 a 30 anos",
  3: "31 a 40 anos",
  4: "41 a 50 anos",
  5: "51 a 60 anos",
  6: "61 a 70 anos",
  7: "71 a 80 anos",
  9: "mais de 80 anos",
};

const SITUACAO_LABEL: Record<string, string> = {
  "01": "Nula",
  "02": "Ativa",
  "03": "Suspensa",
  "04": "Inapta",
  "08": "Baixada",
};

const SITUACAO_COLOR: Record<string, "success" | "error" | "warning" | "light"> = {
  "02": "success",
  "03": "warning",
  "04": "warning",
  "08": "error",
  "01": "light",
};

const PAGE_SIZE = 20;

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
    <ComponentCard title="Rede de Co-propriedade" desc="Pessoa ↔ Empresas ↔ Co-sócios (Neo4j)">
      <p className="text-sm text-gray-500 dark:text-gray-400">Carregando grafo...</p>
    </ComponentCard>
  );
}

export default function PessoaDetail({ cpfMascarado, nome }: PessoaDetailProps) {
  const [data, setData] = useState<PessoaDetailResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [currentPage, setCurrentPage] = useState(1);

  useEffect(() => {
    const fetchDetail = async () => {
      setLoading(true);
      setError(null);

      try {
        const response = await apiClient.get<PessoaDetailResponse>("/api/cnpj/pessoa/detail/", {
          params: {
            nome,
            cpf_mascarado: cpfMascarado,
          },
        });

        setData(response.data);
        setCurrentPage(1);
      } catch (errorValue) {
        const message =
          isAxiosError(errorValue) && errorValue.response
            ? `Erro ${errorValue.response.status}: ${errorValue.response.statusText}`
            : "Erro de conexão ao carregar detalhes da pessoa";

        setError(message);
        setData(null);
      } finally {
        setLoading(false);
      }
    };

    void fetchDetail();
  }, [cpfMascarado, nome]);

  const totalPages = data ? Math.max(1, Math.ceil(data.empresas.length / PAGE_SIZE)) : 1;
  const safePage = Math.min(currentPage, totalPages);
  const paginatedEmpresas = data
    ? data.empresas.slice((safePage - 1) * PAGE_SIZE, safePage * PAGE_SIZE)
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
          <ComponentCard title="Detalhes da Pessoa" desc={`CPF mascarado: ${data.cpf_mascarado}`}>
            <div className="grid grid-cols-1 gap-3 md:grid-cols-2 xl:grid-cols-4">
              <InfoItem label="Nome" value={data.nome || "—"} />
              <InfoItem label="CPF mascarado" value={data.cpf_mascarado} />
              <InfoItem
                label="Faixa etária"
                value={data.faixa_etaria != null ? FAIXA_ETARIA_LABEL[data.faixa_etaria] ?? String(data.faixa_etaria) : "—"}
              />
              <InfoItem label="Empresas no QSA" value={data.total_empresas.toLocaleString("pt-BR")} />
            </div>
          </ComponentCard>

          <ComponentCard
            title="Quadro Societário"
            desc={`${data.total_empresas.toLocaleString("pt-BR")} empresa${data.total_empresas !== 1 ? "s" : ""}`}
          >
            <div className="space-y-4">
              <div className="overflow-x-auto">
                <Table className="table-auto">
                  <TableHeader>
                    <TableRow className="border-b border-gray-100 dark:border-gray-800">
                      {["CNPJ básico", "Razão Social", "Qualificação", "Entrada", "Situação", "UF", "Município", "Ref.", "Ações"].map((header) => (
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
                    {paginatedEmpresas.map((empresa) => (
                      <TableRow
                        key={empresa.cnpj_basico}
                        className="border-b border-gray-100 transition-colors last:border-0 hover:bg-gray-50 dark:border-gray-800 dark:hover:bg-white/[0.02]"
                      >
                        <TableCell className="whitespace-nowrap px-4 py-3 font-mono text-sm text-gray-700 dark:text-gray-300">
                          {empresa.cnpj_basico}
                        </TableCell>
                        <TableCell className="max-w-xs px-4 py-3">
                          <p className="truncate text-sm font-medium text-gray-800 dark:text-white/90">{empresa.razao_social}</p>
                        </TableCell>
                        <TableCell className="px-4 py-3 text-sm text-gray-600 dark:text-gray-400">
                          {empresa.qualificacao_socio_descricao || empresa.qualificacao_socio || "—"}
                        </TableCell>
                        <TableCell className="whitespace-nowrap px-4 py-3 text-sm text-gray-600 dark:text-gray-400">
                          {empresa.data_entrada_sociedade ? new Date(empresa.data_entrada_sociedade).toLocaleDateString("pt-BR") : "—"}
                        </TableCell>
                        <TableCell className="whitespace-nowrap px-4 py-3">
                          {empresa.situacao_cadastral ? (
                            <Badge
                              variant="light"
                              size="sm"
                              color={SITUACAO_COLOR[empresa.situacao_cadastral] ?? "light"}
                            >
                              {SITUACAO_LABEL[empresa.situacao_cadastral] ?? empresa.situacao_cadastral}
                            </Badge>
                          ) : (
                            <span className="text-sm text-gray-400">—</span>
                          )}
                        </TableCell>
                        <TableCell className="whitespace-nowrap px-4 py-3 text-sm text-gray-600 dark:text-gray-400">
                          {empresa.uf || "—"}
                        </TableCell>
                        <TableCell className="px-4 py-3 text-sm text-gray-600 dark:text-gray-400">
                          {empresa.municipio_nome || "—"}
                        </TableCell>
                        <TableCell className="whitespace-nowrap px-4 py-3 font-mono text-xs text-gray-400">
                          {empresa.reference_month}
                        </TableCell>
                        <TableCell className="whitespace-nowrap px-4 py-3">
                          <Link
                            to={`/cnpj/${empresa.cnpj_basico}`}
                            className="inline-flex items-center justify-center gap-2 rounded-lg bg-white px-4 py-3 text-sm font-medium text-gray-700 ring-1 ring-inset ring-gray-300 transition hover:bg-gray-50 dark:bg-gray-800 dark:text-gray-400 dark:ring-gray-700 dark:hover:bg-white/[0.03] dark:hover:text-gray-300"
                          >
                            Ver empresa
                          </Link>
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </div>

              {totalPages > 1 ? (
                <div className="flex justify-end border-t border-gray-100 pt-4 dark:border-gray-800">
                  <Pagination currentPage={safePage} totalPages={totalPages} onPageChange={setCurrentPage} />
                </div>
              ) : null}
            </div>
          </ComponentCard>
          <Suspense fallback={<GraphFallback />}>
            <PersonNetworkGraph cpfMascarado={cpfMascarado} nome={nome} />
          </Suspense>
        </>
      ) : null}
    </div>
  );
}