import { useCallback, useRef, useState } from "react";
import { Link } from "react-router-dom";
import { isAxiosError } from "axios";
import ComponentCard from "@/components/common/ComponentCard";
import InputField from "@/components/form/input/InputField";
import Button from "@/components/ui/button/Button";
import Badge from "@/components/ui/badge/Badge";
import { Table, TableBody, TableCell, TableHeader, TableRow } from "@/components/ui/table";
import Pagination from "@/components/tables/Pagination";
import apiClient from "@/lib/apiClient";

interface CnpjResult {
  cnpj_14: string;
  cnpj_basico: string;
  razao_social: string;
  nome_fantasia: string | null;
  situacao_cadastral: string;
  codigo_municipio: string | null;
  municipio_nome: string | null;
  uf: string | null;
  cnae_fiscal_principal: string | null;
  cnae_descricao: string | null;
  porte_empresa: string | null;
  natureza_juridica: number | null;
  capital_social: string | null;
}

interface ApiResponse {
  count: number;
  next: string | null;
  previous: string | null;
  results: CnpjResult[];
}

function formatCnpj(cnpj: string): string {
  return cnpj.replace(/^(\d{2})(\d{3})(\d{3})(\d{4})(\d{2})$/, "$1.$2.$3/$4-$5");
}

function formatCapital(value: string | null): string {
  if (!value || value === "0.00") {
    return "—";
  }

  const numericValue = Number.parseFloat(value);

  if (numericValue >= 1_000_000_000) {
    return `R$ ${(numericValue / 1_000_000_000).toFixed(1)}B`;
  }

  if (numericValue >= 1_000_000) {
    return `R$ ${(numericValue / 1_000_000).toFixed(1)}M`;
  }

  if (numericValue >= 1_000) {
    return `R$ ${(numericValue / 1_000).toFixed(0)}K`;
  }

  return `R$ ${numericValue.toFixed(2)}`;
}

const PORTE_LABEL: Record<string, string> = {
  "00": "NI",
  "01": "ME",
  "03": "EPP",
  "05": "Grande",
};

const PAGE_SIZE = 20;

export default function CnpjSearch() {
  const [query, setQuery] = useState("");
  const [data, setData] = useState<ApiResponse | null>(null);
  const [page, setPage] = useState(1);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const lastQuery = useRef("");

  const search = useCallback(async (searchQuery: string, nextPage: number) => {
    const trimmedQuery = searchQuery.trim();

    if (!trimmedQuery) {
      return;
    }

    setLoading(true);
    setError(null);

    try {
      const response = await apiClient.get<ApiResponse>("/api/cnpj/search/", {
        params: { q: trimmedQuery, page: nextPage },
      });

      setData(response.data);
      lastQuery.current = trimmedQuery;
    } catch (errorValue) {
      const message =
        isAxiosError(errorValue) && errorValue.response
          ? `Erro ${errorValue.response.status}: ${errorValue.response.statusText}`
          : "Erro de conexão com o servidor";

      setError(message);
      setData(null);
    } finally {
      setLoading(false);
    }
  }, []);

  const handleSubmit = (event: React.FormEvent) => {
    event.preventDefault();
    setPage(1);
    void search(query, 1);
  };

  const handlePageChange = (nextPage: number) => {
    setPage(nextPage);
    void search(lastQuery.current, nextPage);
    window.scrollTo({ top: 0, behavior: "smooth" });
  };

  const totalPages = data ? Math.ceil(data.count / PAGE_SIZE) : 0;

  return (
    <div className="space-y-4">
      <ComponentCard
        title="Pesquisa de CNPJ"
        desc="Busque por razão social, nome fantasia ou CNPJ (14 dígitos)"
      >
        <form onSubmit={handleSubmit} className="flex flex-col gap-3 md:flex-row">
          <div className="flex-1">
            <InputField
              id="cnpj-search-input"
              type="text"
              placeholder="Ex: Petrobras  ou  33.000.167/0001-01"
              value={query}
              onChange={(event) => setQuery(event.target.value)}
            />
          </div>
          <Button type="submit" size="md" variant="primary" disabled={loading || !query.trim()}>
            {loading ? "Buscando..." : "Buscar"}
          </Button>
        </form>
      </ComponentCard>

      {error ? (
        <div className="rounded-2xl border border-red-200 bg-red-50 px-5 py-4 text-sm text-red-700 dark:border-red-500/20 dark:bg-red-500/10 dark:text-red-400">
          {error}
        </div>
      ) : null}

      {data ? (
        <ComponentCard
          title="Resultados"
          desc={`${data.count.toLocaleString("pt-BR")} empresa${data.count !== 1 ? "s" : ""} encontrada${data.count !== 1 ? "s" : ""}`}
        >
          {data.results.length === 0 ? (
            <p className="py-6 text-center text-sm text-gray-500 dark:text-gray-400">
              Nenhum resultado encontrado para &ldquo;{lastQuery.current}&rdquo;.
            </p>
          ) : (
            <>
              <div className="overflow-x-auto">
                <Table className="table-auto">
                  <TableHeader>
                    <TableRow className="border-b border-gray-100 dark:border-gray-800">
                      {["CNPJ", "Razão Social / Fantasia", "UF", "CNAE", "Porte", "Capital", "Ações"].map((header) => (
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
                    {data.results.map((result) => (
                      <TableRow
                        key={result.cnpj_14}
                        className="border-b border-gray-100 transition-colors last:border-0 hover:bg-gray-50 dark:border-gray-800 dark:hover:bg-white/[0.02]"
                      >
                        <TableCell className="whitespace-nowrap px-4 py-3 font-mono text-sm text-gray-700 dark:text-gray-300">
                          {formatCnpj(result.cnpj_14)}
                        </TableCell>

                        <TableCell className="max-w-xs px-4 py-3">
                          <p className="truncate text-sm font-medium text-gray-800 dark:text-white/90">
                            {result.razao_social}
                          </p>
                          {result.nome_fantasia ? (
                            <p className="truncate text-xs text-gray-500 dark:text-gray-400">{result.nome_fantasia}</p>
                          ) : null}
                        </TableCell>

                        <TableCell className="whitespace-nowrap px-4 py-3 text-sm text-gray-600 dark:text-gray-400">
                          {result.uf ?? "—"}
                        </TableCell>

                        <TableCell className="whitespace-nowrap px-4 py-3 font-mono text-sm text-gray-600 dark:text-gray-400">
                          {result.cnae_descricao || result.cnae_fiscal_principal || "—"}
                        </TableCell>

                        <TableCell className="whitespace-nowrap px-4 py-3">
                          {result.porte_empresa ? (
                            <Badge
                              variant="light"
                              size="sm"
                              color={
                                result.porte_empresa === "05"
                                  ? "primary"
                                  : result.porte_empresa === "03"
                                    ? "info"
                                    : result.porte_empresa === "01"
                                      ? "success"
                                      : "light"
                              }
                            >
                              {PORTE_LABEL[result.porte_empresa] ?? result.porte_empresa}
                            </Badge>
                          ) : (
                            <span className="text-sm text-gray-400">—</span>
                          )}
                        </TableCell>

                        <TableCell className="whitespace-nowrap px-4 py-3 text-right text-sm text-gray-600 dark:text-gray-400">
                          {formatCapital(result.capital_social)}
                        </TableCell>

                        <TableCell className="whitespace-nowrap px-4 py-3">
                          <Link
                            to={`/cnpj/${result.cnpj_basico}`}
                            className="inline-flex items-center justify-center gap-2 rounded-lg bg-white px-4 py-3 text-sm font-medium text-gray-700 ring-1 ring-inset ring-gray-300 transition hover:bg-gray-50 dark:bg-gray-800 dark:text-gray-400 dark:ring-gray-700 dark:hover:bg-white/[0.03] dark:hover:text-gray-300"
                          >
                            Ver detalhes
                          </Link>
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </div>

              {totalPages > 1 ? (
                <div className="flex justify-end border-t border-gray-100 pt-4 dark:border-gray-800">
                  <Pagination currentPage={page} totalPages={totalPages} onPageChange={handlePageChange} />
                </div>
              ) : null}
            </>
          )}
        </ComponentCard>
      ) : null}
    </div>
  );
}