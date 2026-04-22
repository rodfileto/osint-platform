import { useCallback, useRef, useState } from "react";
import { Link } from "react-router-dom";
import { isAxiosError } from "axios";
import ComponentCard from "@/components/common/ComponentCard";
import InputField from "@/components/form/input/InputField";
import Badge from "@/components/ui/badge/Badge";
import { Table, TableBody, TableCell, TableHeader, TableRow } from "@/components/ui/table";
import Pagination from "@/components/tables/Pagination";
import apiClient from "@/lib/apiClient";

interface PessoaResult {
  nome: string;
  cpf_cnpj_socio: string;
  faixa_etaria: number | null;
  total_empresas: number;
}

interface ApiResponse {
  count: number;
  next: string | null;
  previous: string | null;
  results: PessoaResult[];
}

const FAIXA_ETARIA_LABEL: Record<number, string> = {
  0: "NI",
  1: "<= 20",
  2: "21-30",
  3: "31-40",
  4: "41-50",
  5: "51-60",
  6: "61-70",
  7: "71-80",
  9: "> 80",
};

const PAGE_SIZE = 20;

export default function PessoaSearch() {
  const [query, setQuery] = useState("");
  const [data, setData] = useState<ApiResponse | null>(null);
  const [page, setPage] = useState(1);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const lastQuery = useRef("");

  const search = useCallback(async (searchQuery: string, nextPage: number) => {
    const trimmedQuery = searchQuery.trim();

    if (trimmedQuery.length < 3) {
      setError("Informe ao menos 3 caracteres para buscar por nome.");
      return;
    }

    setLoading(true);
    setError(null);

    try {
      const response = await apiClient.get<ApiResponse>("/api/cnpj/pessoa/search/", {
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
        title="Pesquisa de Pessoas"
        desc="Busque por nome. O drill-down usa o par nome + CPF mascarado retornado na busca."
      >
        <form onSubmit={handleSubmit} className="space-y-3">
          <div className="flex flex-col gap-3 md:flex-row">
            <div className="flex-1">
              <InputField
                id="pessoa-search-nome"
                type="text"
                placeholder="Nome do sócio (ex: JOAO DA SILVA)"
                value={query}
                onChange={(event) => setQuery(event.target.value)}
              />
            </div>
            <button
              type="submit"
              disabled={loading || query.trim().length < 3}
              className="inline-flex h-11 items-center justify-center rounded-lg bg-brand-500 px-5 text-sm font-medium text-white transition hover:bg-brand-600 disabled:cursor-not-allowed disabled:opacity-60"
            >
              {loading ? "Buscando..." : "Buscar"}
            </button>
          </div>
          <p className="text-xs text-gray-400 dark:text-gray-500">
            Os resultados já trazem o CPF mascarado necessário para abrir os detalhes e a rede da pessoa sem depender do CPF completo.
          </p>
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
          desc={`${data.count.toLocaleString("pt-BR")} pessoa${data.count !== 1 ? "s" : ""} encontrada${data.count !== 1 ? "s" : ""}`}
        >
          {data.results.length === 0 ? (
            <p className="py-6 text-center text-sm text-gray-500 dark:text-gray-400">
              Nenhum resultado encontrado para &ldquo;{lastQuery.current}&rdquo;.
            </p>
          ) : (
            <>
              <div className="mb-3 rounded-xl border border-sky-200 bg-sky-50 px-4 py-2.5 text-xs text-sky-700 dark:border-sky-500/20 dark:bg-sky-500/10 dark:text-sky-300">
                Cada linha representa uma identidade distinta conforme a chave disponível na base: nome + CPF mascarado.
              </div>
              <div className="overflow-x-auto">
                <Table className="table-auto">
                  <TableHeader>
                    <TableRow className="border-b border-gray-100 dark:border-gray-800">
                      {["Nome", "CPF mascarado", "Faixa etária", "Empresas", "Ações"].map((header) => (
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
                    {data.results.map((result, index) => {
                      const detailHref = `/pessoa/detalhe?nome=${encodeURIComponent(result.nome)}&cpf_mascarado=${encodeURIComponent(result.cpf_cnpj_socio)}`;

                      return (
                        <TableRow
                          key={`${result.cpf_cnpj_socio}-${result.nome}-${index}`}
                          className="border-b border-gray-100 transition-colors last:border-0 hover:bg-gray-50 dark:border-gray-800 dark:hover:bg-white/[0.02]"
                        >
                          <TableCell className="px-4 py-3 text-sm font-medium text-gray-800 dark:text-white/90">
                            {result.nome || "—"}
                          </TableCell>
                          <TableCell className="whitespace-nowrap px-4 py-3 font-mono text-sm text-gray-500 dark:text-gray-400">
                            {result.cpf_cnpj_socio || "—"}
                          </TableCell>
                          <TableCell className="whitespace-nowrap px-4 py-3">
                            {result.faixa_etaria != null ? (
                              <Badge variant="light" size="sm" color="info">
                                {FAIXA_ETARIA_LABEL[result.faixa_etaria] ?? String(result.faixa_etaria)}
                              </Badge>
                            ) : (
                              <span className="text-sm text-gray-400">—</span>
                            )}
                          </TableCell>
                          <TableCell className="whitespace-nowrap px-4 py-3 text-right text-sm font-semibold text-gray-700 dark:text-gray-300">
                            {result.total_empresas.toLocaleString("pt-BR")}
                          </TableCell>
                          <TableCell className="whitespace-nowrap px-4 py-3">
                            <Link
                              to={detailHref}
                              className="inline-flex items-center justify-center gap-2 rounded-lg bg-white px-4 py-3 text-sm font-medium text-gray-700 ring-1 ring-inset ring-gray-300 transition hover:bg-gray-50 dark:bg-gray-800 dark:text-gray-400 dark:ring-gray-700 dark:hover:bg-white/[0.03] dark:hover:text-gray-300"
                            >
                              Ver detalhes
                            </Link>
                          </TableCell>
                        </TableRow>
                      );
                    })}
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