"use client";

import React, { useState, useCallback, useRef } from "react";
import Link from "next/link";
import ComponentCard from "@/components/common/ComponentCard";
import Input from "@/components/form/input/InputField";
import Badge from "@/components/ui/badge/Badge";
import {
  Table,
  TableHeader,
  TableBody,
  TableRow,
  TableCell,
} from "@/components/ui/table";
import Pagination from "@/components/tables/Pagination";
import apiClient from "@/lib/apiClient";
import axios from "axios";

// ─── Types ────────────────────────────────────────────────────────────────────

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

// ─── Helpers ──────────────────────────────────────────────────────────────────

const FAIXA_ETARIA_LABEL: Record<number, string> = {
  0: "NI",
  1: "≤ 20",
  2: "21–30",
  3: "31–40",
  4: "41–50",
  5: "51–60",
  6: "61–70",
  7: "71–80",
  9: "> 80",
};

const PAGE_SIZE = 20;

// ─── Component ────────────────────────────────────────────────────────────────

export default function PessoaSearch() {
  const [query, setQuery] = useState("");
  const [data, setData] = useState<ApiResponse | null>(null);
  const [page, setPage] = useState(1);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const lastQuery = useRef("");

  const search = useCallback(async (q: string, p: number) => {
    const trimmedQ = q.trim();

    if (trimmedQ.length < 3) {
      setError("Informe ao menos 3 caracteres para buscar por nome.");
      return;
    }

    setLoading(true);
    setError(null);

    try {
      const params: Record<string, string | number> = { page: p };
      params.q = trimmedQ;

      const { data: json } = await apiClient.get<ApiResponse>(
        "/api/cnpj/pessoa/search/",
        { params }
      );
      setData(json);
      lastQuery.current = trimmedQ;
    } catch (e) {
      const msg =
        axios.isAxiosError(e) && e.response
          ? `Erro ${e.response.status}: ${e.response.statusText}`
          : "Erro de conexão com o servidor";
      setError(msg);
      setData(null);
    } finally {
      setLoading(false);
    }
  }, []);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    setPage(1);
    search(query, 1);
  };

  const handlePageChange = (p: number) => {
    setPage(p);
    search(lastQuery.current, p);
    window.scrollTo({ top: 0, behavior: "smooth" });
  };

  const totalPages = data ? Math.ceil(data.count / PAGE_SIZE) : 0;

  return (
    <div className="space-y-4">
      {/* Search form */}
      <ComponentCard
        title="Pesquisa de Pessoas"
        desc="Busque por nome. O drill-down usa o par nome + CPF mascarado retornado na busca."
      >
        <form onSubmit={handleSubmit} className="space-y-3">
          <div className="flex gap-3">
            <div className="flex-1">
              <Input
                id="pessoa-search-nome"
                type="text"
                placeholder="Nome do sócio  (ex: JOAO DA SILVA)"
                value={query}
                onChange={(e) => setQuery(e.target.value)}
              />
            </div>
            <button
              type="submit"
              disabled={loading || query.trim().length < 3}
              className="inline-flex h-11 items-center justify-center rounded-lg bg-brand-500 px-5 text-sm font-medium text-white transition hover:bg-brand-600 disabled:cursor-not-allowed disabled:opacity-60"
            >
              {loading ? "Buscando…" : "Buscar"}
            </button>
          </div>
          <p className="text-xs text-gray-400 dark:text-gray-500">
            Os resultados já trazem o CPF mascarado necessário para abrir os detalhes e a rede da pessoa sem depender do CPF completo.
          </p>
        </form>
      </ComponentCard>

      {/* Error */}
      {error && (
        <div className="rounded-2xl border border-error-200 bg-error-50 px-5 py-4 text-sm text-error-700 dark:border-error-500/20 dark:bg-error-500/10 dark:text-error-400">
          {error}
        </div>
      )}

      {/* Results */}
      {data && (
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
              <div className="mb-3 rounded-xl border border-info-200 bg-info-50 px-4 py-2.5 text-xs text-info-700 dark:border-info-500/20 dark:bg-info-500/10 dark:text-info-300">
                Cada linha representa uma identidade distinta conforme a chave disponível na base: nome + CPF mascarado.
              </div>
              <div className="overflow-x-auto">
                <Table className="table-auto">
                  <TableHeader>
                    <TableRow className="border-b border-gray-100 dark:border-gray-800">
                      {["Nome", "CPF mascarado", "Faixa etária", "Empresas", "Ações"].map((h) => (
                        <TableCell
                          key={h}
                          isHeader
                          className="px-4 py-3 text-left text-xs font-semibold uppercase tracking-wide text-gray-500 dark:text-gray-400 whitespace-nowrap"
                        >
                          {h}
                        </TableCell>
                      ))}
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {data.results.map((r, i) => {
                      const detailHref = `/pessoa/detalhe?nome=${encodeURIComponent(r.nome)}&cpf_mascarado=${encodeURIComponent(r.cpf_cnpj_socio)}`;
                      return (
                        <TableRow
                          key={`${r.cpf_cnpj_socio}-${r.nome}-${i}`}
                          className="border-b border-gray-100 last:border-0 hover:bg-gray-50 dark:border-gray-800 dark:hover:bg-white/[0.02] transition-colors"
                        >
                          {/* Nome */}
                          <TableCell className="px-4 py-3 text-sm font-medium text-gray-800 dark:text-white/90">
                            {r.nome || "—"}
                          </TableCell>

                          {/* CPF mascarado */}
                          <TableCell className="px-4 py-3 text-sm font-mono text-gray-500 dark:text-gray-400 whitespace-nowrap">
                            {r.cpf_cnpj_socio || "—"}
                          </TableCell>

                          {/* Faixa etária */}
                          <TableCell className="px-4 py-3 whitespace-nowrap">
                            {r.faixa_etaria != null ? (
                              <Badge variant="light" size="sm" color="info">
                                {FAIXA_ETARIA_LABEL[r.faixa_etaria] ?? String(r.faixa_etaria)}
                              </Badge>
                            ) : (
                              <span className="text-sm text-gray-400">—</span>
                            )}
                          </TableCell>

                          {/* Total empresas */}
                          <TableCell className="px-4 py-3 text-sm font-semibold text-gray-700 dark:text-gray-300 whitespace-nowrap text-right">
                            {r.total_empresas.toLocaleString("pt-BR")}
                          </TableCell>

                          {/* Ações */}
                          <TableCell className="px-4 py-3 whitespace-nowrap">
                            <Link
                              href={detailHref}
                              className="inline-flex items-center justify-center font-medium gap-2 rounded-lg transition px-4 py-3 text-sm bg-white text-gray-700 ring-1 ring-inset ring-gray-300 hover:bg-gray-50 dark:bg-gray-800 dark:text-gray-400 dark:ring-gray-700 dark:hover:bg-white/[0.03] dark:hover:text-gray-300"
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

              {totalPages > 1 && (
                <div className="flex justify-end pt-4 border-t border-gray-100 dark:border-gray-800">
                  <Pagination
                    currentPage={page}
                    totalPages={totalPages}
                    onPageChange={handlePageChange}
                  />
                </div>
              )}
            </>
          )}
        </ComponentCard>
      )}
    </div>
  );
}
