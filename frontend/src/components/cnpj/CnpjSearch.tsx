"use client";

import React, { useState, useCallback, useRef } from "react";
import ComponentCard from "@/components/common/ComponentCard";
import Input from "@/components/form/input/InputField";
import Button from "@/components/ui/button/Button";
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

interface CnpjResult {
  cnpj_14: string;
  cnpj_basico: string;
  razao_social: string;
  nome_fantasia: string | null;
  situacao_cadastral: number;
  municipio: string | null;
  uf: string | null;
  cnae_fiscal_principal: number | null;
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

// ─── Helpers ──────────────────────────────────────────────────────────────────

function formatCnpj(cnpj: string): string {
  return cnpj.replace(
    /^(\d{2})(\d{3})(\d{3})(\d{4})(\d{2})$/,
    "$1.$2.$3/$4-$5"
  );
}

function formatCapital(value: string | null): string {
  if (!value || value === "0.00") return "—";
  const n = parseFloat(value);
  if (n >= 1_000_000_000) return `R$ ${(n / 1_000_000_000).toFixed(1)}B`;
  if (n >= 1_000_000) return `R$ ${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `R$ ${(n / 1_000).toFixed(0)}K`;
  return `R$ ${n.toFixed(2)}`;
}

const PORTE_LABEL: Record<string, string> = {
  "00": "NI",
  "01": "ME",
  "03": "EPP",
  "05": "Grande",
};

const PAGE_SIZE = 20;

// ─── Component ────────────────────────────────────────────────────────────────

export default function CnpjSearch() {
  const [query, setQuery] = useState("");
  const [data, setData] = useState<ApiResponse | null>(null);
  const [page, setPage] = useState(1);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const lastQuery = useRef("");

  const search = useCallback(async (q: string, p: number) => {
    const trimmed = q.trim();
    if (!trimmed) return;

    setLoading(true);
    setError(null);

    try {
      const { data: json } = await apiClient.get<ApiResponse>(
        "/api/cnpj/search/",
        { params: { q: trimmed, page: p } }
      );
      setData(json);
      lastQuery.current = trimmed;
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
      {/* Search bar */}
      <ComponentCard
        title="Pesquisa de CNPJ"
        desc="Busque por razão social, nome fantasia ou CNPJ (14 dígitos)"
      >
        <form onSubmit={handleSubmit} className="flex gap-3">
          <div className="flex-1">
            <Input
              id="cnpj-search-input"
              type="text"
              placeholder="Ex: Petrobras  ou  33.000.167/0001-01"
              value={query}
              onChange={(e) => setQuery(e.target.value)}
            />
          </div>
          <Button
            type="submit"
            size="md"
            variant="primary"
            disabled={loading || !query.trim()}
          >
            {loading ? "Buscando…" : "Buscar"}
          </Button>
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
          title={`Resultados`}
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
                      {["CNPJ", "Razão Social / Fantasia", "UF", "CNAE", "Porte", "Capital"].map(
                        (h) => (
                          <TableCell
                            key={h}
                            isHeader
                            className="px-4 py-3 text-left text-xs font-semibold uppercase tracking-wide text-gray-500 dark:text-gray-400 whitespace-nowrap"
                          >
                            {h}
                          </TableCell>
                        )
                      )}
                    </TableRow>
                  </TableHeader>

                  <TableBody>
                    {data.results.map((r) => (
                      <TableRow
                        key={r.cnpj_14}
                        className="border-b border-gray-100 last:border-0 hover:bg-gray-50 dark:border-gray-800 dark:hover:bg-white/[0.02] transition-colors"
                      >
                        {/* CNPJ */}
                        <TableCell className="px-4 py-3 text-sm font-mono text-gray-700 dark:text-gray-300 whitespace-nowrap">
                          {formatCnpj(r.cnpj_14)}
                        </TableCell>

                        {/* Razão Social + Fantasia */}
                        <TableCell className="px-4 py-3 max-w-xs">
                          <p className="text-sm font-medium text-gray-800 dark:text-white/90 truncate">
                            {r.razao_social}
                          </p>
                          {r.nome_fantasia && (
                            <p className="text-xs text-gray-500 dark:text-gray-400 truncate">
                              {r.nome_fantasia}
                            </p>
                          )}
                        </TableCell>

                        {/* UF */}
                        <TableCell className="px-4 py-3 text-sm text-gray-600 dark:text-gray-400 whitespace-nowrap">
                          {r.uf ?? "—"}
                        </TableCell>

                        {/* CNAE */}
                        <TableCell className="px-4 py-3 text-sm font-mono text-gray-600 dark:text-gray-400 whitespace-nowrap">
                          {r.cnae_fiscal_principal ?? "—"}
                        </TableCell>

                        {/* Porte */}
                        <TableCell className="px-4 py-3 whitespace-nowrap">
                          {r.porte_empresa ? (
                            <Badge
                              variant="light"
                              size="sm"
                              color={
                                r.porte_empresa === "05"
                                  ? "primary"
                                  : r.porte_empresa === "03"
                                    ? "info"
                                    : r.porte_empresa === "01"
                                      ? "success"
                                      : "light"
                              }
                            >
                              {PORTE_LABEL[r.porte_empresa] ?? r.porte_empresa}
                            </Badge>
                          ) : (
                            <span className="text-sm text-gray-400">—</span>
                          )}
                        </TableCell>

                        {/* Capital */}
                        <TableCell className="px-4 py-3 text-sm text-gray-600 dark:text-gray-400 whitespace-nowrap text-right">
                          {formatCapital(r.capital_social)}
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </div>

              {/* Pagination */}
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
