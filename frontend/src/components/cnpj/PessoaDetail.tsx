"use client";

import React, { useEffect, useState } from "react";
import Link from "next/link";
import axios from "axios";
import ComponentCard from "@/components/common/ComponentCard";
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
import PersonNetworkGraph from "@/components/cnpj/PersonNetworkGraph";

// ─── Types ────────────────────────────────────────────────────────────────────

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

interface PessoaDetailProps {
  cpfMascarado: string;
  nome: string;
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

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

const SITUACAO_COLOR: Record<
  string,
  "success" | "error" | "warning" | "light"
> = {
  "02": "success",
  "03": "warning",
  "04": "warning",
  "08": "error",
  "01": "light",
};

const PAGE_SIZE = 20;

// ─── Component ────────────────────────────────────────────────────────────────

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
        const params: Record<string, string> = {
          nome,
          cpf_mascarado: cpfMascarado,
        };
        const { data: detail } = await apiClient.get<PessoaDetailResponse>(
          "/api/cnpj/pessoa/detail/",
          { params }
        );
        setData(detail);
        setCurrentPage(1);
      } catch (e) {
        const msg =
          axios.isAxiosError(e) && e.response
            ? `Erro ${e.response.status}: ${e.response.statusText}`
            : "Erro de conexão ao carregar detalhes da pessoa";
        setError(msg);
        setData(null);
      } finally {
        setLoading(false);
      }
    };
    fetchDetail();
  }, [cpfMascarado, nome]);

  const totalPages = data
    ? Math.max(1, Math.ceil(data.empresas.length / PAGE_SIZE))
    : 1;
  const safePage = Math.min(currentPage, totalPages);
  const paginatedEmpresas = data
    ? data.empresas.slice((safePage - 1) * PAGE_SIZE, safePage * PAGE_SIZE)
    : [];

  return (
    <div className="space-y-5">
      {loading && (
        <p className="text-sm text-gray-500 dark:text-gray-400">Carregando detalhes…</p>
      )}

      {error && (
        <div className="rounded-2xl border border-error-200 bg-error-50 px-5 py-4 text-sm text-error-700 dark:border-error-500/20 dark:bg-error-500/10 dark:text-error-400">
          {error}
        </div>
      )}

      {!loading && !error && data && (
        <>
          {/* Header card */}
          <ComponentCard
            title="Detalhes da Pessoa"
            desc={`CPF mascarado: ${data.cpf_mascarado}`}
          >
            <div className="space-y-4">
              <div className="grid grid-cols-1 gap-3 md:grid-cols-2 xl:grid-cols-4">
                <InfoItem label="Nome" value={data.nome || "—"} />
                <InfoItem label="CPF mascarado" value={data.cpf_mascarado} />
                <InfoItem
                  label="Faixa etária"
                  value={
                    data.faixa_etaria != null
                      ? (FAIXA_ETARIA_LABEL[data.faixa_etaria] ?? String(data.faixa_etaria))
                      : "—"
                  }
                />
                <InfoItem
                  label="Empresas no QSA"
                  value={data.total_empresas.toLocaleString("pt-BR")}
                />
              </div>
            </div>
          </ComponentCard>

          {/* Companies table */}
          <ComponentCard
            title="Quadro Societário"
            desc={`${data.total_empresas.toLocaleString("pt-BR")} empresa${data.total_empresas !== 1 ? "s" : ""}`}
          >
            <div className="space-y-4">
              <div className="overflow-x-auto">
                <Table className="table-auto">
                  <TableHeader>
                    <TableRow className="border-b border-gray-100 dark:border-gray-800">
                      {[
                        "CNPJ básico",
                        "Razão Social",
                        "Qualificação",
                        "Entrada",
                        "Situação",
                        "UF",
                        "Município",
                        "Ref.",
                        "Ações",
                      ].map((h) => (
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
                    {paginatedEmpresas.map((emp) => (
                      <TableRow
                        key={emp.cnpj_basico}
                        className="border-b border-gray-100 last:border-0 hover:bg-gray-50 dark:border-gray-800 dark:hover:bg-white/[0.02] transition-colors"
                      >
                        {/* CNPJ básico */}
                        <TableCell className="px-4 py-3 text-sm font-mono text-gray-700 dark:text-gray-300 whitespace-nowrap">
                          {emp.cnpj_basico}
                        </TableCell>

                        {/* Razão Social */}
                        <TableCell className="px-4 py-3 max-w-xs">
                          <p className="text-sm font-medium text-gray-800 dark:text-white/90 truncate">
                            {emp.razao_social}
                          </p>
                        </TableCell>

                        {/* Qualificação */}
                        <TableCell className="px-4 py-3 text-sm text-gray-600 dark:text-gray-400">
                          {emp.qualificacao_socio_descricao || emp.qualificacao_socio || "—"}
                        </TableCell>

                        {/* Data entrada */}
                        <TableCell className="px-4 py-3 text-sm text-gray-600 dark:text-gray-400 whitespace-nowrap">
                          {emp.data_entrada_sociedade
                            ? new Date(emp.data_entrada_sociedade).toLocaleDateString("pt-BR")
                            : "—"}
                        </TableCell>

                        {/* Situação */}
                        <TableCell className="px-4 py-3 whitespace-nowrap">
                          {emp.situacao_cadastral ? (
                            <Badge
                              variant="light"
                              size="sm"
                              color={SITUACAO_COLOR[emp.situacao_cadastral] ?? "light"}
                            >
                              {SITUACAO_LABEL[emp.situacao_cadastral] ?? emp.situacao_cadastral}
                            </Badge>
                          ) : (
                            <span className="text-sm text-gray-400">—</span>
                          )}
                        </TableCell>

                        {/* UF */}
                        <TableCell className="px-4 py-3 text-sm text-gray-600 dark:text-gray-400 whitespace-nowrap">
                          {emp.uf || "—"}
                        </TableCell>

                        {/* Município */}
                        <TableCell className="px-4 py-3 text-sm text-gray-600 dark:text-gray-400">
                          {emp.municipio_nome || "—"}
                        </TableCell>

                        {/* Ref month */}
                        <TableCell className="px-4 py-3 text-xs font-mono text-gray-400 whitespace-nowrap">
                          {emp.reference_month}
                        </TableCell>

                        {/* Ações */}
                        <TableCell className="px-4 py-3 whitespace-nowrap">
                          <Link
                            href={`/cnpj/${emp.cnpj_basico}`}
                            className="inline-flex items-center justify-center font-medium gap-2 rounded-lg transition px-4 py-3 text-sm bg-white text-gray-700 ring-1 ring-inset ring-gray-300 hover:bg-gray-50 dark:bg-gray-800 dark:text-gray-400 dark:ring-gray-700 dark:hover:bg-white/[0.03] dark:hover:text-gray-300"
                          >
                            Ver empresa
                          </Link>
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </div>

              {totalPages > 1 && (
                <div className="flex justify-end border-t border-gray-100 pt-4 dark:border-gray-800">
                  <Pagination
                    currentPage={safePage}
                    totalPages={totalPages}
                    onPageChange={(p) => setCurrentPage(p)}
                  />
                </div>
              )}
            </div>
          </ComponentCard>

          {/* Network graph */}
          <PersonNetworkGraph cpfMascarado={cpfMascarado} nome={nome} />
        </>
      )}
    </div>
  );
}

function InfoItem({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-xl border border-gray-200 px-3 py-2 dark:border-gray-800">
      <p className="text-xs uppercase tracking-wide text-gray-500 dark:text-gray-400">{label}</p>
      <p className="mt-1 text-sm font-medium text-gray-800 dark:text-white/90">{value}</p>
    </div>
  );
}
