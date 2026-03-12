"use client";

import React from "react";
import { useParams } from "next/navigation";
import EmpresaDetail from "@/components/cnpj/EmpresaDetail";

export default function EmpresaDetailPage() {
  const params = useParams<{ cnpjBasico: string }>();
  const cnpjBasico = params?.cnpjBasico ?? null;

  return (
    <div className="grid grid-cols-12 gap-4 md:gap-6">
      <div className="col-span-12">
        <EmpresaDetail cnpjBasico={cnpjBasico} />
      </div>
    </div>
  );
}
