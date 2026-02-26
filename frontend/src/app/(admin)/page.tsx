import type { Metadata } from "next";
import React from "react";
import CnpjSearch from "@/components/cnpj/CnpjSearch";

export const metadata: Metadata = {
  title: "OSINT Platform — Pesquisa CNPJ",
  description: "Plataforma de inteligência para pesquisa de CNPJs",
};

export default function Home() {
  return (
    <div className="grid grid-cols-12 gap-4 md:gap-6">
      <div className="col-span-12">
        <CnpjSearch />
      </div>
    </div>
  );
}
