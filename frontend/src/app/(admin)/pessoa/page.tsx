import type { Metadata } from "next";
import PessoaSearch from "@/components/cnpj/PessoaSearch";

export const metadata: Metadata = {
  title: "OSINT Platform — Pesquisa de Pessoas",
  description: "Pesquisa de sócios e co-propriedade por nome e CPF",
};

export default function PessoaSearchPage() {
  return (
    <div className="grid grid-cols-12 gap-4 md:gap-6">
      <div className="col-span-12">
        <PessoaSearch />
      </div>
    </div>
  );
}
