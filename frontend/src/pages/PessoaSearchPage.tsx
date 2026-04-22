import { useEffect } from "react";
import PessoaSearch from "@/components/cnpj/PessoaSearch";

export default function PessoaSearchPage() {
  useEffect(() => {
    document.title = "OSINT Platform - Pesquisa de Pessoas";
  }, []);

  return (
    <div className="grid grid-cols-12 gap-4 md:gap-6">
      <div className="col-span-12">
        <PessoaSearch />
      </div>
    </div>
  );
}