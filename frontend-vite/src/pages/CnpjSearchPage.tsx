import { useEffect } from "react";
import CnpjSearch from "@/components/cnpj/CnpjSearch";

export default function CnpjSearchPage() {
  useEffect(() => {
    document.title = "OSINT Platform - Pesquisa CNPJ";
  }, []);

  return (
    <div className="grid grid-cols-12 gap-4 md:gap-6">
      <div className="col-span-12">
        <CnpjSearch />
      </div>
    </div>
  );
}