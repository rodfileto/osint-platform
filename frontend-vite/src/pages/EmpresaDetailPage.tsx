import { useEffect } from "react";
import { useParams } from "react-router-dom";
import EmpresaDetail from "@/components/cnpj/EmpresaDetail";

export default function EmpresaDetailPage() {
  const params = useParams<{ cnpjBasico: string }>();
  const cnpjBasico = params.cnpjBasico ?? null;

  useEffect(() => {
    document.title = cnpjBasico
      ? `OSINT Platform - Empresa ${cnpjBasico}`
      : "OSINT Platform - Empresa";
  }, [cnpjBasico]);

  return (
    <div className="grid grid-cols-12 gap-4 md:gap-6">
      <div className="col-span-12">
        <EmpresaDetail cnpjBasico={cnpjBasico} />
      </div>
    </div>
  );
}