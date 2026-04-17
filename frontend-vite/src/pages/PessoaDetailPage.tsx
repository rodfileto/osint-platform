import { useEffect } from "react";
import { useParams, useSearchParams } from "react-router-dom";
import PessoaDetail from "@/components/cnpj/PessoaDetail";

function InvalidParamsMessage() {
  return (
    <div className="grid grid-cols-12 gap-4 md:gap-6">
      <div className="col-span-12">
        <div className="rounded-2xl border border-red-200 bg-red-50 px-5 py-4 text-sm text-red-700 dark:border-red-500/20 dark:bg-red-500/10 dark:text-red-400">
          Parâmetros inválidos. Informe nome e CPF mascarado válidos para abrir a pessoa.
        </div>
      </div>
    </div>
  );
}

export default function PessoaDetailPage() {
  const params = useParams<{ cpf: string }>();
  const [searchParams] = useSearchParams();
  const nome = searchParams.get("nome") ?? "";
  const cpfMascarado = searchParams.get("cpf_mascarado") ?? "";
  const isValid = nome.trim().length >= 3 && /^\*\*\*\d{6}\*\*$/.test(cpfMascarado);

  useEffect(() => {
    document.title = params.cpf
      ? `OSINT Platform - Pessoa ${params.cpf}`
      : "OSINT Platform - Pessoa detalhe";
  }, [params.cpf]);

  if (!isValid) {
    return <InvalidParamsMessage />;
  }

  return (
    <div className="grid grid-cols-12 gap-4 md:gap-6">
      <div className="col-span-12">
        <PessoaDetail cpfMascarado={cpfMascarado} nome={nome} />
      </div>
    </div>
  );
}