import PessoaDetail from "@/components/cnpj/PessoaDetail";

type PessoaDetailQueryPageProps = {
  searchParams: Promise<{
    nome?: string;
    cpf_mascarado?: string;
  }>;
};

export default async function PessoaDetailQueryPage({
  searchParams,
}: PessoaDetailQueryPageProps) {
  const resolvedSearchParams = await searchParams;
  const nome = resolvedSearchParams.nome ?? "";
  const cpfMascarado = resolvedSearchParams.cpf_mascarado ?? "";

  if (nome.trim().length < 3 || !/^\*\*\*\d{6}\*\*$/.test(cpfMascarado)) {
    return (
      <div className="grid grid-cols-12 gap-4 md:gap-6">
        <div className="col-span-12">
          <div className="rounded-2xl border border-error-200 bg-error-50 px-5 py-4 text-sm text-error-700 dark:border-error-500/20 dark:bg-error-500/10 dark:text-error-400">
            Parâmetros inválidos. Informe nome e CPF mascarado válidos para abrir a pessoa.
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="grid grid-cols-12 gap-4 md:gap-6">
      <div className="col-span-12">
        <PessoaDetail cpfMascarado={cpfMascarado} nome={nome} />
      </div>
    </div>
  );
}