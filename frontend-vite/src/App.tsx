import { Suspense, lazy } from "react";
import { BrowserRouter, Navigate, Route, Routes } from "react-router-dom";
import { SidebarProvider } from "@/contexts/SidebarContext";
import { ThemeProvider } from "@/contexts/ThemeContext";
import AppLayout from "@/layout/AppLayout";
import { runtimeEnv } from "@/lib/env";

const CnpjSearchPage = lazy(() => import("@/pages/CnpjSearchPage"));
const EmpresaDetailPage = lazy(() => import("@/pages/EmpresaDetailPage"));
const FinepOverviewPage = lazy(() => import("@/pages/FinepOverviewPage"));
const InpiOverviewPage = lazy(() => import("@/pages/InpiOverviewPage"));
const PessoaDetailPage = lazy(() => import("@/pages/PessoaDetailPage"));
const PessoaSearchPage = lazy(() => import("@/pages/PessoaSearchPage"));

function RouteFallback() {
  return <div className="px-1 py-6 text-sm text-gray-500 dark:text-gray-400">Carregando página...</div>;
}

export default function App() {
  const apiBaseUrl = runtimeEnv.apiUrl;

  return (
    <ThemeProvider>
      <SidebarProvider>
        <BrowserRouter>
          <Suspense fallback={<RouteFallback />}>
            <Routes>
              <Route element={<AppLayout apiBaseUrl={apiBaseUrl} />}>
                <Route path="/" element={<CnpjSearchPage />} />
                <Route path="/pessoa" element={<PessoaSearchPage />} />
                <Route path="/pessoa/detalhe" element={<PessoaDetailPage />} />
                <Route path="/pessoa/:cpf" element={<PessoaDetailPage />} />
                <Route path="/finep" element={<FinepOverviewPage />} />
                <Route path="/inpi" element={<InpiOverviewPage />} />
                <Route path="/cnpj/:cnpjBasico" element={<EmpresaDetailPage />} />
                <Route path="*" element={<Navigate to="/" replace />} />
              </Route>
            </Routes>
          </Suspense>
        </BrowserRouter>
      </SidebarProvider>
    </ThemeProvider>
  );
}