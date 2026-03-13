import type { Metadata } from "next";
import React from "react";

import PageBreadcrumb from "@/components/common/PageBreadCrumb";
import FinepOverview from "@/components/finep/FinepOverview";


export const metadata: Metadata = {
  title: "OSINT Platform — FINEP",
  description: "Painel inicial para exploração dos dados FINEP",
};


export default function FinepPage() {
  return (
    <div>
      <PageBreadcrumb pageTitle="Painel FINEP" />
      <FinepOverview />
    </div>
  );
}