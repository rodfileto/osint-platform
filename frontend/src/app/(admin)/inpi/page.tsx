import type { Metadata } from "next";
import React from "react";

import PageBreadcrumb from "@/components/common/PageBreadCrumb";
import InpiOverview from "@/components/inpi/InpiOverview";

export const metadata: Metadata = {
  title: "OSINT Platform — INPI Patentes",
  description: "Painel de exploração dos dados de patentes do INPI",
};

export default function InpiPage() {
  return (
    <div>
      <PageBreadcrumb pageTitle="Painel INPI — Patentes" />
      <InpiOverview />
    </div>
  );
}
