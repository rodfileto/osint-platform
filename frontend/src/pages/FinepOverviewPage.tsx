import { useEffect } from "react";
import FinepOverview from "@/components/finep/FinepOverview";

export default function FinepOverviewPage() {
  useEffect(() => {
    document.title = "OSINT Platform - FINEP";
  }, []);

  return <FinepOverview />;
}