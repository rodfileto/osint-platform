import { useEffect } from "react";
import InpiOverview from "@/components/inpi/InpiOverview";

export default function InpiOverviewPage() {
  useEffect(() => {
    document.title = "OSINT Platform - INPI";
  }, []);

  return <InpiOverview />;
}