import { useEffect } from "react";
import InovalinkMunicipalityValidation from "@/components/inovalink/InovalinkMunicipalityValidation";

export default function InovalinkValidationPage() {
  useEffect(() => {
    document.title = "OSINT Platform - Inovalink";
  }, []);

  return <InovalinkMunicipalityValidation />;
}