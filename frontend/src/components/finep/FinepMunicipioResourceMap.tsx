"use client";

import dynamic from "next/dynamic";

import type { FinepMunicipioMapFeatureCollection } from "./finepMunicipioMapTypes";

const FinepMunicipioResourceMapClient = dynamic(
  () => import("./FinepMunicipioResourceMapClient"),
  {
    ssr: false,
  }
);

interface FinepMunicipioResourceMapProps {
  featureCollection: FinepMunicipioMapFeatureCollection | null;
  loading: boolean;
}

export type { FinepMunicipioMapFeatureCollection } from "./finepMunicipioMapTypes";

export default function FinepMunicipioResourceMap(
  props: FinepMunicipioResourceMapProps
) {
  return <FinepMunicipioResourceMapClient {...props} />;
}