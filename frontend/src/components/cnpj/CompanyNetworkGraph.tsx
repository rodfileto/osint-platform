"use client";

import React, { useEffect, useMemo, useRef, useState } from "react";
import axios from "axios";
import ComponentCard from "@/components/common/ComponentCard";
import Button from "@/components/ui/button/Button";
import apiClient from "@/lib/apiClient";

interface GraphCore {
  destroy: () => void;
  fit: (padding?: number) => void;
  layout: (options: Record<string, unknown>) => { run: () => void };
}

interface NetworkNode {
  id: string;
  type: "empresa" | "socio";
  label: string;
  data?: {
    cnpj_basico?: string;
    cpf_cnpj_socio?: string;
    is_core?: boolean;
  };
}

interface NetworkEdge {
  id: string;
  source: string;
  target: string;
  type: string;
}

interface NetworkMetadata {
  core_cnpj_basico: string;
  total_nodes: number;
  total_edges: number;
  total_relationships: number;
  truncated: boolean;
}

interface NetworkResponse {
  nodes: NetworkNode[];
  edges: NetworkEdge[];
  metadata: NetworkMetadata;
}

interface CompanyNetworkGraphProps {
  cnpjBasico: string;
}

export default function CompanyNetworkGraph({ cnpjBasico }: CompanyNetworkGraphProps) {
  const [data, setData] = useState<NetworkResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const graphRef = useRef<HTMLDivElement | null>(null);
  const cyRef = useRef<GraphCore | null>(null);

  useEffect(() => {
    const fetchNetwork = async () => {
      setLoading(true);
      setError(null);

      try {
        const response = await apiClient.get<NetworkResponse>(`/api/cnpj/network/${cnpjBasico}/`);
        setData(response.data);
      } catch (e) {
        const msg =
          axios.isAxiosError(e) && e.response
            ? `Erro ${e.response.status}: ${e.response.statusText}`
            : "Erro de conexão ao carregar rede societária";
        setError(msg);
        setData(null);
      } finally {
        setLoading(false);
      }
    };

    fetchNetwork();
  }, [cnpjBasico]);

  const elements = useMemo(() => {
    if (!data) {
      return [];
    }

    return [
      ...data.nodes.map((node) => ({
        data: {
          id: node.id,
          label: node.label,
          type: node.type,
          isCore: node.data?.is_core ? 1 : 0,
        },
      })),
      ...data.edges.map((edge) => ({
        data: {
          id: edge.id,
          source: edge.source,
          target: edge.target,
          label: edge.type,
        },
      })),
    ];
  }, [data]);

  useEffect(() => {
    let isMounted = true;

    const mountGraph = async () => {
      if (!graphRef.current || elements.length === 0) {
        if (cyRef.current) {
          cyRef.current.destroy();
          cyRef.current = null;
        }
        return;
      }

      const cytoscapeModule = await import("cytoscape");
      const coseBilkentModule = await import("cytoscape-cose-bilkent");

      if (!isMounted) {
        return;
      }

      const cytoscape = cytoscapeModule.default;
      const coseBilkent = coseBilkentModule.default;
      cytoscape.use(coseBilkent);

      if (cyRef.current) {
        cyRef.current.destroy();
      }

      cyRef.current = cytoscape({
        container: graphRef.current,
        elements,
        style: [
          {
            selector: "node",
            style: {
              label: "data(label)",
              "font-size": 10,
              "text-wrap": "wrap",
              "text-max-width": 140,
              color: "var(--color-gray-900)",
              "background-color": "var(--color-success-500)",
              "border-width": 1,
              "border-color": "var(--color-success-700)",
              width: 30,
              height: 30,
            },
          },
          {
            selector: 'node[type = "empresa"]',
            style: {
              "background-color": "var(--color-brand-500)",
              "border-color": "var(--color-brand-600)",
              width: 44,
              height: 44,
            },
          },
          {
            selector: "node[isCore = 1]",
            style: {
              "border-width": 3,
            },
          },
          {
            selector: "edge",
            style: {
              width: 1.4,
              "line-color": "var(--color-gray-400)",
              "target-arrow-color": "var(--color-gray-400)",
              "target-arrow-shape": "triangle",
              "curve-style": "bezier",
            },
          },
        ],
        layout: {
          name: "cose-bilkent",
          animate: false,
          nodeRepulsion: 6000,
          idealEdgeLength: 130,
        },
      });

      cyRef.current?.fit(30);
    };

    mountGraph();

    return () => {
      isMounted = false;
      if (cyRef.current) {
        cyRef.current.destroy();
        cyRef.current = null;
      }
    };
  }, [elements]);

  const handleRefit = () => {
    if (cyRef.current) {
      cyRef.current.fit(30);
    }
  };

  const handleRelayout = () => {
    if (cyRef.current) {
      cyRef.current.layout({
        name: "cose-bilkent",
        animate: true,
        animationDuration: 300,
      }).run();
    }
  };

  return (
    <ComponentCard title="Rede Societária" desc="Grafo Empresa ↔ Sócios (Neo4j)">
      {loading && (
        <p className="text-sm text-gray-500 dark:text-gray-400">Carregando rede…</p>
      )}

      {error && (
        <div className="rounded-2xl border border-error-200 bg-error-50 px-5 py-4 text-sm text-error-700 dark:border-error-500/20 dark:bg-error-500/10 dark:text-error-400">
          {error}
        </div>
      )}

      {!loading && !error && data && (
        <div className="space-y-4">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <div className="text-xs text-gray-500 dark:text-gray-400">
              Nós: {data.metadata.total_nodes} • Arestas: {data.metadata.total_edges}
              {data.metadata.truncated ? " • resultado truncado" : ""}
            </div>
            <div className="flex items-center gap-2">
              <Button variant="outline" size="sm" onClick={handleRelayout}>
                Reorganizar
              </Button>
              <Button variant="outline" size="sm" onClick={handleRefit}>
                Ajustar
              </Button>
            </div>
          </div>

          {data.nodes.length === 0 ? (
            <p className="text-sm text-gray-500 dark:text-gray-400">
              Nenhuma conexão societária encontrada para este CNPJ.
            </p>
          ) : (
            <div className="h-[460px] w-full rounded-xl border border-gray-200 dark:border-gray-800">
              <div ref={graphRef} className="h-full w-full" />
            </div>
          )}
        </div>
      )}
    </ComponentCard>
  );
}
