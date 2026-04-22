import { useEffect, useMemo, useRef, useState } from "react";
import { isAxiosError } from "axios";
import ComponentCard from "@/components/common/ComponentCard";
import Select from "@/components/form/Select";
import Button from "@/components/ui/button/Button";
import apiClient from "@/lib/apiClient";

interface GraphCore {
  destroy: () => void;
  fit: (padding?: number) => void;
  layout: (options: Record<string, unknown>) => { run: () => void };
  on: (event: string, selectorOrHandler: string | (() => void), handler?: (event: GraphNodeEvent) => void) => void;
}

interface NetworkNode {
  id: string;
  type: "pessoa" | "empresa";
  label: string;
  data?: {
    cnpj_basico?: string | null;
    cpf_cnpj_socio?: string | null;
    nome?: string | null;
    is_core?: boolean;
    porte_empresa?: string | null;
  };
}

interface NetworkEdge {
  id: string;
  source: string;
  target: string;
  type: string;
}

interface PersonNetworkMetadata {
  core_cpf_mascarado: string;
  core_nome: string | null;
  depth: number;
  total_nodes: number;
  total_edges: number;
  total_relationships: number;
  truncated: boolean;
  max_edges: number;
}

interface PersonNetworkResponse {
  nodes: NetworkNode[];
  edges: NetworkEdge[];
  metadata: PersonNetworkMetadata;
}

interface GraphNodeEventTarget {
  data: (key: string) => unknown;
  renderedPosition: () => { x: number; y: number };
}

interface GraphNodeEvent {
  target: GraphNodeEventTarget;
}

type PersonNetworkGraphProps = {
  cpfMascarado: string;
  nome: string;
};

type NetworkTooltipState = {
  label: string;
  x: number;
  y: number;
  visible: boolean;
};

function getShortLabel(label: string, type: NetworkNode["type"]) {
  const maxLength = type === "empresa" ? 22 : 18;

  if (label.length <= maxLength) {
    return label;
  }

  return `${label.slice(0, maxLength - 1)}…`;
}

export default function PersonNetworkGraph({ cpfMascarado, nome }: PersonNetworkGraphProps) {
  const [data, setData] = useState<PersonNetworkResponse | null>(null);
  const [depth, setDepth] = useState("1");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [tooltip, setTooltip] = useState<NetworkTooltipState>({
    label: "",
    x: 0,
    y: 0,
    visible: false,
  });
  const graphRef = useRef<HTMLDivElement | null>(null);
  const cyRef = useRef<GraphCore | null>(null);

  useEffect(() => {
    const fetchNetwork = async () => {
      setLoading(true);
      setError(null);

      try {
        const response = await apiClient.get<PersonNetworkResponse>("/api/cnpj/pessoa-network/detail/", {
          params: {
            depth,
            nome,
            cpf_mascarado: cpfMascarado,
          },
        });
        setData(response.data);
      } catch (errorValue) {
        const message =
          isAxiosError(errorValue) && errorValue.response
            ? `Erro ${errorValue.response.status}: ${errorValue.response.statusText}`
            : "Erro de conexão ao carregar rede de co-propriedade";

        setError(message);
        setData(null);
      } finally {
        setLoading(false);
      }
    };

    void fetchNetwork();
  }, [cpfMascarado, nome, depth]);

  const elements = useMemo(() => {
    if (!data) {
      return [];
    }

    return [
      ...data.nodes.map((node) => {
        const isPessoa = node.type === "pessoa";
        const isCore = node.data?.is_core ? 1 : 0;

        return {
          data: {
            id: node.id,
            label: getShortLabel(node.label, node.type),
            fullLabel: node.label,
            type: node.type,
            isCore,
            size: isPessoa ? (isCore ? 48 : 30) : 44,
            nodeShape: isPessoa ? "ellipse" : "round-rectangle",
            nodeColor: isPessoa ? (isCore ? "#92400e" : "#c2410c") : "#1d4ed8",
            nodeBorderColor: isPessoa ? (isCore ? "#78350f" : "#9a3412") : "#1e40af",
          },
        };
      }),
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

      const cyInstance = cytoscape({
        container: graphRef.current,
        elements,
        style: [
          {
            selector: "node",
            style: {
              label: "data(label)",
              shape: "data(nodeShape)",
              "font-size": 10,
              "font-weight": 600,
              "text-wrap": "wrap",
              "text-max-width": 92,
              color: "#f8fafc",
              "background-color": "data(nodeColor)",
              "border-width": 2,
              "border-color": "data(nodeBorderColor)",
              width: "data(size)",
              height: "data(size)",
              padding: "10px",
              "text-valign": "center",
              "text-halign": "center",
            },
          },
          {
            selector: "node[isCore = 1]",
            style: {
              "border-width": 4,
              "overlay-padding": 6,
              "overlay-opacity": 0.08,
              "overlay-color": "#92400e",
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
      cyRef.current = cyInstance;

      const showTooltip = (event: GraphNodeEvent) => {
        const renderedPosition = event.target.renderedPosition();
        const fullLabel = String(event.target.data("fullLabel") || event.target.data("label") || "");

        setTooltip({
          label: fullLabel,
          x: renderedPosition.x + 12,
          y: renderedPosition.y - 12,
          visible: true,
        });
      };

      const hideTooltip = () => {
        setTooltip((currentTooltip) =>
          currentTooltip.visible ? { ...currentTooltip, visible: false } : currentTooltip,
        );
      };

      cyInstance.on("mouseover", "node", showTooltip);
      cyInstance.on("mousemove", "node", showTooltip);
      cyInstance.on("mouseout", "node", hideTooltip);
      cyInstance.on("tap", hideTooltip);
      cyInstance.on("dragpan", hideTooltip);
      cyInstance.on("zoom", hideTooltip);
      cyInstance.fit(30);
    };

    void mountGraph();

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
      cyRef.current
        .layout({
          name: "cose-bilkent",
          animate: false,
          nodeRepulsion: 6000,
          idealEdgeLength: 130,
        })
        .run();
    }
  };

  return (
    <ComponentCard title="Rede de Co-propriedade" desc="Pessoa ↔ Empresas ↔ Co-sócios (Neo4j)">
      {loading ? <p className="text-sm text-gray-500 dark:text-gray-400">Carregando rede...</p> : null}

      {error ? (
        <div className="rounded-2xl border border-red-200 bg-red-50 px-5 py-4 text-sm text-red-700 dark:border-red-500/20 dark:bg-red-500/10 dark:text-red-400">
          {error}
        </div>
      ) : null}

      {!loading && !error && data ? (
        <div className="space-y-4">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <div className="text-xs text-gray-500 dark:text-gray-400">
              Profundidade: {data.metadata.depth} • Nós: {data.metadata.total_nodes} • Arestas: {data.metadata.total_edges}
              {data.metadata.truncated ? " • resultado truncado" : ""}
            </div>
            <div className="flex flex-wrap items-center gap-2">
              <div className="min-w-[160px]">
                <Select
                  options={[
                    { value: "1", label: "Profundidade 1" },
                    { value: "2", label: "Profundidade 2" },
                    { value: "3", label: "Profundidade 3" },
                  ]}
                  value={depth}
                  onChange={setDepth}
                  className="h-9 rounded-lg py-2 text-xs"
                />
              </div>
              <Button variant="outline" size="sm" onClick={handleRelayout}>
                Reorganizar
              </Button>
              <Button variant="outline" size="sm" onClick={handleRefit}>
                Ajustar
              </Button>
            </div>
          </div>

          <div className="flex flex-wrap items-center gap-3 text-xs text-gray-500 dark:text-gray-400">
            <div className="flex items-center gap-2">
              <span className="h-3 w-3 rounded-full bg-amber-800" />
              <span>Pessoa (foco)</span>
            </div>
            <div className="flex items-center gap-2">
              <span className="h-3 w-3 rounded-full bg-orange-700" />
              <span>Co-sócio (PF)</span>
            </div>
            <div className="flex items-center gap-2">
              <span className="h-3 w-3 rounded-sm bg-blue-700" />
              <span>Empresa</span>
            </div>
          </div>

          {data.nodes.length === 0 ? (
            <p className="text-sm text-gray-500 dark:text-gray-400">
              Nenhuma conexão societária encontrada para esta pessoa.
            </p>
          ) : (
            <div className="relative h-[460px] w-full rounded-xl border border-gray-200 dark:border-gray-800">
              <div ref={graphRef} className="h-full w-full" />
              {tooltip.visible ? (
                <div
                  className="pointer-events-none absolute z-10 max-w-64 rounded-lg bg-gray-950/90 px-3 py-2 text-xs font-medium text-white shadow-lg"
                  style={{ left: Math.min(tooltip.x, 520), top: Math.max(tooltip.y, 8) }}
                >
                  {tooltip.label}
                </div>
              ) : null}
            </div>
          )}
        </div>
      ) : null}
    </ComponentCard>
  );
}