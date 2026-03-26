import { useEffect, useRef, useCallback, useState } from 'react';
import cytoscape from 'cytoscape';
import type { Core, NodeSingular } from 'cytoscape';
import '../../cytoscape-setup';
import { fetchSchema, fetchNodes, expandNode } from '../../api';
import type { GraphSchema, HighlightedNode } from '../../types';

// ── Colour palette per node type ────────────────────────────────────────────
const TYPE_COLORS: Record<string, string> = {
  SalesOrder: '#6366f1',
  SalesOrderItem: '#8b5cf6',
  DeliveryHeader: '#06b6d4',
  DeliveryItem: '#0891b2',
  BillingHeader: '#f59e0b',
  BillingItem: '#d97706',
  JournalEntry: '#10b981',
  Payment: '#059669',
  Customer: '#ec4899',
  Product: '#f97316',
  Plant: '#84cc16',
  __meta__: '#334155',
};

function colorFor(type: string) {
  return TYPE_COLORS[type] ?? '#64748b';
}

function nodeLabel(type: string, id: string, props?: Record<string, unknown>): string {
  if (type === '__meta__') return id;
  const short = id.length > 14 ? id.slice(0, 12) + '…' : id;
  if (props) {
    const name =
      (props['businessPartnerFullName'] as string) ||
      (props['organizationBpName1'] as string) ||
      (props['productDescription'] as string) ||
      (props['plantName'] as string) || '';
    if (name) return `${type}\n${short}\n${name.slice(0, 20)}`;
  }
  return `${type}\n${short}`;
}

interface GraphViewProps {
  highlights: HighlightedNode[];
  onNodeSelect: (node: { type: string; id: string; properties: Record<string, unknown> } | null) => void;
}

export default function GraphView({ highlights, onNodeSelect }: GraphViewProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const cyRef = useRef<Core | null>(null);
  const [schema, setSchema] = useState<GraphSchema | null>(null);
  const [loading, setLoading] = useState(true);
  const [expandingId, setExpandingId] = useState<string | null>(null);

  // ── Build meta-graph (entity type overview) ────────────────────────────────
  const buildMetaGraph = useCallback((s: GraphSchema) => {
    if (!cyRef.current) return;
    const cy = cyRef.current;
    cy.elements().remove();

    // Add meta-nodes (one per entity type)
    s.node_types.forEach(nt => {
      cy.add({
        data: {
          id: `meta:${nt.type}`,
          label: `${nt.type}\n(${nt.count.toLocaleString()})`,
          nodeType: '__meta__',
          realType: nt.type,
          color: nt.color,
          isMetaNode: true,
          count: nt.count,
        },
      });
    });

    // Add meta-edges from edge type definitions
    s.edge_types.forEach((et, i) => {
      const srcId = `meta:${et.source}`;
      const tgtId = `meta:${et.target}`;
      if (cy.getElementById(srcId).length && cy.getElementById(tgtId).length) {
        cy.add({ data: { id: `meta_edge:${i}`, source: srcId, target: tgtId, label: et.label } });
      }
    });

    cy.layout({
      name: 'dagre',
      rankDir: 'LR',
      nodeSep: 60,
      rankSep: 100,
      padding: 40,
    } as cytoscape.LayoutOptions).run();
  }, []);

  // ── Initialise Cytoscape ────────────────────────────────────────────────────
  useEffect(() => {
    if (!containerRef.current) return;

    const cy = cytoscape({
      container: containerRef.current,
      style: [
        {
          selector: 'node',
          style: {
            'background-color': 'data(color)',
            label: 'data(label)',
            color: '#fff',
            'text-valign': 'center',
            'text-halign': 'center',
            'font-size': 10,
            'text-wrap': 'wrap',
            'text-max-width': '100px',
            width: 80,
            height: 80,
            shape: 'ellipse',
            'border-width': 2,
            'border-color': '#1e293b',
          },
        },
        {
          selector: 'node[isMetaNode]',
          style: {
            width: 100,
            height: 100,
            shape: 'round-rectangle',
            'font-size': 11,
            'font-weight': 'bold',
          },
        },
        {
          selector: 'node.highlighted',
          style: {
            'border-color': '#fbbf24',
            'border-width': 5,
            'background-blacken': -0.1,
          },
        },
        {
          selector: 'node.selected',
          style: {
            'border-color': '#38bdf8',
            'border-width': 4,
          },
        },
        {
          selector: 'edge',
          style: {
            width: 2,
            'line-color': '#475569',
            'target-arrow-color': '#475569',
            'target-arrow-shape': 'triangle',
            'curve-style': 'bezier',
            label: 'data(label)',
            'font-size': 9,
            color: '#94a3b8',
            'text-background-color': '#0f1117',
            'text-background-opacity': 0.8,
            'text-background-padding': '2px',
          },
        },
      ],
      elements: [],
      wheelSensitivity: 0.3,
      minZoom: 0.1,
      maxZoom: 4,
    });

    cyRef.current = cy;

    // Node click handler
    cy.on('tap', 'node', async (evt) => {
      const node = evt.target as NodeSingular;
      const data = node.data();

      cy.nodes().removeClass('selected');
      node.addClass('selected');

      if (data.isMetaNode) {
        // Load first page of real nodes for this type
        setExpandingId(data.id);
        try {
          const result = await fetchNodes(data.realType, 1, 30);
          const idCol = getIdCol(data.realType);
          result.nodes.forEach((props) => {
            const nodeId = `${data.realType}:${props[idCol]}`;
            if (!cy.getElementById(nodeId).length) {
              cy.add({
                data: {
                  id: nodeId,
                  label: nodeLabel(data.realType, String(props[idCol]), props),
                  nodeType: data.realType,
                  color: colorFor(data.realType),
                  properties: props,
                  isMetaNode: false,
                },
              });
              cy.add({ data: { id: `link_${data.id}_${nodeId}`, source: data.id, target: nodeId, label: 'HAS' } });
            }
          });
          cy.layout({ name: 'dagre', rankDir: 'LR', nodeSep: 40, rankSep: 80, padding: 30 } as cytoscape.LayoutOptions).run();
        } finally {
          setExpandingId(null);
        }
        onNodeSelect(null);
      } else {
        // Expand this real node's neighbours
        setExpandingId(data.id);
        onNodeSelect({ type: data.nodeType, id: data.id.replace(`${data.nodeType}:`, ''), properties: data.properties ?? {} });
        try {
          const rawId = data.id.replace(`${data.nodeType}:`, '');
          const result = await expandNode(data.nodeType, rawId);
          result.nodes.forEach((n) => {
            const cId = `${n.type}:${n.id}`;
            if (!cy.getElementById(cId).length) {
              cy.add({
                data: {
                  id: cId,
                  label: nodeLabel(n.type, n.id, n.properties),
                  nodeType: n.type,
                  color: colorFor(n.type),
                  properties: n.properties,
                  isMetaNode: false,
                },
              });
            }
          });
          result.edges.forEach((e) => {
            const srcId = e.source.includes(':') ? e.source : `${e.source.split(':')[0]}:${e.source}`;
            const tgtId = e.target.includes(':') ? e.target : `${e.target.split(':')[0]}:${e.target}`;
            const eId = `e_${srcId}_${tgtId}_${e.label}`;
            if (!cy.getElementById(eId).length) {
              cy.add({ data: { id: eId, source: e.source, target: e.target, label: e.label } });
            }
          });
          cy.layout({ name: 'dagre', rankDir: 'LR', nodeSep: 40, rankSep: 80, padding: 30 } as cytoscape.LayoutOptions).run();
        } catch {
          // ignore expand errors silently
        } finally {
          setExpandingId(null);
        }
      }
    });

    // Background tap — deselect
    cy.on('tap', (evt) => {
      if (evt.target === cy) {
        cy.nodes().removeClass('selected');
        onNodeSelect(null);
      }
    });

    return () => { cy.destroy(); cyRef.current = null; };
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // ── Load schema on mount ───────────────────────────────────────────────────
  useEffect(() => {
    fetchSchema()
      .then((s) => {
        setSchema(s);
        setLoading(false);
        buildMetaGraph(s);
      })
      .catch(() => setLoading(false));
  }, [buildMetaGraph]);

  // ── Apply highlights from chat ─────────────────────────────────────────────
  useEffect(() => {
    if (!cyRef.current) return;
    const cy = cyRef.current;
    cy.nodes().removeClass('highlighted');
    highlights.forEach(({ type, id }) => {
      const cId = `${type}:${id}`;
      const node = cy.getElementById(cId);
      if (node.length) {
        node.addClass('highlighted');
      }
    });
  }, [highlights]);

  return (
    <div className="relative flex-1 h-full bg-[#0f1117]">
      {loading && (
        <div className="absolute inset-0 flex items-center justify-center z-10 bg-[#0f1117]/80">
          <div className="text-slate-400 text-sm">Loading graph schema…</div>
        </div>
      )}
      {expandingId && (
        <div className="absolute top-3 left-1/2 -translate-x-1/2 z-10 bg-slate-800 text-slate-300 text-xs px-3 py-1 rounded-full shadow">
          Expanding…
        </div>
      )}
      {schema && (
        <div className="absolute bottom-3 left-3 z-10 bg-slate-900/90 rounded-lg p-2 text-xs text-slate-400 space-y-1 max-w-[180px]">
          <div className="font-semibold text-slate-300 mb-1">Legend</div>
          {schema.node_types.map(nt => (
            <div key={nt.type} className="flex items-center gap-2">
              <span className="w-3 h-3 rounded-full inline-block" style={{ background: nt.color }} />
              <span>{nt.type}</span>
            </div>
          ))}
        </div>
      )}
      {/* Reset button */}
      <button
        onClick={() => { if (schema && cyRef.current) buildMetaGraph(schema); }}
        className="absolute top-3 left-3 z-10 bg-slate-800 hover:bg-slate-700 text-slate-300 text-xs px-3 py-1.5 rounded-lg shadow transition-colors"
      >
        ↺ Reset View
      </button>
      <div ref={containerRef} className="w-full h-full" />
    </div>
  );
}

function getIdCol(type: string): string {
  const map: Record<string, string> = {
    SalesOrder: 'salesOrder',
    SalesOrderItem: 'salesOrder',
    DeliveryHeader: 'deliveryDocument',
    DeliveryItem: 'deliveryDocument',
    BillingHeader: 'billingDocument',
    BillingItem: 'billingDocument',
    JournalEntry: 'accountingDocument',
    Payment: 'accountingDocument',
    Customer: 'businessPartner',
    Product: 'product',
    Plant: 'plant',
  };
  return map[type] ?? 'id';
}
