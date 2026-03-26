// Shared TypeScript type definitions

export interface NodeTypeDef {
  type: string;
  count: number;
  color: string;
  table: string;
}

export interface EdgeTypeDef {
  source: string;
  target: string;
  label: string;
}

export interface GraphSchema {
  node_types: NodeTypeDef[];
  edge_types: EdgeTypeDef[];
}

export interface GraphNode {
  type: string;
  id: string;
  properties: Record<string, unknown>;
}

export interface GraphEdge {
  source: string; // "NodeType:id"
  target: string;
  label: string;
}

export interface ExpandResult {
  nodes: GraphNode[];
  edges: GraphEdge[];
}

export interface ChatMessage {
  role: 'user' | 'assistant';
  content: string;
  sql?: string | null;
  highlighted_nodes?: HighlightedNode[];
}

export interface HighlightedNode {
  type: string;
  id: string;
}

export interface ChatResponse {
  response: string;
  sql: string | null;
  highlighted_nodes: HighlightedNode[];
  error?: string | null;
}
