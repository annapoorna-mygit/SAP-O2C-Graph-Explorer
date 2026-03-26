import { useState } from "react";
import GraphView from "./components/GraphView";
import ChatPanel from "./components/ChatPanel";
import NodeInspector from "./components/NodeInspector";
import type { HighlightedNode } from "./types";

export default function App() {
  const [highlights, setHighlights] = useState<HighlightedNode[]>([]);
  const [selectedNode, setSelectedNode] = useState<{
    type: string;
    id: string;
    properties: Record<string, unknown>;
  } | null>(null);
  const [inspectorOpen, setInspectorOpen] = useState(false);

  function handleNodeSelect(node: typeof selectedNode) {
    setSelectedNode(node);
    if (node) setInspectorOpen(true);
  }

  return (
    <div className="flex flex-col h-screen bg-[#0f1117] text-slate-200">
      {/* Top bar */}
      <header className="flex items-center gap-3 px-4 py-2 bg-[#1a2035] border-b border-slate-700/50 shrink-0">
        <div className="flex items-center gap-2">
          <div className="w-6 h-6 rounded bg-indigo-600 flex items-center justify-center text-xs font-bold">D</div>
          <span className="font-semibold text-slate-100 text-sm">SAP O2C Graph Explorer</span>
        </div>
        <span className="text-slate-500 text-xs ml-1">|  Order-to-Cash Data Intelligence</span>
        <div className="ml-auto flex gap-2 text-xs text-slate-500">
          <span>Click entity nodes to explore relationships</span>
        </div>
      </header>

      {/* Main content */}
      <div className="flex flex-1 min-h-0">
        {/* Graph (left, flexible) */}
        <div className="flex-1 min-w-0 relative">
          <GraphView highlights={highlights} onNodeSelect={handleNodeSelect} />
        </div>

        {/* Right panel: chat + optional inspector */}
        <div className="flex flex-col w-[380px] shrink-0 border-l border-slate-700/50">
          {/* Node Inspector (collapsible) */}
          {inspectorOpen && (
            <div className="h-52 shrink-0 border-b border-slate-700/50 bg-[#1a2035] flex flex-col">
              <div className="flex items-center justify-between px-3 py-1.5 border-b border-slate-700/30">
                <span className="text-xs font-semibold text-slate-400">Node Inspector</span>
                <button onClick={() => setInspectorOpen(false)} className="text-slate-500 hover:text-slate-300 text-xs">?</button>
              </div>
              <div className="flex-1 min-h-0">
                <NodeInspector node={selectedNode} />
              </div>
            </div>
          )}

          {/* Chat Panel */}
          <div className="flex-1 min-h-0">
            <ChatPanel onHighlight={setHighlights} />
          </div>
        </div>
      </div>
    </div>
  );
}
