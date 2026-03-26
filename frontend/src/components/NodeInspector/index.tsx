interface NodeInspectorProps {
  node: { type: string; id: string; properties: Record<string, unknown> } | null;
}

export default function NodeInspector({ node }: NodeInspectorProps) {
  if (!node) {
    return (
      <div className="h-full flex items-center justify-center text-slate-500 text-xs px-4 text-center">
        Click a node in the graph to inspect its properties.
      </div>
    );
  }

  const entries = Object.entries(node.properties).filter(([, v]) => v !== null && v !== undefined && v !== '');

  return (
    <div className="h-full overflow-y-auto px-3 py-3">
      <div className="mb-3">
        <span className="text-xs font-bold uppercase tracking-wider text-slate-400">{node.type}</span>
        <p className="text-slate-200 font-mono text-sm mt-0.5 break-all">{node.id}</p>
      </div>
      <div className="space-y-1.5">
        {entries.map(([key, val]) => (
          <div key={key} className="flex flex-col">
            <span className="text-xs text-slate-500">{key}</span>
            <span className="text-xs text-slate-300 font-mono break-all">
              {typeof val === 'boolean' ? (val ? 'true' : 'false') : String(val)}
            </span>
          </div>
        ))}
      </div>
    </div>
  );
}
