import { useState, useRef, useEffect } from 'react';
import { sendChat } from '../../api';
import type { ChatMessage, HighlightedNode } from '../../types';

interface ChatPanelProps {
  onHighlight: (nodes: HighlightedNode[]) => void;
}

const EXAMPLE_QUERIES = [
  'Which products are associated with the highest number of billing documents?',
  'How many sales orders have been fully delivered and billed?',
  'Identify sales orders that were delivered but not billed.',
  'Show me the top 5 customers by total billed amount.',
];

export default function ChatPanel({ onHighlight }: ChatPanelProps) {
  const [messages, setMessages] = useState<ChatMessage[]>([
    {
      role: 'assistant',
      content: 'Hello! I can answer questions about the SAP Order-to-Cash data. Ask me about orders, deliveries, billing, payments, customers, or products.',
    },
  ]);
  const [input, setInput] = useState('');
  const [loading, setLoading] = useState(false);
  const [showSql, setShowSql] = useState<Record<number, boolean>>({});
  const bottomRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages]);

  const submit = async (text: string) => {
    if (!text.trim() || loading) return;
    const userMsg: ChatMessage = { role: 'user', content: text.trim() };
    const history = messages
      .filter(m => m.role === 'user' || m.role === 'assistant')
      .map(m => ({ role: m.role, content: m.content }));

    setMessages(prev => [...prev, userMsg]);
    setInput('');
    setLoading(true);

    try {
      const res = await sendChat(text.trim(), history);
      const assistantMsg: ChatMessage = {
        role: 'assistant',
        content: res.response,
        sql: res.sql,
        highlighted_nodes: res.highlighted_nodes,
      };
      setMessages(prev => [...prev, assistantMsg]);
      if (res.highlighted_nodes?.length) {
        onHighlight(res.highlighted_nodes);
      }
    } catch (err) {
      setMessages(prev => [
        ...prev,
        { role: 'assistant', content: `Error: ${(err as Error).message}` },
      ]);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="flex flex-col h-full bg-[#161b27] border-l border-slate-700/50">
      {/* Header */}
      <div className="px-4 py-3 border-b border-slate-700/50 bg-[#1a2035] flex items-center gap-2">
        <div className="w-2 h-2 rounded-full bg-emerald-500" />
        <span className="text-sm font-semibold text-slate-200">O2C Query Assistant</span>
        <span className="ml-auto text-xs text-slate-500">Groq · Llama 3.3 70B</span>
      </div>

      {/* Messages */}
      <div className="flex-1 overflow-y-auto px-3 py-4 space-y-3">
        {messages.map((msg, idx) => (
          <div key={idx} className={`flex ${msg.role === 'user' ? 'justify-end' : 'justify-start'}`}>
            <div
              className={`max-w-[90%] rounded-xl px-3 py-2 text-sm leading-relaxed ${
                msg.role === 'user'
                  ? 'bg-indigo-600 text-white rounded-br-sm'
                  : 'bg-slate-800 text-slate-200 rounded-bl-sm'
              }`}
            >
              <p className="whitespace-pre-wrap">{msg.content}</p>

              {/* SQL toggle */}
              {msg.sql && (
                <div className="mt-2">
                  <button
                    onClick={() => setShowSql(prev => ({ ...prev, [idx]: !prev[idx] }))}
                    className="text-xs text-slate-400 hover:text-slate-200 underline"
                  >
                    {showSql[idx] ? 'Hide SQL' : 'Show SQL'}
                  </button>
                  {showSql[idx] && (
                    <pre className="mt-1 text-xs bg-slate-900 rounded p-2 overflow-x-auto text-emerald-300 whitespace-pre-wrap">
                      {msg.sql}
                    </pre>
                  )}
                </div>
              )}

              {/* Highlighted nodes badge */}
              {msg.highlighted_nodes && msg.highlighted_nodes.length > 0 && (
                <div className="mt-1 flex flex-wrap gap-1">
                  {msg.highlighted_nodes.slice(0, 6).map((n, ni) => (
                    <span key={ni} className="text-xs bg-amber-900/50 text-amber-300 px-1.5 py-0.5 rounded">
                      {n.type}: {n.id.length > 12 ? n.id.slice(0, 10) + '…' : n.id}
                    </span>
                  ))}
                </div>
              )}
            </div>
          </div>
        ))}

        {loading && (
          <div className="flex justify-start">
            <div className="bg-slate-800 rounded-xl rounded-bl-sm px-3 py-2">
              <div className="flex gap-1">
                <span className="w-1.5 h-1.5 rounded-full bg-slate-500 animate-bounce" style={{ animationDelay: '0ms' }} />
                <span className="w-1.5 h-1.5 rounded-full bg-slate-500 animate-bounce" style={{ animationDelay: '150ms' }} />
                <span className="w-1.5 h-1.5 rounded-full bg-slate-500 animate-bounce" style={{ animationDelay: '300ms' }} />
              </div>
            </div>
          </div>
        )}

        <div ref={bottomRef} />
      </div>

      {/* Example queries */}
      {messages.length <= 1 && (
        <div className="px-3 pb-2 flex flex-col gap-1">
          <p className="text-xs text-slate-500 mb-1">Try an example:</p>
          {EXAMPLE_QUERIES.map((q, i) => (
            <button
              key={i}
              onClick={() => submit(q)}
              disabled={loading}
              className="text-left text-xs text-slate-400 hover:text-slate-200 hover:bg-slate-800 px-2 py-1.5 rounded transition-colors"
            >
              {q}
            </button>
          ))}
        </div>
      )}

      {/* Input */}
      <div className="px-3 py-3 border-t border-slate-700/50 bg-[#1a2035]">
        <div className="flex gap-2">
          <textarea
            value={input}
            onChange={e => setInput(e.target.value)}
            onKeyDown={e => {
              if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); submit(input); }
            }}
            placeholder="Ask about orders, deliveries, billing…"
            disabled={loading}
            rows={2}
            className="flex-1 resize-none bg-slate-800 text-slate-200 placeholder-slate-500 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-1 focus:ring-indigo-500 disabled:opacity-50"
          />
          <button
            onClick={() => submit(input)}
            disabled={loading || !input.trim()}
            className="px-3 py-2 bg-indigo-600 hover:bg-indigo-500 disabled:bg-slate-700 disabled:text-slate-500 text-white rounded-lg text-sm font-medium transition-colors self-end"
          >
            Send
          </button>
        </div>
      </div>
    </div>
  );
}
