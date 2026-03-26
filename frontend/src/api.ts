import type { GraphSchema, ExpandResult, ChatResponse } from './types';

const BASE_URL = import.meta.env.VITE_API_URL ?? '';

async function apiFetch<T>(path: string, options?: RequestInit): Promise<T> {
  const res = await fetch(`${BASE_URL}${path}`, options);
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`API error ${res.status}: ${text}`);
  }
  return res.json() as Promise<T>;
}

export function fetchSchema(): Promise<GraphSchema> {
  return apiFetch('/api/graph/schema');
}

export function fetchNodes(type: string, page = 1, limit = 50) {
  return apiFetch<{ type: string; nodes: Record<string, unknown>[]; page: number; count: number }>(
    `/api/graph/nodes?type=${encodeURIComponent(type)}&page=${page}&limit=${limit}`
  );
}

export function expandNode(nodeType: string, nodeId: string): Promise<ExpandResult> {
  return apiFetch(
    `/api/graph/expand?nodeType=${encodeURIComponent(nodeType)}&nodeId=${encodeURIComponent(nodeId)}`
  );
}

export function sendChat(message: string, history: { role: string; content: string }[]): Promise<ChatResponse> {
  return apiFetch('/api/chat', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ message, history }),
  });
}
