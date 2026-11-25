import type { Health, Incident, IncidentDetail, Overview, ScenarioName } from './types'

const API_BASE = import.meta.env.VITE_API_BASE ?? 'http://127.0.0.1:8000'

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${API_BASE}${path}`, {
    ...init,
    headers: {
      'Content-Type': 'application/json',
      ...init?.headers,
    },
  })
  if (!response.ok) {
    const body = await response.json().catch(() => ({ detail: response.statusText }))
    throw new Error(body.detail ?? `Request failed with status ${response.status}`)
  }
  return response.json() as Promise<T>
}

export const api = {
  overview: () => request<Overview>('/overview'),
  health: () => request<Health>('/health'),
  incidents: () => request<Incident[]>('/incidents'),
  incident: (id: string) => request<IncidentDetail>(`/incidents/${id}`),
  loadScenario: (scenario: ScenarioName) =>
    request<{ status: string }>(`/demo/scenarios/${scenario}`, { method: 'POST' }),
  reset: () => request<{ status: string }>('/demo/reset', { method: 'POST' }),
  tick: () => request<{ status: string }>('/demo/worker/tick', { method: 'POST' }),
  approve: (id: string, issueId: string) =>
    request<{ status: string }>(`/incidents/${id}/approve-retry`, {
      method: 'POST',
      body: JSON.stringify({
        expected_issue_id: issueId,
        approved_by: 'console-operator',
        correlation_id: `console-${Date.now()}`,
      }),
    }),
}
