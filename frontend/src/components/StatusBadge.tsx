import type { IncidentState } from '../types'

const LABELS: Record<IncidentState, string> = {
  discovered: 'Discovered',
  awaiting_approval: 'Awaiting approval',
  approved: 'Approved',
  executing: 'Executing',
  verifying: 'Verifying',
  resolved: 'Verified recovery',
  manual_review: 'Manual review',
}

export function StatusBadge({ state }: { state: IncidentState }) {
  return <span className={`status-badge status-${state}`}>{LABELS[state]}</span>
}
