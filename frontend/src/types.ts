export type IncidentState =
  | 'discovered'
  | 'awaiting_approval'
  | 'approved'
  | 'executing'
  | 'verifying'
  | 'resolved'
  | 'manual_review'

export interface Overview {
  total: number
  open: number
  awaiting_approval: number
  verified_recoveries: number
  manual_review: number
}

export interface Health {
  backend: string
  simulator_connected: boolean
  worker: string
  last_worker_event: string | null
}

export interface Incident {
  id: string
  latest_issue_id: string
  base_id: string
  destination_app_id: string
  destination_table_id: string
  record_identity: string
  state: IncidentState
  version: number
  created_at: string
  updated_at: string
  action: string | null
  issue_message: string | null
  source_record_id: string | null
  destination_record_id: string | null
  detection_method: string
}

export interface FieldComparison {
  name: string
  source: unknown
  destination: unknown
  matches: boolean
}

export interface AuditEvent {
  sequence: number
  event_type: string
  correlation_id: string
  incident_id: string | null
  job_id: string | null
  from_state: string | null
  to_state: string | null
  details: Record<string, unknown>
  occurred_at: string
}

export interface IncidentDetail {
  incident: Incident
  occurrence: {
    issue_id: string
    action: string
    issue_message: string
    changed_columns: Record<string, unknown> | null
    created_time: string
    last_modified_time: string
  } | null
  comparison: {
    available: boolean
    fields: FieldComparison[]
    warning: string | null
  }
  issue_open: boolean | null
  recovery_eligible: boolean
  verification_evidence: AuditEvent | null
  timeline: AuditEvent[]
}

export type ScenarioName =
  | 'missed_webhook'
  | 'verified_recovery'
  | 'false_success'
  | 'ambiguous_outcome'
