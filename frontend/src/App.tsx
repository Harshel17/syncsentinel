import { useCallback, useEffect, useMemo, useState } from 'react'
import { api } from './api'
import { StatusBadge } from './components/StatusBadge'
import type {
  Health,
  Incident,
  IncidentDetail,
  Overview,
  ScenarioName,
} from './types'
import './styles.css'

const EMPTY_OVERVIEW: Overview = {
  total: 0,
  open: 0,
  awaiting_approval: 0,
  verified_recoveries: 0,
  manual_review: 0,
}

const SCENARIOS: Array<{
  id: ScenarioName
  title: string
  description: string
  outcome: string
}> = [
  {
    id: 'missed_webhook',
    title: 'Missed webhook',
    description: 'Create an issue with no notification delivery.',
    outcome: 'Scheduled reconciliation discovers the failure.',
  },
  {
    id: 'verified_recovery',
    title: 'Verified recovery',
    description: 'Retry a record whose source and destination differ.',
    outcome: 'Resolution requires convergence and issue closure.',
  },
  {
    id: 'false_success',
    title: 'False success prevention',
    description: 'Return API success without updating the destination.',
    outcome: 'The incident remains unresolved for manual review.',
  },
  {
    id: 'ambiguous_outcome',
    title: 'Ambiguous API outcome',
    description: 'Apply recovery, then simulate a response timeout.',
    outcome: 'Verification runs without blind re-execution.',
  },
]

const formatTime = (value: string | null | undefined) =>
  value
    ? new Intl.DateTimeFormat('en', {
        month: 'short',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
      }).format(new Date(value))
    : 'Not observed'

const shortId = (value: string) => `${value.slice(0, 8)}…${value.slice(-4)}`

function App() {
  const [overview, setOverview] = useState<Overview>(EMPTY_OVERVIEW)
  const [health, setHealth] = useState<Health | null>(null)
  const [incidents, setIncidents] = useState<Incident[]>([])
  const [selectedId, setSelectedId] = useState<string | null>(null)
  const [detail, setDetail] = useState<IncidentDetail | null>(null)
  const [loading, setLoading] = useState(true)
  const [action, setAction] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [activeScenario, setActiveScenario] = useState<ScenarioName | null>(null)

  const refresh = useCallback(async (quiet = false) => {
    if (!quiet) setLoading(true)
    try {
      const [nextOverview, nextHealth, nextIncidents] = await Promise.all([
        api.overview(),
        api.health(),
        api.incidents(),
      ])
      setOverview(nextOverview)
      setHealth(nextHealth)
      setIncidents(nextIncidents)
      const nextSelected = nextIncidents.some((incident) => incident.id === selectedId)
        ? selectedId
        : nextIncidents[0]?.id ?? null
      if (nextSelected) {
        const nextDetail = await api.incident(nextSelected)
        setSelectedId(nextSelected)
        setDetail(nextDetail)
      } else {
        setSelectedId(null)
        setDetail(null)
      }
      setError(null)
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : 'Unable to load console data')
    } finally {
      if (!quiet) setLoading(false)
    }
  }, [selectedId])

  useEffect(() => {
    const initial = window.setTimeout(() => void refresh(), 0)
    const timer = window.setInterval(() => void refresh(true), 2500)
    return () => {
      window.clearTimeout(initial)
      window.clearInterval(timer)
    }
  }, [refresh])

  const selectIncident = async (id: string) => {
    setSelectedId(id)
    try {
      setDetail(await api.incident(id))
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : 'Unable to load incident')
    }
  }

  const execute = async (name: string, operation: () => Promise<unknown>) => {
    setAction(name)
    setError(null)
    try {
      await operation()
      await refresh(true)
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : 'Action failed')
    } finally {
      setAction(null)
    }
  }

  const loadScenario = (scenario: ScenarioName) =>
    execute(`scenario-${scenario}`, async () => {
      await api.loadScenario(scenario)
      setActiveScenario(scenario)
      setSelectedId(null)
    })

  const reset = () =>
    execute('reset', async () => {
      await api.reset()
      setActiveScenario(null)
      setSelectedId(null)
    })

  const selectedScenario = useMemo(
    () => SCENARIOS.find((scenario) => scenario.id === activeScenario),
    [activeScenario],
  )

  return (
    <div className="app-shell">
      <header className="topbar">
        <div className="brand-block">
          <div className="brand-mark" aria-hidden="true"><span /></div>
          <div>
            <div className="brand-name">SyncSentinel</div>
            <div className="brand-subtitle">Reliability operator console</div>
          </div>
        </div>
        <div className="header-status">
          <span className="environment-label">Simulated environment</span>
          <span className={`connection ${health?.backend === 'online' ? 'is-online' : ''}`}>
            <i /> Backend {health?.backend ?? 'checking'}
          </span>
          <span className={`connection ${health?.simulator_connected ? 'is-online' : ''}`}>
            <i /> Simulator {health?.simulator_connected ? 'connected' : 'waiting'}
          </span>
          <span className="connection is-online"><i /> Worker {health?.worker ?? 'checking'}</span>
        </div>
      </header>

      <main className="workspace">
        <aside className="scenario-panel">
          <div className="section-kicker">Demonstration controls</div>
          <h2>Failure scenarios</h2>
          <p className="section-copy">Each scenario resets local data and exercises the real durable workflow.</p>
          <div className="scenario-list">
            {SCENARIOS.map((scenario) => (
              <button
                className={`scenario-button ${activeScenario === scenario.id ? 'is-active' : ''}`}
                disabled={action !== null}
                key={scenario.id}
                onClick={() => void loadScenario(scenario.id)}
              >
                <span>{scenario.title}</span>
                <small>{scenario.description}</small>
              </button>
            ))}
          </div>
          {selectedScenario && (
            <div className="scenario-note">
              <strong>Expected evidence</strong>
              <span>{selectedScenario.outcome}</span>
            </div>
          )}
          <div className="scenario-actions">
            <button
              className="button button-primary"
              disabled={action !== null || incidents.length === 0}
              onClick={() => void execute('tick', api.tick)}
            >
              {action === 'tick' ? 'Worker running…' : 'Run worker cycle'}
            </button>
            <button className="button button-secondary" disabled={action !== null} onClick={() => void reset()}>
              Reset environment
            </button>
          </div>
          <div className="local-warning">
            Local demonstration only. Operator endpoints are intentionally unauthenticated.
          </div>
        </aside>

        <section className="main-console">
          <div className="console-heading">
            <div>
              <div className="section-kicker">Operational state</div>
              <h1>Sync incident control plane</h1>
            </div>
            <div className="last-observed">Worker observed: {formatTime(health?.last_worker_event)}</div>
          </div>

          {error && <div className="error-banner"><strong>Console error</strong><span>{error}</span></div>}

          <section className="metrics" aria-label="Incident overview">
            {[
              ['Total incidents', overview.total],
              ['Open incidents', overview.open],
              ['Awaiting approval', overview.awaiting_approval],
              ['Verified recoveries', overview.verified_recoveries],
              ['Manual review', overview.manual_review],
            ].map(([label, value]) => (
              <div className="metric" key={label}>
                <span>{label}</span>
                <strong>{value}</strong>
              </div>
            ))}
          </section>

          <section className="data-section">
            <div className="section-header">
              <div><h2>Incidents</h2><span>Durable failures discovered from Stacksync issue polling</span></div>
              <button className="text-button" onClick={() => void refresh()} disabled={loading}>Refresh data</button>
            </div>
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>Incident</th><th>Destination</th><th>Affected record</th><th>Occurrence</th>
                    <th>Error</th><th>Detection</th><th>State</th><th>Updated</th>
                  </tr>
                </thead>
                <tbody>
                  {incidents.map((incident) => (
                    <tr
                      className={selectedId === incident.id ? 'is-selected' : ''}
                      key={incident.id}
                      onClick={() => void selectIncident(incident.id)}
                    >
                      <td><code>{shortId(incident.id)}</code></td>
                      <td><strong>{incident.destination_app_id}</strong><small>{incident.destination_table_id}</small></td>
                      <td><code>{shortId(incident.record_identity)}</code></td>
                      <td><code>{shortId(incident.latest_issue_id)}</code></td>
                      <td className="error-cell" title={incident.issue_message ?? ''}>{incident.issue_message ?? 'Not reported'}</td>
                      <td>{incident.detection_method}</td>
                      <td><StatusBadge state={incident.state} /></td>
                      <td>{formatTime(incident.updated_at)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
              {!loading && incidents.length === 0 && (
                <div className="empty-state"><strong>No incidents loaded</strong><span>Select a demonstration scenario to begin.</span></div>
              )}
              {loading && <div className="empty-state"><span>Loading durable incident state…</span></div>}
            </div>
          </section>

          {detail && (
            <section className="detail-grid">
              <div className="detail-panel">
                <div className="section-header compact">
                  <div><h2>Incident detail</h2><span>{detail.incident.id}</span></div>
                  <StatusBadge state={detail.incident.state} />
                </div>
                <dl className="definition-grid">
                  <div><dt>Current occurrence</dt><dd><code>{detail.incident.latest_issue_id}</code></dd></div>
                  <div><dt>Affected record</dt><dd><code>{detail.incident.record_identity}</code></dd></div>
                  <div><dt>Operation</dt><dd>{detail.occurrence?.action ?? 'Unavailable'}</dd></div>
                  <div><dt>Issue closure</dt><dd>{detail.issue_open === null ? 'Not observed' : detail.issue_open ? 'Issue remains open' : 'Issue not open'}</dd></div>
                </dl>

                <div className="subsection-heading"><h3>Source / destination evidence</h3><span>Simulator connector reads</span></div>
                {detail.comparison.available ? (
                  <div className="comparison-table">
                    <div className="comparison-head"><span>Field</span><span>Source</span><span>Destination</span><span>Result</span></div>
                    {detail.comparison.fields.map((field) => (
                      <div className="comparison-row" key={field.name}>
                        <code>{field.name}</code><span>{String(field.source)}</span><span>{String(field.destination)}</span>
                        <strong className={field.matches ? 'match' : 'mismatch'}>{field.matches ? 'Match' : 'Mismatch'}</strong>
                      </div>
                    ))}
                  </div>
                ) : (
                  <div className="warning-box">{detail.comparison.warning}</div>
                )}

                {detail.verification_evidence && (
                  <div className={`evidence-box ${detail.verification_evidence.event_type === 'verification_succeeded' ? 'success' : 'warning'}`}>
                    <strong>{detail.verification_evidence.event_type === 'verification_succeeded' ? 'Verified evidence' : 'Verification did not pass'}</strong>
                    <span>{String(detail.verification_evidence.details.reason ?? 'No reason recorded')}</span>
                  </div>
                )}

                <div className="operator-action">
                  <div><strong>Operator-approved retry</strong><span>Approval is pinned to occurrence {shortId(detail.incident.latest_issue_id)}.</span></div>
                  <button
                    className="button button-primary"
                    disabled={!detail.recovery_eligible || action !== null}
                    onClick={() => void execute('approve', () => api.approve(detail.incident.id, detail.incident.latest_issue_id))}
                  >
                    {action === 'approve' ? 'Submitting…' : 'Approve retry'}
                  </button>
                </div>
              </div>

              <div className="timeline-panel">
                <div className="section-header compact"><div><h2>Execution timeline</h2><span>Persisted audit evidence only</span></div></div>
                <ol className="timeline">
                  {detail.timeline.map((event) => (
                    <li key={event.sequence}>
                      <i className={event.event_type.includes('failed') || event.event_type.includes('ambiguous') ? 'event-warning' : ''} />
                      <div><strong>{event.event_type.replaceAll('_', ' ')}</strong><span>{String(event.details.reason ?? event.details.issue_id ?? 'Recorded')}</span><time>{formatTime(event.occurred_at)}</time></div>
                    </li>
                  ))}
                  {detail.timeline.length === 0 && <li className="timeline-empty">No incident events recorded.</li>}
                </ol>
              </div>
            </section>
          )}
        </section>
      </main>
    </div>
  )
}

export default App
