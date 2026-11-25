import { renderToStaticMarkup } from 'react-dom/server'
import { describe, expect, it } from 'vitest'
import { StatusBadge } from './StatusBadge'

describe('StatusBadge', () => {
  it('labels resolved incidents as verified recoveries', () => {
    const html = renderToStaticMarkup(<StatusBadge state="resolved" />)
    expect(html).toContain('Verified recovery')
  })

  it('does not mislabel manual review as success', () => {
    const html = renderToStaticMarkup(<StatusBadge state="manual_review" />)
    expect(html).toContain('Manual review')
    expect(html).not.toContain('Verified recovery')
  })
})
