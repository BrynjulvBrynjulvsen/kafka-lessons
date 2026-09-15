import { node, table, sampleLabel, groupName } from './experiment-view.js';
export function mountOffsets(root) {
  const view = root.querySelector('[data-role="experiment-view"]');
  function reset() { latest = null; view.textContent = 'Waiting for experiment offsets…'; }
  let latest;
  root.addEventListener('change', () => { if (latest) render(latest); });
  reset();
  function render(s) {
    latest = s;
    const rows = s.offsets.map(o => {
      const member = s.members.find(m => m.group === o.group && m.assignment.includes(o.partition));
      return [groupName(o.group), `P${o.partition}`, o.start, member?.position[o.partition],
        member?.processed[o.partition], o.committed, o.end];
    });
    view.replaceChildren(node('p', sampleLabel(s), 'sample-label'),
      table(['Group', 'Lane', 'Start', 'Position →', 'Processed →', 'Commit →', 'End →'], rows));
    const selected = s.offsets.find(o => o.group === root.querySelector('[name="group"]')?.value
      && o.partition === Number(root.querySelector('[name="partition"]')?.value));
    if (selected) {
      const member = s.members.find(m => m.group === selected.group && m.assignment.includes(selected.partition));
      const timeline = node('div', '', 'offset-timeline');
      timeline.append(node('p', `Selected lane ${groupName(selected.group)} / P${selected.partition} · retained range ${selected.start}–${selected.end}`, 'legend'));
      [['Fetch', member?.position[selected.partition]], ['Processed', member?.processed[selected.partition]], ['Commit', selected.committed]].forEach(([label, value]) => {
        const row = node('div', '', 'offset-track-row');
        const track = node('div', '', 'offset-track');
        if (value != null) {
          const marker = node('i', '', 'offset-marker');
          marker.style.left = `${Math.max(0, Math.min(100, (value - selected.start) / Math.max(1, selected.end - selected.start) * 100))}%`;
          track.append(marker);
        }
        row.append(node('span', label), track, node('b', value ?? '—')); timeline.append(row);
      });
      view.insertBefore(timeline, view.querySelector('table'));
    }
    const recent = s.events.filter(e => ['committed', 'offset-reset', 'worker-error'].includes(e.kind)).slice(-1);
    recent.forEach(e => view.append(node('p', `${e.kind} · group ${groupName(e.group ?? '')} · P${e.partition ?? '—'} · ${e.offset ?? e.detail ?? ''}`, 'event-line')));
  }
  return { reset, onRecord() {}, onExperiment: render };
}
