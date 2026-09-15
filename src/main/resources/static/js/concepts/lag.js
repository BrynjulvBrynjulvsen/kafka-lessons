import { node, sampleLabel, groupName } from './experiment-view.js';
export function mountLag(root) {
  const view = root.querySelector('[data-role="experiment-view"]');
  const history = new Map(); let lastSample;
  function reset() { history.clear(); lastSample = null; view.textContent = 'Waiting for initialized group commits…'; }
  reset();
  return { reset, onRecord() {}, onExperiment(s) {
    const fresh = !s.error && s.sampledAt && s.at - s.sampledAt < 5000;
    if (fresh && s.sampledAt !== lastSample) {
      s.offsets.forEach(o => {
        const key = `${o.group}/${o.partition}`;
        if (!history.has(key)) history.set(key, []);
        const points = history.get(key); points.push(o.lag); if (points.length > 40) points.shift();
      });
      lastSample = s.sampledAt;
    }
    view.replaceChildren(node('p', sampleLabel(s), 'sample-label'));
    const maximum = Math.max(1, ...[...history.values()].flat().filter(Number.isFinite));
    s.offsets.forEach(o => {
      const row = node('div', '', 'lag-row');
      row.append(node('strong', `${groupName(o.group)} / P${o.partition}`));
      const bars = node('div', '', 'lag-history');
      (history.get(`${o.group}/${o.partition}`) ?? []).forEach(value => {
        const bar = node('i', '', 'lag-bar');
        bar.style.height = `${value === null || value < 0 ? 0 : Math.max(2, value / maximum * 48)}px`;
        bar.title = value === null ? 'No commit' : `Lag ${value}`; bars.append(bar);
      });
      row.append(bars, node('span', o.lag === null ? 'No commit' : `${fresh ? '' : 'Stale: '}${o.lag}${o.lag < 0 ? ' (inconsistent)' : ''}`));
      view.append(row);
    });
    view.append(node('p', `Shared scale 0–${maximum} · last 40 successful samples · ${s.groups.map(g => `${groupName(g)}: ${s.groupDelays?.[g] ?? "—"} ms`).join(" · ")} per record`, 'legend'));
  } };
}
