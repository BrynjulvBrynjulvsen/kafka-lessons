// Small DOM helpers shared by independent experiment concepts.
export function node(tag, text, className) {
  const el = document.createElement(tag); el.textContent = text;
  if (className) el.className = className;
  return el;
}
export function sampleLabel(snapshot) {
  if (snapshot.error) return snapshot.error;
  if (!snapshot.sampledAt) return 'Waiting for broker offsets…';
  const age = snapshot.at - snapshot.sampledAt;
  return `${age > 5000 ? 'STALE · ' : ''}Broker sample ${new Date(snapshot.sampledAt).toLocaleTimeString()} · non-atomic samples`;
}
export function groupName(group) { return group.endsWith('-a') ? 'A' : 'B'; }
export function table(headers, rows) {
  const result = document.createElement('table');
  const head = document.createElement('tr'); headers.forEach(h => head.append(node('th', h))); result.append(head);
  rows.forEach(row => { const tr = document.createElement('tr'); row.forEach(cell => tr.append(node('td', cell ?? '—'))); result.append(tr); });
  return result;
}
