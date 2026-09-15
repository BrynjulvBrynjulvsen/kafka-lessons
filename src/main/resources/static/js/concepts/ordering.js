export function mountOrdering(root) {
  const view = root.querySelector('[data-role="lanes"]');
  const lanes = new Map();
  function reset() { lanes.clear(); view.textContent = 'Send a numbered sequence to observe partition-local order.'; }
  reset();
  return { reset, onRecord(record) {
    if (!lanes.has(record.partition)) lanes.set(record.partition, []);
    const records = lanes.get(record.partition);
    records.push(record); if (records.length > 6) records.shift();
    view.replaceChildren();
    [...lanes].sort(([a], [b]) => a - b).forEach(([partition, records]) => {
      const row = document.createElement('article'); row.className = 'order-lane';
      const title = document.createElement('strong'); title.textContent = `Partition ${partition}`; row.append(title);
      records.forEach(record => {
        const card = document.createElement('div'); card.className = 'order-card';
        const label = document.createElement('b'); label.textContent = `offset ${record.offset}`;
        const text = document.createElement('span'); text.textContent = `${record.key ?? '(null key)'}: ${record.value ?? '(tombstone)'}`;
        card.append(label, text); row.append(card);
      });
      view.append(row);
    });
  } };
}
