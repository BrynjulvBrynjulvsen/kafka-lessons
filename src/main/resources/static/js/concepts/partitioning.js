// Compiled-in concept boundary: mount once, accept observations, reset the display.
export function mountPartitioning(root) {
  const buckets = root.querySelector('#partitions');
  const counter = root.querySelector('#count');
  const partitions = new Map();
  let count = 0;
  function reset() {
    partitions.clear(); count = 0; counter.textContent = '0';
    const empty = document.createElement('p');
    empty.className = 'empty';
    empty.textContent = 'Waiting for consumed records. Produce a record to discover its partition.';
    buckets.replaceChildren(empty);
  }
  root.querySelector('#clear').addEventListener('click', reset);
  return {
    reset,
    onRecord(record) {
      counter.textContent = String(++count);
      if (!partitions.has(record.partition)) {
        if (!partitions.size) buckets.replaceChildren();
        const column = document.createElement('article'); column.className = 'partition';
        const heading = document.createElement('h3'); heading.textContent = `Partition ${record.partition}`;
        column.append(heading); partitions.set(record.partition, column);
        [...partitions.entries()].sort(([a], [b]) => a - b).forEach(([, node]) => buckets.append(node));
      }
      const column = partitions.get(record.partition);
      const card = document.createElement('div'); card.className = 'record';
      const offset = document.createElement('strong'); offset.textContent = `OFFSET ${record.offset}`;
      const key = document.createElement('p'); key.textContent = record.key === null ? '(null key)' : `key: ${record.key}`;
      const value = document.createElement('small'); value.textContent = record.value === null ? '(tombstone)' : record.value;
      value.style.maxHeight = '32px'; value.style.overflow = 'auto';
      card.append(offset, key, value); column.insertBefore(card, column.children[1] ?? null);
      while (column.children.length > 5) column.lastElementChild.remove();
    },
  };
}
