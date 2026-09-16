export function wireNotifications(root, { command, selectedTopic, selectTopic, render }) {
  const status = root.querySelector('[data-role="notification-status"]');
  const observe = root.querySelector('[data-action="notification-observe"]');
  const start = root.querySelector('[data-action="notification-start"]');
  const stop = root.querySelector('[data-action="notification-stop"]');
  const place = root.querySelector('[data-action="notification-place"]');
  const clear = root.querySelector('[data-action="notification-clear"]');
  let snapshot, busy = false, stale = true;
  function refresh() {
    const active = ['starting', 'running', 'stopping'].includes(snapshot?.state);
    observe.disabled = busy || !snapshot?.enabled;
    start.disabled = busy || !snapshot?.enabled || active;
    stop.disabled = busy || !active;
    place.disabled = busy || stale || snapshot?.state !== 'running' || selectedTopic() !== snapshot?.topic || snapshot?.sending
      || snapshot?.orders?.some(order => !order.completedAt && !order.sendError);
    clear.disabled = busy || !snapshot || active;
  }
  function update(value, isStale = false) {
    if (!value || !Array.isArray(value.orders)) return;
    snapshot = value; stale = isStale; refresh(); render(value, stale);
    if (value.orders.at(-1)?.sendError) status.textContent = value.orders.at(-1).sendError;
  }
  async function run(action, orderId) {
    if (busy) return;
    busy = true; refresh(); status.textContent = action === 'place' ? 'Publishing OrderPlaced…' : 'Updating service…';
    try {
      // Only an explicit presenter action changes the shared observed topic.
      if (snapshot?.topic && selectedTopic() !== snapshot.topic) selectTopic(snapshot.topic);
      const result = await command({ action, ...(orderId ? { orderId } : {}) });
      update(result);
      status.textContent = result.orders.at(-1)?.sendError || (action === 'place'
        ? 'Order acknowledged. Watch the confirmation event reach the inbox consumer.'
        : action === 'clear' ? 'Local inbox cleared. Kafka records are unchanged.' : 'Command accepted. Watch the service state.');
    } catch (error) { status.textContent = error.message; }
    finally { busy = false; refresh(); }
  }
  observe.addEventListener('click', () => {
    if (!snapshot) return;
    selectTopic(snapshot.topic);
    status.textContent = 'Observing the notification topic. Service membership is unchanged.';
  });
  start.addEventListener('click', () => run('start'));
  stop.addEventListener('click', () => run('stop'));
  clear.addEventListener('click', () => run('clear'));
  root.querySelector('form').addEventListener('submit', event => {
    event.preventDefault();
    if (!place.disabled) void run('place', root.querySelector('[name="order-id"]').value);
  });
  refresh();
  return { update, refresh, markStale() { stale = true; refresh(); if (snapshot) render(snapshot, true); }, error(error) { stale = true; status.textContent = error; refresh(); } };
}
