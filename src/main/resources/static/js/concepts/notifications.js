// These results come from the notification service, never from observer record cards.
export function mountNotifications(root) {
  const inbox = root.querySelector('[data-role="notification-inbox"]');
  const stages = root.querySelectorAll('[data-role="notification-stage"]');
  const service = root.querySelector('[data-role="notification-service"]');
  function reset() {
    inbox.replaceChildren();
    const empty = document.createElement('p'); empty.className = 'notification-empty';
    empty.textContent = 'A generated confirmation will appear here.'; inbox.append(empty);
    stages.forEach(stage => { stage.dataset.done = 'false'; stage.querySelector('small').textContent = 'Waiting'; });
    service.textContent = 'Discovering notification service…';
  }
  function onNotification(snapshot) {
    if (!snapshot || !Array.isArray(snapshot.orders)) return;
    service.textContent = `${snapshot.stale ? 'STALE · ' : ''}Chain: ${snapshot.state} · ${snapshot.topic} → ${snapshot.confirmationTopic}`;
    if (snapshot.error) service.textContent += ` · ${snapshot.error}`;
    const latest = snapshot.orders.at(-1);
    const fields = ['acknowledgedAt', 'receivedAt', 'confirmationAcknowledgedAt', 'completedAt'];
    fields.forEach((field, i) => {
      const done = Number.isFinite(latest?.[field]);
      stages[i].dataset.done = String(done);
      stages[i].querySelector('small').textContent = done
        ? (i === 0 ? `P${latest.partition} · offset ${latest.offset}` : i === 2
          ? `P${latest.confirmationPartition} · offset ${latest.confirmationOffset}` : new Date(latest[field]).toLocaleTimeString())
        : (i === 0 && latest?.sendError ? 'Acknowledgment unknown' : 'Waiting');
    });
    const messages = snapshot.orders.filter(order => typeof order.notification === 'string' && Number.isFinite(order.completedAt)).slice(-4).reverse();
    inbox.replaceChildren();
    if (!messages.length) {
      const empty = document.createElement('p'); empty.className = 'notification-empty';
      empty.textContent = latest?.receivedAt ? 'Service is preparing the confirmation…' : 'A generated confirmation will appear here.';
      inbox.append(empty);
    }
    for (const order of messages) {
      const card = document.createElement('article'); card.className = 'notification-card';
      const title = document.createElement('strong'); title.textContent = order.orderId;
      const body = document.createElement('p'); body.textContent = order.notification;
      const metadata = document.createElement('small'); metadata.textContent = `${snapshot.confirmationTopic} · P${order.inboxPartition} / offset ${order.inboxOffset}`;
      card.append(title, body, metadata); inbox.append(card);
    }
  }
  reset();
  return { reset, onRecord() {}, onNotification };
}
