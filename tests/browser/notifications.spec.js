import { test, expect } from '@playwright/test';

test('notification fixture separates stages, uses safe text and recovers failed commands', async ({ page }) => {
  const topic = 'kafka-demo-lab'; let socket, sockets = 0, places = 0, fail = false;
  const notification = { enabled: true, topic, confirmationTopic: 'kafka-demo-confirmations', group: 'demo-notifications', state: 'stopped', error: null, orders: [], sending: false };
  const state = { type: 'experiment-snapshot', version: 1, enabled: true, topic, groups: ['demo-a', 'demo-b'],
    partitions: [0,1,2], members: [], events: [], offsets: [], groupDelays: { 'demo-a': 0, 'demo-b': 0 },
    at: Date.now(), sampledAt: Date.now(), error: null, producing: false, notifications: notification };
  await page.route('**/api/topics', r => r.fulfill({ json: { topics: ['kafka-demo', topic], defaultTopic: 'kafka-demo' } }));
  await page.route('**/api/experiment', r => r.fulfill({ json: state }));
  await page.routeWebSocket('**/ws/topics/*', ws => {
    sockets++;
    const selected = ws.url().split('/').at(-1);
    ws.send(JSON.stringify({ type: 'subscribed', version: 1, topic: selected }));
    if (selected === topic) { socket = ws; ws.send(JSON.stringify(state)); }
  });
  await page.route('**/api/notifications', r => {
    if (r.request().method() === 'POST') {
      const action = r.request().postDataJSON().action;
      if (action === 'start') notification.state = 'running';
      if (action === 'place') {
        places++;
        if (fail) return r.fulfill({ status: 503, json: { message: 'Service unavailable; inspect state before retrying.' } });
        notification.orders = [{ orderId: 'order-42', eventId: 'event-1', acknowledgedAt: Date.now(), partition: 1, offset: 7 }];
      }
    }
    return r.fulfill({ json: notification });
  });
  await page.goto('/#/demo-record-journey');
  const root = page.locator('#demo-record-journey');
  await expect(root.getByRole('button', { name: 'Start chain' })).toBeEnabled();
  await expect(root.getByRole('button', { name: 'Place order' })).toBeDisabled();
  expect(sockets).toBe(1);
  await root.getByRole('button', { name: 'Start chain' }).click();
  await expect(root.getByRole('button', { name: 'Place order' })).toBeEnabled();
  expect(sockets).toBe(2); // Explicit service start selects the configured demo topic.
  await root.getByRole('button', { name: 'Place order' }).click();
  await expect(root.locator('[data-done="true"]')).toHaveCount(1);
  await expect(root.locator('.notification-card')).toHaveCount(0);
  notification.orders[0].receivedAt = Date.now(); socket.send(JSON.stringify(state));
  await expect(root.locator('[data-done="true"]')).toHaveCount(2);
  notification.orders[0].confirmationAcknowledgedAt = Date.now();
  notification.orders[0].confirmationPartition = 2; notification.orders[0].confirmationOffset = 10;
  socket.send(JSON.stringify(state));
  await expect(root.locator('[data-done="true"]')).toHaveCount(3);
  await expect(root.locator('.notification-card')).toHaveCount(0);
  notification.orders[0].completedAt = Date.now();
  notification.orders[0].inboxPartition = 2; notification.orders[0].inboxOffset = 10;
  notification.orders[0].notification = '<img src=x onerror=alert(1)> order confirmed'; socket.send(JSON.stringify(state));
  await expect(root.locator('[data-done="true"]')).toHaveCount(4);
  await expect(root.locator('.notification-card')).toContainText('<img src=x');
  await expect(root.locator('.notification-card img')).toHaveCount(0);
  fail = true;
  await root.getByRole('button', { name: 'Place order' }).click();
  await expect(root.locator('[data-role="notification-status"]')).toContainText('Service unavailable');
  await expect(root.getByRole('button', { name: 'Place order' })).toBeEnabled();
  expect(places).toBe(2);
  await page.evaluate(() => { location.hash = '/partitioning'; });
  await page.locator('#topic').selectOption('kafka-demo');
  await page.evaluate(() => { location.hash = '/demo-record-journey'; });
  await expect(root.getByRole('button', { name: 'Place order' })).toBeDisabled();
  await root.getByRole('button', { name: 'Observe demo topic' }).click();
  await expect(root.getByRole('button', { name: 'Place order' })).toBeEnabled();
  expect(notification.state).toBe('running');
  expect(places).toBe(2);
  expect(sockets).toBe(4);
  state.error = 'STALE'; socket.send(JSON.stringify(state));
  await expect(root.locator('[data-role="notification-service"]')).toContainText('STALE');
  await expect(root.getByRole('button', { name: 'Place order' })).toBeDisabled();
});

test('notification chain consumes the second-topic result across navigation and reconnect', async ({ page, request }) => {
  let sockets = 0; const errors = [];
  page.on('websocket', () => sockets++); page.on('pageerror', error => errors.push(error.message));
  await request.post('/api/notifications', { data: { action: 'stop' } });
  await expect.poll(async () => (await (await request.get('/api/notifications')).json()).state).toBe('stopped');
  await request.post('/api/notifications', { data: { action: 'clear' } });
  try {
    await page.goto('/#/demo-record-journey');
    const root = page.locator('#demo-record-journey');
    await root.getByRole('button', { name: 'Start chain' }).click();
    await expect(root.getByRole('button', { name: 'Place order' })).toBeEnabled({ timeout: 30000 });
    await root.locator('[name="order-id"]').fill('order-browser-test');
    const count = sockets;
    await root.getByRole('button', { name: 'Place order' }).click();
    await expect(root.locator('.notification-card')).toContainText('Order order-browser-test received.', { timeout: 15000 });
    await expect(root.locator('[data-done="true"]')).toHaveCount(4);
    const snapshot = await (await request.get('/api/notifications')).json();
    await expect(root.locator('.notification-card')).toContainText(snapshot.confirmationTopic);
    expect(snapshot.confirmationTopic).not.toBe(snapshot.topic);
    expect(snapshot.orders[0].inboxOffset).toBe(snapshot.orders[0].confirmationOffset);
    for (const width of [1440, 1024]) {
      await page.setViewportSize({ width, height: width === 1440 ? 960 : 768 });
      await page.screenshot({ path: `build/notification-live-${width}.png`, animations: 'disabled' });
      expect(await root.evaluate(el => { const r=el.getBoundingClientRect();return r.top>=0 && r.bottom<=innerHeight; })).toBe(true);
    }
    await page.evaluate(() => { location.hash = '/partitioning'; });
    expect(sockets).toBe(count);
    await page.getByRole('button', { name: 'Reconnect stream' }).click();
    await expect.poll(() => sockets).toBe(count + 1);
    await page.evaluate(() => { location.hash = '/demo-record-journey'; });
    await expect(root.locator('.notification-card')).toHaveCount(1);
    expect((await (await request.get('/api/notifications')).json()).state).toBe('running');
    expect(errors).toEqual([]);
  } finally {
    await request.post('/api/notifications', { data: { action: 'stop' } });
    await expect.poll(async () => (await (await request.get('/api/notifications')).json()).state).toBe('stopped');
    await request.post('/api/notifications', { data: { action: 'clear' } });
  }
});
