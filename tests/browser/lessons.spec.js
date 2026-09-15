import { test, expect } from '@playwright/test';

test('numbered ordering and experiment controls preserve one transport', async ({ page, request }) => {
  let sockets = 0;
  const errors = [];
  page.on('websocket', () => sockets++);
  page.on('pageerror', e => errors.push(e.message));
  await page.goto('/#/ordering');
  await expect(page.locator('#connection')).toContainText('Subscribed');
  await page.locator('[data-action="sequence"]').click();
  await expect(page.locator('[data-role="sequence-status"]')).toContainText('Six sends acknowledged');
  await expect(page.locator('.order-card')).toHaveCount(6);
  await page.screenshot({ path: 'build/lesson-ordering.png', animations: 'disabled' });
  const config = await (await request.get('/api/experiment')).json();
  expect(config.enabled).toBe(true);
  await page.goto('/#/groups'); // new page for experiment session
  await expect(page.locator('#groups button').first()).toBeEnabled();
  await page.locator('#groups button').first().click();
  await expect(page.locator('#connection')).toHaveText(`Subscribed · ${config.topic} · no replay`);
  const baselineSockets = sockets;
  // Start only through explicit controls. Stopping first makes reruns deterministic.
  for (const group of config.groups) await request.post('/api/experiment', { data: { action: 'stop', group } });
  await expect.poll(async () => (await (await request.get('/api/experiment')).json()).members.every(m => m.state === 'stopped')).toBe(true);
  await page.locator('#groups').getByRole('button', { name: '+ Add member' }).click();
  await expect(page.locator('#groups .member').first()).toContainText('running', { timeout: 30000 });
  await request.post('/api/experiment', { data: { action: 'delay', group: config.groups[0], delayMs: 200 } });
  await page.locator('#groups').getByRole('button', { name: 'Send 60' }).click();
  await expect(page.locator('#groups .member').first()).toContainText('Last observed', { timeout: 15000 });
  await page.screenshot({ path: 'build/lesson-groups.png', animations: 'disabled' });
  for (const id of ['offsets', 'lag']) {
    await page.evaluate(id => { location.hash = `/${id}`; }, id);
    await expect(page.locator('section.present')).toHaveAttribute('id', id);
    await expect(page.locator(`#${id} [data-role="experiment-view"]`)).toContainText('Broker sample');
    await page.screenshot({ path: `build/lesson-${id}.png`, animations: 'disabled' });
  }
  expect(sockets).toBe(baselineSockets);
  const membersBefore = (await (await request.get('/api/experiment')).json()).members.map(m => m.id);
  await page.evaluate(() => { location.hash = '/partitioning'; });
  await page.getByRole('button', { name: 'Reconnect stream' }).click();
  await expect.poll(() => sockets).toBe(baselineSockets + 1);
  await expect(page.locator('#connection')).toHaveText(`Subscribed · ${config.topic} · no replay`);
  expect((await (await request.get('/api/experiment')).json()).members.map(m => m.id)).toEqual(membersBefore);
  for (const width of [1440, 1024]) {
    await page.setViewportSize({ width, height: width === 1440 ? 960 : 768 });
    for (const id of ['ordering', 'groups', 'offsets', 'lag']) {
      await page.evaluate(id => { location.hash = `/${id}`; }, id);
      await expect(page.locator('section.present')).toHaveAttribute('id', id);
      const fits = await page.locator(`#${id}`).evaluate(el => {
        const box = el.getBoundingClientRect();
        return box.top >= 0 && box.bottom <= innerHeight && box.left >= 0 && box.right <= innerWidth;
      });
      expect(fits).toBe(true);
      await page.screenshot({ path: `build/lesson-${id}-${width}.png`, animations: 'disabled' });
    }
  }
  await request.post('/api/experiment', { data: { action: 'delay', group: config.groups[0], delayMs: 0 } });
  await request.post('/api/experiment', { data: { action: 'stop-production' } });
  for (const group of config.groups) await request.post('/api/experiment', { data: { action: 'stop', group } });
  expect(errors).toEqual([]);
});

test('experiment errors recover controls and stale observations are labelled', async ({ page }) => {
  const topic = 'kafka-demo-lab';
  const state = {
    type: 'experiment-snapshot', version: 1, enabled: true, topic, groups: ['demo-a', 'demo-b'],
    partitions: [0], members: [], events: [], offsets: [{ group: 'demo-a', partition: 0, start: 0, end: 10, committed: null, lag: null }],
    at: Date.now(), sampledAt: Date.now(), error: null, groupDelays: { 'demo-a': 0, 'demo-b': 0 }, producing: false,
  };
  await page.route('**/api/topics', r => r.fulfill({ json: { topics: [topic], defaultTopic: topic } }));
  let commands = 0;
  await page.route('**/api/experiment', r => {
    if (r.request().method() === 'GET') return r.fulfill({ json: state });
    commands++; return r.fulfill({ status: 409, json: { message: 'Stop all group members first' } });
  });
  await page.routeWebSocket('**/ws/topics/*', socket => {
    socket.send(JSON.stringify({ type: 'subscribed', version: 1, topic }));
    socket.send(JSON.stringify(state));
  });
  await page.goto('/#/offsets');
  const reset = page.locator('#offsets').getByRole('button', { name: 'Reset inactive' });
  await expect(reset).toBeEnabled();
  await reset.click();
  await expect(page.locator('#offsets .command-status')).toHaveText('Stop all group members first');
  await expect(reset).toBeEnabled();
  expect(commands).toBe(1);
  await page.evaluate(() => { location.hash = '/lag'; });
  await expect(page.locator('#lag .lag-row')).toContainText('No commit');
  await expect(page.locator('#lag .sample-label')).toContainText('STALE', { timeout: 10000 });
});
