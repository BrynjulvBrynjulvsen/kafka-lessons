import { test, expect } from '@playwright/test';

test('narrative keeps live demos connected and actions explicitly triggered', async ({ page }) => {
  const topic = 'kafka-demo';
  const errors = [];
  let sockets = 0, posts = 0, stream;
  const state = { type: 'experiment-snapshot', version: 1, enabled: true, topic,
    groups: ['demo-a', 'demo-b'], partitions: [0, 1, 2], members: [], events: [], offsets: [],
    at: Date.now(), sampledAt: Date.now(), error: null,
    groupDelays: { 'demo-a': 0, 'demo-b': 0 }, producing: false };
  page.on('pageerror', error => errors.push(error.message));
  await page.route('**/api/topics', route => route.fulfill({ json: { topics: [topic], defaultTopic: topic } }));
  await page.route('**/api/experiment', route => {
    if (route.request().method() === 'POST') posts++;
    return route.fulfill({ json: state });
  });
  await page.routeWebSocket('**/ws/topics/*', socket => {
    sockets++; stream = socket;
    socket.send(JSON.stringify({ type: 'subscribed', version: 1, topic }));
    socket.send(JSON.stringify(state));
  });
  await page.route('**/api/messages', async route => {
    posts++;
    const { key, value } = route.request().postDataJSON();
    const metadata = { topic, partition: 1, offset: 7, timestamp: Date.now() };
    await route.fulfill({ json: metadata });
    stream.send(JSON.stringify({ type: 'record-consumed', version: 1, ...metadata, key, value, headers: [] }));
  });
  await page.goto('/');
  await expect(page.locator('#connection')).toContainText('Subscribed');
  for (const id of ['choose-key', 'partitioning', 'ordering-meaning', 'ordering',
    'consumer-instances', 'groups', 'customer-experience', 'recovery-predict', 'lag-predict', 'lag',
    'duplicate-effects', 'client-questions', 'first-session-end', 'offsets', 'lag']) {
    await page.evaluate(id => { location.hash = `/${id}`; }, id);
    await expect(page.locator('section.present')).toHaveAttribute('id', id);
  }
  await expect(page.locator('[data-concept]')).toHaveCount(6);
  expect(sockets).toBe(1);
  expect(posts).toBe(0);
  await page.evaluate(() => { location.hash = '/partitioning'; });
  await page.getByRole('button', { name: 'Produce record' }).click();
  await expect(page.locator('#produce-status')).toContainText('offset 7');
  await expect(page.locator('#partitions .record')).toContainText('OrderPlaced');
  expect(posts).toBe(1);
  expect(sockets).toBe(1);
  expect(errors).toEqual([]);
});
