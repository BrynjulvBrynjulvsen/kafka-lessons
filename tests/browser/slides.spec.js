import { test, expect } from '@playwright/test';

// These smoke tests intentionally produce records through the running backend.
test('live records, bounded cards, navigation, reconnect and safe text', async ({ page }) => {
  const errors = [];
  const externalRequests = [];
  let sockets = 0;
  page.on('pageerror', error => errors.push(error.message));
  page.on('websocket', () => sockets++);
  page.on('request', request => {
    if (new URL(request.url()).origin !== new URL(test.info().project.use.baseURL).origin) externalRequests.push(request.url());
  });
  await page.goto('/');
  await expect(page.locator('.reveal')).toHaveClass(/ready/);
  await expect(page.locator('#connection')).toContainText('Subscribed');
  await expect(page.locator('body')).toHaveCSS('background-color', 'rgb(238, 243, 247)');
  await page.screenshot({ path: 'build/slides-title.png', animations: 'disabled' });
  await page.keyboard.press('ArrowRight');
  await page.keyboard.press('ArrowRight');
  await expect(page.locator('section.present')).toHaveAttribute('data-concept', 'partitioning');
  await page.locator('#key').fill(`browser-test-${Date.now()}`);
  const payload = '<img src=x onerror=alert(1)> live test';
  await page.locator('#value').fill(payload);
  await page.locator('#value').press('Space');
  await expect(page.locator('section.present')).toHaveAttribute('data-concept', 'partitioning');
  let partition;
  for (let index = 0; index < 6; index++) {
    const response = page.waitForResponse(response => response.url().endsWith('/api/messages') && response.request().method() === 'POST');
    await page.getByRole('button', { name: 'Produce record' }).click();
    const metadata = await (await response).json();
    partition ??= metadata.partition;
    expect(metadata.partition).toBe(partition);
    await expect(page.locator('#produce-status')).toContainText(`offset ${metadata.offset}`);
    const column = page.locator('.partition').filter({ has: page.getByRole('heading', { name: `Partition ${partition}`, exact: true }) });
    await expect(column.locator('.record').first()).toContainText(`OFFSET ${metadata.offset}`);
    await expect(column.locator('.record').first()).toContainText(payload);
    await expect(column.locator('.record')).toHaveCount(Math.min(index + 1, 4));
  }
  await expect(page.locator('#partitions img')).toHaveCount(0);
  await page.screenshot({ path: 'build/slides-live.png', animations: 'disabled' });
  await page.locator('#send').blur();
  await page.keyboard.press('ArrowRight');
  await page.keyboard.press('ArrowLeft');
  expect(sockets).toBe(1);
  await page.getByRole('button', { name: 'Clear display' }).click();
  await expect(page.locator('.record')).toHaveCount(0);
  await page.getByRole('button', { name: 'Reconnect stream' }).click();
  await expect.poll(() => sockets).toBe(2);
  await expect(page.locator('#connection')).toContainText('Subscribed');
  await expect(page.locator('.record')).toHaveCount(0);
  expect(errors).toEqual([]);
  expect(externalRequests).toEqual([]);
});

test('failed production is explained and controls recover without automatic retry', async ({ page }) => {
  let posts = 0;
  await page.route('**/api/messages', route => { posts++; return route.fulfill({ status: 503, body: '{}' }); });
  await page.goto('/#/partitioning');
  await expect(page.locator('#send')).toBeEnabled();
  await page.getByRole('button', { name: 'Produce record' }).click();
  await expect(page.locator('#produce-status')).toContainText('retrying can duplicate');
  await expect(page.locator('#send')).toBeEnabled();
  expect(posts).toBe(1);
});
