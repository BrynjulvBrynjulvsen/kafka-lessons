import { test, expect } from '@playwright/test';

test('shared membership widget selects groups and shows transitions, assignments and stale state', async ({ page }) => {
  const topic = 'kafka-demo-lab'; let socket; let sockets = 0; const commands = [];
  const state = { type:'experiment-snapshot', version:1, enabled:true, topic, groups:['demo-a','demo-b'], partitions:[0,1,2],
    members:[{ id:'a1',group:'demo-a',state:'running',assignment:[0,1,2],position:{},processed:{},committed:{} }],
    offsets:[],events:[],sampledAt:Date.now(),at:Date.now(),error:null,groupDelays:{'demo-a':1000,'demo-b':0},producing:false };
  await page.route('**/api/topics', r => r.fulfill({json:{topics:[topic],defaultTopic:topic}}));
  await page.route('**/api/experiment', r => {
    if (r.request().method() === 'POST') commands.push(r.request().postDataJSON());
    return r.fulfill({json:state});
  });
  await page.routeWebSocket('**/ws/topics/*', ws => {
    socket = ws; sockets++;
    ws.send(JSON.stringify({type:'subscribed',version:1,topic})); ws.send(JSON.stringify(state));
  });
  await page.goto('/#/lag');
  const widget = page.locator('#lag .group-membership');
  await expect(widget.locator('.membership-summary')).toContainText('1 member');
  await expect(widget.locator('.membership-state')).toHaveText('Running');
  await expect(widget.locator('.membership-chip')).toHaveText(['P0','P1','P2']);
  await widget.getByLabel('Consumer group').selectOption('demo-b');
  await expect(widget.locator('.membership-state')).toHaveText('Inactive');
  await expect(widget.getByRole('button',{name:'Stop group'})).toBeDisabled();
  await expect(widget.locator('[name=delayMs]')).toHaveValue('0');
  await widget.locator('[name=delayMs]').selectOption('200');
  await expect.poll(() => commands[0]).toEqual({action:'delay',group:'demo-b',delayMs:200});
  commands.shift();
  await widget.getByRole('button',{name:'+ Add member'}).click();
  await expect.poll(() => commands).toEqual([{action:'start',group:'demo-b',policy:'earliest'}]);
  await expect(page.locator('#lag .command-status')).toContainText('Command accepted');
  state.members.push({id:'b1',group:'demo-b',state:'joining',assignment:[],position:{},processed:{},committed:{}});
  socket.send(JSON.stringify(state));
  await expect(widget.locator('.membership-state')).toHaveText('Joining');
  await expect(widget.getByRole('button',{name:'+ Add member'})).toBeDisabled();
  state.members[1].state='running'; state.members[1].assignment=[0,1,2]; socket.send(JSON.stringify(state));
  await expect(widget.locator('.membership-state')).toHaveText('Running');
  await widget.getByRole('button',{name:'Stop group'}).click();
  await expect.poll(() => commands[1]).toEqual({action:'stop',group:'demo-b'});
  for (const id of ['groups','offsets']) {
    await page.evaluate(id => {location.hash=`/${id}`;},id);
    await expect(page.locator(`#${id} .membership-summary`)).toContainText('1 member');
  }
  expect(sockets).toBe(1);
  state.error='STALE'; socket.send(JSON.stringify(state));
  await expect(page.locator('#offsets .membership-state')).toHaveText('Unknown');
  await expect(page.locator('#offsets .membership-summary')).toContainText('last known');
});
