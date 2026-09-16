import { mountConcepts, dispatchConcepts } from '/kafka-demo/js/deck.js';
import { LiveClient } from './live-client.js';
import { mountNotifications } from './concepts/notifications.js';
import { wireNotifications } from './controls/notification-controls.js';
import { mountPartitioning } from './concepts/partitioning.js';

import { mountOrdering } from './concepts/ordering.js';
import { mountGroups } from './concepts/groups.js';
import { mountOffsets } from './concepts/offsets.js';
import { mountLag } from './concepts/lag.js';
import { wireOrdering } from './controls/ordering-controls.js';
import { wireExperimentControls } from './controls/experiment-controls.js';

const client = new LiveClient();
const concepts = { notifications: mountNotifications, partitioning: mountPartitioning, ordering: mountOrdering, groups: mountGroups, offsets: mountOffsets, lag: mountLag };
const mounted = mountConcepts(document.querySelectorAll('[data-concept]'), concepts);
const topic = document.querySelector('#topic');
const send = document.querySelector('#send');
const status = document.querySelector('#produce-status');
const connection = document.querySelector('#connection');
let busy = false;
client.addEventListener('status', ({ detail }) => {
  connection.textContent = detail.state === 'live' ? detail.text.replace(' · no replay', ' · no browser replay') : detail.text;
  connection.dataset.state = detail.state;
  if (detail.state !== 'live') notificationControls.markStale();
});
function dispatch(method, detail) { dispatchConcepts(mounted, method, detail); }
client.addEventListener('record', ({ detail }) => dispatch('onRecord', detail));
const experimentControls = [...document.querySelectorAll('[data-role="experiment-controls"]')].map(host =>
  wireExperimentControls(host.closest('section'), {
    command: command => client.experiment(command), selectedTopic: () => topic.value,
    selectTopic: name => { topic.value = name; selectTopic(); },
  }));
const notificationControls = wireNotifications(document.querySelector('[data-concept="notifications"]'), {
  command: command => client.notifications(command), selectedTopic: () => topic.value,
  selectTopic: name => { topic.value = name; selectTopic(); },
  render: (snapshot, stale) => dispatch('onNotification', { ...snapshot, stale }),
});
let experimentSnapshot;
function experiment(snapshot) {
  experimentSnapshot = snapshot;
  if (snapshot.topic === topic.value && snapshot.notifications) notificationControls.update(snapshot.notifications, Boolean(snapshot.error));
  experimentControls.forEach(control => control.update(snapshot));
  if (snapshot.topic === topic.value) dispatch('onExperiment', snapshot);
}
client.addEventListener('experiment', ({ detail }) => experiment(detail));
wireOrdering(document.querySelector('[data-concept="ordering"]'), {
  produce: message => client.produce(message), selectedTopic: () => topic.value,
});

function selectTopic() {
  dispatch('reset');
  status.textContent = 'Ready to produce. Subscription does not confirm Kafka consumer assignment.';
  client.connect(topic.value);
  notificationControls.refresh();
  if (experimentSnapshot) experimentControls.forEach(control => control.update(experimentSnapshot));
}
async function discover() {
  try {
    const config = await client.topics();
    topic.replaceChildren(...config.topics.map(name => new Option(name, name)));
    topic.value = config.defaultTopic; topic.disabled = false; send.disabled = false;
    selectTopic();
    client.notifications().then(value => notificationControls.update(value, true)).catch(error => notificationControls.error(error.message));
    client.experiment().then(experiment).catch(error => experimentControls.forEach(c => c.error(error.message)));
  } catch (error) {
    status.textContent = `${error.message} Use Reconnect stream to retry.`;
    connection.textContent = 'Backend unavailable'; connection.dataset.state = 'error';
  }
}
topic.addEventListener('change', selectTopic);
document.querySelector('#reconnect').addEventListener('click', () => topic.value ? client.connect(topic.value) : discover());
document.querySelector('#producer').addEventListener('submit', async event => {
  event.preventDefault();
  if (busy || !topic.value) return;
  busy = true; send.disabled = true; topic.disabled = true;
  status.textContent = 'Waiting for Kafka acknowledgment…';
  try {
    const result = await client.produce({ topic: topic.value,
      key: document.querySelector('#key').value || null, value: document.querySelector('#value').value });
    status.textContent = `Acknowledged · ${result.topic} · partition ${result.partition} · offset ${result.offset}. Watch for the separate consumed observation.`;
  } catch (error) { status.textContent = error.message; }
  finally { busy = false; send.disabled = false; topic.disabled = false; }
});
window.addEventListener('pagehide', () => client.disconnect());
window.addEventListener('pageshow', event => { if (event.persisted && topic.value) client.connect(topic.value); });
void discover();
try {
  const { initializeDeck } = await import('/kafka-demo/js/deck.js');
  await initializeDeck();
} catch {
  connection.textContent = 'Slides could not load reveal.js. Run npm ci and npm run vendor, then rebuild.';
  document.querySelector('.reveal').style.overflow = 'auto';
}
