import { mountGroupMembership } from './group-membership.js';
import { node } from '../concepts/experiment-view.js';

export function wireExperimentControls(root, { command, selectTopic, selectedTopic }) {
  const host = root.querySelector('[data-role="experiment-controls"]');
  const status = node('p', 'Loading experiment configuration…', 'command-status');
  status.setAttribute('role', 'status');
  let snapshot; let busy = false;
  const fields = {};
  function field(name, label, options, initial) {
    const wrapper = node('label', label);
    const input = document.createElement(options ? 'select' : 'input');
    input.name = name;
    if (options) options.forEach(([value, label]) => input.add(new Option(label, value)));
    else { input.type = 'number'; input.min = '0'; input.step = '1'; }
    if (initial !== undefined) input.value = initial;
    fields[name] = input; wrapper.append(input); host.append(wrapper); return input;
  }
  async function run(action, payload = {}) {
    if (busy) return;
    if (!snapshot?.enabled || selectedTopic() !== snapshot.topic) { status.textContent = 'Observe the experiment topic first.'; return; }
    busy = true; refresh(); status.textContent = 'Waiting for backend…';
    try {
      snapshot = await command({ action, group: membership.group, ...payload });
      status.textContent = 'Command accepted. Watch the observed state; membership changes take time.';
    } catch (error) { status.textContent = error.message; }
    finally { busy = false; refresh(); }
  }
  function button(text, action) {
    const b = node('button', text); b.type = 'button'; b.addEventListener('click', action); host.append(b); return b;
  }
  const connect = button('Observe experiment topic', () => {
    if (snapshot?.enabled) { selectTopic(snapshot.topic); status.textContent = `Observing ${snapshot.topic}`; }
  });
  const membership = mountGroupMembership(host, { run });
  if (root.dataset.concept === 'offsets') {
    field('partition', 'Partition to reset', []);
    field('offset', 'Resume at offset', null, 0);
    button('Reset inactive group offset', () => {
      const partition = Number(fields.partition.value), offset = Number(fields.offset.value);
      if (!fields.offset.value.trim() || !Number.isSafeInteger(offset) || offset < 0) {
        status.textContent = 'Enter a non-negative safe integer offset.'; return;
      }
      return run('reset', { partition, offset });
    });
  } else {
    button('Send 60 records / 50 ms apart', () => run('produce'));
    button('Stop production', () => run('stop-production'));
  }
  host.append(status);
  function refresh() {
    host.querySelectorAll('button, input, select').forEach(el => { el.disabled = busy || !snapshot?.enabled; });
    membership.update(snapshot, busy || !snapshot?.enabled || selectedTopic() !== snapshot?.topic);
    if (snapshot?.enabled && selectedTopic() !== snapshot.topic) {
      host.querySelectorAll('button').forEach(el => { el.disabled = el !== connect; });
    }
  }
  function options(select, values) {
    const previous = select.value;
    if ([...select.options].map(o => o.value).join() === values.map(String).join()) return;
    select.replaceChildren(...values.map(v => new Option(String(v), String(v))));
    if (values.map(String).includes(previous)) select.value = previous;
  }
  return {
    update(s) {
      const first = !snapshot;
      snapshot = s;

      if (first && s.enabled) status.textContent = 'Ready. Observe the experiment topic to use these controls.';
      if (fields.partition) options(fields.partition, s.partitions);
      if (!s.enabled) status.textContent = 'Experiment disabled. Enable DEMO_EXPERIMENT_ENABLED and configure its topic (README).';
      refresh();
    },
    error(message) { status.textContent = message; },
  };
}
