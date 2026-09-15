import { node } from '../concepts/experiment-view.js';

// A reusable presenter widget. It consumes snapshots and delegates explicit commands.
export function mountGroupMembership(host, { run }) {
  const widget = node('div', '', 'group-membership');
  const heading = node('label', 'Consumer group', 'membership-heading');
  const select = document.createElement('select'); select.name = 'group';
  heading.append(select);
  const summary = node('div', '', 'membership-summary');
  const count = node('strong', '— members');
  const state = node('span', 'Waiting', 'membership-state'); state.setAttribute('role', 'status');
  summary.append(count, state);
  const assignments = node('div', '', 'membership-assignments');
  assignments.setAttribute('aria-label', 'Member partition assignments');
  const actions = node('div', '', 'membership-actions');
  const add = node('button', '+ Add member'); add.type = 'button';
  const stop = node('button', 'Stop group'); stop.type = 'button';
  actions.append(add, stop);
  const options = document.createElement('details');
  options.append(node('summary', 'Start position'));
  const label = node('label', 'If no valid commit');
  const policy = document.createElement('select'); policy.name = 'policy';
  policy.add(new Option('Earliest retained', 'earliest')); policy.add(new Option('Latest at assignment', 'latest'));
  label.append(policy); options.append(label);
  const delayLabel = node('label', 'Processing delay / record', 'membership-delay');
  const delay = document.createElement('select'); delay.name = 'delayMs';
  [0, 200, 1000].forEach(ms => delay.add(new Option(`${ms} ms`, String(ms))));
  delayLabel.append(delay);
  delay.addEventListener('change', () => run('delay', { group: select.value, delayMs: Number(delay.value) }));
  widget.append(heading, summary, assignments, actions, delayLabel, options); host.append(widget);
  let snapshot; let disabled = true;
  add.addEventListener('click', () => run('start', { group: select.value, policy: policy.value }));
  stop.addEventListener('click', () => run('stop', { group: select.value }));
  select.addEventListener('change', render);
  function render() {
    const members = snapshot?.members.filter(m => m.group === select.value && m.state !== 'stopped') ?? [];
    const stale = !snapshot || snapshot.error || snapshot.at - snapshot.sampledAt > 5000;
    const transitioning = members.some(m => m.state !== 'running');
    const assigned = members.flatMap(m => m.assignment);
    const complete = snapshot?.partitions.length > 0 && snapshot.partitions.every(p => assigned.filter(a => a === p).length === 1);
    const status = stale ? 'Unknown' : !members.length ? 'Inactive' : members.some(m => m.state === 'stopping') ? 'Stopping'
      : members.some(m => m.state === 'joining') ? 'Joining' : transitioning || !complete ? 'Rebalancing' : 'Running';
    count.textContent = `${members.length} ${members.length === 1 ? 'member' : 'members'}${stale ? ' · last known' : ''}`;
    state.textContent = status; state.dataset.state = status.toLowerCase();
    state.title = 'Derived from observed demo workers and their assignments; not a broker group-state query.';
    assignments.replaceChildren();
    if (!members.length) assignments.append(node('p', 'Add a member to start consuming.', 'membership-empty'));
    members.forEach((member, index) => {
      const row = node('div', '', 'membership-row');
      const name = node('span', `Member ${index + 1}`); name.title = member.id;
      const chips = node('div', '', 'membership-partitions');
      member.assignment.forEach(p => chips.append(node('span', `P${p}`, 'membership-chip')));
      if (!member.assignment.length) chips.append(node('span', member.state === 'running' ? 'Idle' : member.state, 'membership-idle'));
      row.append(name, chips); assignments.append(row);
    });
    const ms = snapshot?.groupDelays?.[select.value];
    if (ms != null) {
      if (![...delay.options].some(o => o.value === String(ms))) delay.add(new Option(`${ms} ms`, String(ms)));
      delay.value = String(ms);
    }
    delay.disabled = disabled || stale || ms == null;
    select.disabled = disabled; policy.disabled = disabled;
    add.disabled = disabled || stale || transitioning || members.length >= 4;
    stop.disabled = disabled || !members.length || members.every(m => m.state === 'stopping');
  }
  return {
    get group() { return select.value; },
    update(s, unavailable) {
      snapshot = s; disabled = unavailable;
      if (s && [...select.options].map(o => o.value).join() !== s.groups.join()) {
        const previous = select.value;
        select.replaceChildren(...s.groups.map((group, i) => new Option(`Group ${String.fromCharCode(65 + i)}`, group)));
        if (s.groups.includes(previous)) select.value = previous;
      }
      render();
    },
  };
}
