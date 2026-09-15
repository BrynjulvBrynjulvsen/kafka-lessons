import { node, groupName, sampleLabel } from './experiment-view.js';
export function mountGroups(root) {
  const view = root.querySelector('[data-role="experiment-view"]');
  function reset() { view.textContent = 'Start a demo member. Browser viewers are not group members.'; }
  reset();
  return { reset, onRecord() {}, onExperiment(s) {
    view.replaceChildren(node('p', sampleLabel(s), 'sample-label'));
    view.firstChild.style.gridColumn = '1 / -1';
    s.groups.forEach(group => {
      const panel = node('article', '', 'group-panel'); panel.append(node('h3', `Group ${groupName(group)}`));
      const members = s.members.filter(m => m.group === group && m.state !== 'stopped');
      panel.append(node('p', `Topic partitions: ${s.partitions.map(p => `P${p}`).join(' · ')}`, 'legend'));
      if (!members.length) panel.append(node('p', 'No active members'));
      members.forEach(member => {
        const row = node('div', '', 'member');
        const delivery = [...s.events].reverse().find(e => e.member === member.id && e.kind === 'delivered');
        row.append(node('strong', `${member.id} · ${member.state}`));
        const links = node('div', '', 'assignment-links');
        if (!member.assignment.length) links.append(node('span', 'No assigned partitions'));
        member.assignment.forEach(p => links.append(node('span', `P${p} →`, 'assignment-chip')));
        row.append(links);
        if (delivery) row.append(node('small', `Last observed: P${delivery.partition} / offset ${delivery.offset} · ${delivery.detail ?? ''}`));
        panel.append(row);
      });
      const error = [...s.events].reverse().find(e => e.group === group && e.kind === 'worker-error');
      if (error) panel.append(node('p', `Worker error: ${error.detail}`, 'command-status'));
      view.append(panel);
    });
  } };
}
