// Commands live outside visual concepts; production happens only after a click.
export function wireOrdering(root, { produce, selectedTopic }) {
  let busy = false;
  const button = root.querySelector('[data-action="sequence"]');
  const status = root.querySelector('[data-role="sequence-status"]');
  button.addEventListener('click', async () => {
    if (busy) return;
    busy = true; button.disabled = true;
    const topic = selectedTopic();
    const a = root.querySelector('[name="first-key"]').value || null;
    const b = root.querySelector('[name="second-key"]').value || null;
    const run = Date.now().toString(36);
    try {
      for (let n = 1; n <= 3; n++) {
        for (const key of [a, b]) await produce({ topic, key, value: `${run} #${n}` });
      }
      status.textContent = 'Six sends acknowledged. Compare payload numbers with actual offsets; keys may share a lane.';
    } catch (error) { status.textContent = error.message; }
    finally { busy = false; button.disabled = false; }
  });
}
