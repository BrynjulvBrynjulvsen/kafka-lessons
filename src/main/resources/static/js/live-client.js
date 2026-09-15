import { KafkaClient } from '/kafka-demo/js/kafka-client.js';
export class LiveClient extends KafkaClient {
  constructor() {
    super();
    this.addEventListener('observation', ({ detail: event }) => {
      if (event.type === 'experiment-snapshot' && Array.isArray(event.members) && Array.isArray(event.offsets)) {
        this.lastExperiment = event; this.receivedAt = Date.now(); this.emit('experiment', event);
      }
    });
  }
  connect(topic) {
    super.connect(topic);
    this.freshnessTimer = setInterval(() => {
      if (this.lastExperiment && Date.now() - this.receivedAt > 5000) {
        this.emit('experiment', { ...this.lastExperiment, at: Date.now(), error: 'STALE · no experiment update for more than 5 seconds' });
      }
    }, 1000);
  }
  disconnect() { super.disconnect(); clearInterval(this.freshnessTimer); this.lastExperiment = undefined; }
  async experiment(command) {
    const response = await fetch('/api/experiment', {
      method: command ? 'POST' : 'GET', headers: { 'Content-Type': 'application/json' },
      ...(command ? { body: JSON.stringify(command) } : {}), signal: AbortSignal.timeout(25000),
    });
    const result = await response.json();
    if (!response.ok) throw new Error(result.message || `Command failed (HTTP ${response.status}); inspect current state before retrying.`);
    return result;
  }
}
