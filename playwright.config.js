import { defineConfig } from '@playwright/test';
export default defineConfig({
  testDir: './tests/browser', workers: 1, timeout: 60000,
  use: { baseURL: process.env.DEMO_URL || 'http://localhost:8080', viewport: { width: 1440, height: 960 } },
});
