import { defineConfig } from '@playwright/test';

export default defineConfig({
  testDir: 'tests/e2e',
  timeout: 30000,
  use: {
    baseURL: 'http://localhost:7272',
    headless: true,
  },
  webServer: {
    command: 'python app.py',
    port: 7272,
    reuseExistingServer: true,
  },
});
