import { defineConfig, devices } from '@playwright/test'

// Roda contra a stack de verdade (docker compose); ver README.
export default defineConfig({
  testDir: 'e2e',
  timeout: 90_000,
  retries: 0,
  reporter: 'list',
  use: { baseURL: process.env.BASE_URL ?? 'http://localhost:5180', trace: 'retain-on-failure' },
  projects: [{ name: 'chromium', use: { ...devices['Desktop Chrome'] } }],
})
