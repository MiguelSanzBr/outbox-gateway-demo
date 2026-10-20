import react from '@vitejs/plugin-react'
import { defineConfig } from 'vitest/config'

// em dev o vite repassa /v1 pro gateway; no compose quem faz isso é o nginx
const proxy = { '/v1': 'http://localhost:8080' }

export default defineConfig({
  plugins: [react()],
  server: { port: 5180, strictPort: true, proxy },
  preview: { port: 5180, strictPort: true, proxy },
  test: {
    environment: 'jsdom',
    include: ['src/**/*.test.{ts,tsx}'],
    setupFiles: ['src/setup.ts'],
    coverage: {
      provider: 'v8',
      include: ['src/**'],
      // main.tsx só monta o App no #root; o App em si é testado inteiro
      exclude: ['src/main.tsx', 'src/setup.ts', 'src/**/*.test.{ts,tsx}'],
      thresholds: { statements: 100, branches: 100, functions: 100, lines: 100 },
    },
  },
})
