import js from '@eslint/js'
import reactHooks from 'eslint-plugin-react-hooks'
import globals from 'globals'
import tseslint from 'typescript-eslint'

// Regra de dependência: domain não conhece ninguém, application só conhece domain
// e as próprias portas; React, fetch e EventSource ficam nos adapters.
const only = (regex, message) => ({ 'no-restricted-imports': ['error', { patterns: [{ regex, message }] }] })

export default tseslint.config(
  { ignores: ['dist', 'coverage', 'playwright-report', 'test-results'] },
  js.configs.recommended,
  tseslint.configs.strict,
  reactHooks.configs.flat['recommended-latest'],
  { languageOptions: { globals: { ...globals.browser, ...globals.node } } },
  {
    files: ['src/domain/**'],
    rules: only('(^|/)(application|adapters)(/|$)|^react|^@tanstack/', 'domain não depende de application, adapters nem React'),
  },
  {
    files: ['src/application/**'],
    rules: only('(^|/)adapters(/|$)|^react|^@tanstack/', 'application não depende de adapters nem React'),
  },
)
