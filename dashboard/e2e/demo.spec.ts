import { expect, test } from '@playwright/test'

// Contra a stack de verdade (gateway + postgres + mock), subida com
// DEMO_ADMIN=1 WORKERS=2 LEASE=6s MOCK_LATENCY=1s MOCK_JITTER=0s MOCK_FAIL_RATE=0:
// com 2 workers e 1 s por envio sempre tem worker no meio de um envio pra derrubar.
test('mensagem enfileirada chega em sent e worker derrubado no meio do envio volta pelo reaper', async ({ page }) => {
  await page.goto('/')
  await expect(page.getByRole('status')).toHaveText('ao vivo')

  await page.getByRole('button', { name: 'Enfileirar 20 mensagens' }).click()
  const log = page.getByRole('log')
  await expect(log).toContainText('20 mensagens enfileiradas', { timeout: 20_000 })

  await expect(page.getByRole('listitem', { name: /^worker 0: enviando #\d+/ })).toBeVisible({ timeout: 20_000 })
  await page.getByRole('button', { name: 'Derrubar worker 0' }).click()
  await expect(log).toContainText('worker 0 derrubado')
  await expect(page.getByRole('listitem', { name: 'worker 0: caído' })).toBeVisible()

  // a linha fica em sending até o lease vencer; o reaper devolve e o worker 1 entrega
  const back = log.getByText(/#\d+ devolvida pelo reaper/)
  await expect(back).toBeVisible({ timeout: 30_000 })
  const id = /#(\d+)/.exec((await back.textContent()) ?? '')?.[1]
  expect(id).toBeDefined()
  await expect(page.getByRole('listitem', { name: new RegExp(`^mensagem ${id}: sent`) })).toBeVisible({
    timeout: 30_000,
  })

  await page.getByRole('button', { name: 'Religar workers' }).click()
  await expect(page.getByRole('listitem', { name: 'worker 0: caído' })).toHaveCount(0)
})
