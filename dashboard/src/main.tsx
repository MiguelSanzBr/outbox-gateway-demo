import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import { httpApi } from './adapters/http'
import { sseEvents } from './adapters/sse'
import { App } from './adapters/ui/App'

// Raiz de composição: liga as portas aos adapters de verdade.
createRoot(document.getElementById('root') as HTMLElement).render(
  <StrictMode>
    <QueryClientProvider client={new QueryClient()}>
      <App api={httpApi} events={sseEvents} />
    </QueryClientProvider>
  </StrictMode>,
)
