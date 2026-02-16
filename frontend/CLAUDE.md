# Frontend — CLAUDE.md

## Stack

React 18 + Vite + Tailwind CSS + Radix UI + TanStack React Query + React Router DOM.

## Commands

```bash
npm run dev          # dev server
npm run build        # production build
npm run test:run     # run tests once (vitest)
npm run test         # watch mode
npm run lint         # eslint
```

## Structure

```
src/
├── api/           # API client functions (fetch wrappers)
├── components/    # Reusable UI components (shadcn/ui pattern with Radix primitives)
├── pages/         # Route-level page components
├── lib/           # Utilities (cn helper for Tailwind merging)
├── test/          # Test files
├── App.jsx        # Router + QueryClientProvider setup
└── main.jsx       # Entry point
```

## Conventions

- Components use Radix UI primitives with Tailwind styling (shadcn/ui pattern).
- Class merging via `cn()` from `lib/utils` (clsx + tailwind-merge).
- Data fetching with `@tanstack/react-query` — use `useQuery`/`useMutation` hooks.
- Icons from `lucide-react`.
- Animations with `framer-motion`.
- Tests use Vitest + React Testing Library + jsdom.
- No TypeScript — plain JSX.
