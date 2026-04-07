# Chicago Crime AI Studio Frontend

Frontend app for the Chicago Crime AI Studio, built with React + Vite.

## UI theme notes

The Chicago frontend now uses a dedicated **warm civic analytics** theme that is intentionally different from the NYC GBFS app.

Primary token source:
- `src/index.css`

Key surfaces updated with this theme:
- top navigation (`src/components/TopNav.tsx`)
- dashboard shell (`src/pages/Index.tsx`)
- map actions and panels (`src/pages/MapPage.tsx`, `src/components/D3LiveMap.tsx`)
- limitations modal (`src/components/LimitationsModal.tsx`)

### Core token groups

- base colors: `--background`, `--foreground`, `--card`, `--border`
- accents: `--primary`, `--accent`, `--ring`
- semantic/risk colors: `--safety-high`, `--safety-mid`, `--safety-low`, `--destructive`
- glass surfaces: `--glass-bg`, `--glass-border`, `--glass-blur`
- gradients: `--gradient-start`, `--gradient-mid`, `--gradient-end`

When adjusting the Chicago look, prefer changing tokens in `src/index.css` first so pages stay consistent.
