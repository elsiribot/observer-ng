# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Fedimint Observer is a Rust-based monitoring platform for Fedimint federations, aiming to be "the mempool.space for Fedimint". It provides transparency into federation operations while respecting privacy constraints.

## Development Commands

### Essential Commands
```bash
# Enter development environment (requires Nix)
nix develop

# Start local PostgreSQL for development
just pg_start

# Run backend server with auto-reload
just watch

# Check code compilation and common issues (preferred during development)
just clippy

# Run all tests
just test

# Format code
just format

# Run all checks before PR
just final-check
```

### Build Commands
```bash
just build              # Build everything
just build_package fmo_server
```

### Code Quality Commands
```bash
just clippy            # Run clippy linter (preferred for quick compilation checks)
just clippy-fix        # Run clippy with auto-fix
just check             # Run cargo check
just lint              # Run pre-commit linting
just typos             # Check for typos
just typos-fix-all     # Fix all typos
```

### Database Commands
```bash
just pg_start          # Start local PostgreSQL
just pg_stop           # Stop PostgreSQL
just pg_backup         # Backup database
just pg_restore BACKUP_FILE
```

### Testing Specific Components
```bash
just test_package fmo_server
```

## Architecture Overview

### Workspace Structure
- `fmo_api_types/` - Shared API types between frontend and backend
- `fmo_core/` - Module-agnostic observer core (library)
  - `fetch.rs` - downloads raw sessions, stores them + structural facts (no module decoding)
  - `ingest.rs` - structural ingest shared by fetcher and import tool
  - `dispatch.rs` - decodes sessions and dispatches items to observer modules; per-module cursors make live processing and historical replay the same code path
  - `gold.rs` - gold layer: incremental per-federation processor that folds silver (core structural tables + module contract tables) into deduplicated `user_transactions`; cursor (`gold_progress`) trails `min(module cursors)`
  - `module.rs` - the `ObserverModule` trait (decoder, own migrations, process_input/output/ci, background tasks, API router)
  - `import.rs` - one-shot import from a pre-modularization (schema v8) database
  - `services/` - block times, guardian health, nostr sync, meta caches
  - `api/` - axum routers: core endpoints + module router mounting + `/config/*`
  - `schema/core/v0.sql` - core schema (new lineage, append-only migrations)
- `fmo_modules/fmo_module_{mint,mintv2,wallet,walletv2,ln,lnv2}/` - one crate per fedimint module kind, each owning Postgres schema `fmo_<kind>` with its own migration lineage
- `fmo_server/` - thin binary: `FedimintObserverBuilder` + standard modules; `serve` and `import` subcommands; `examples/custom_fmo.rs` shows a custom build
- `fmo_frontend_react/` - Frontend (React + TypeScript)

### Key Patterns
1. **Shared Types**: All API types are defined in `fmo_api_types` and used by both frontend and backend
2. **Three-layer data flow** (issue #8): fetch raw sessions (bronze) → modules normalize into their own schemas (silver, Rust-side and in-transaction) → cross-module gold layer dedupes into directly-queryable user-transaction tables (not yet exposed via an API route)
3. **Per-module cursors**: `module_progress` tracks each module's processing position per federation; adding a module later or bumping its `version()` triggers schema drop + full replay from raw sessions — no refetching
4. **Idempotent processing**: all inserts use `ON CONFLICT DO NOTHING` so crash-resume and replay are safe
5. **Graceful unknown data**: unknown module kinds/versions are stored raw/JSON, never panic; a failing module only stalls itself
6. **Module API routes**: mounted at `/federations/:federation_id/modules/<kind>/…` with compat aliases at historical paths (`/utxos`, `/nonces/spend`, `/gateways`)
7. **Error Handling**: `AppError` type wrapping `anyhow::Error` in `fmo_core::error`
8. **Gold layer**: `fmo_core/src/gold.rs` folds silver (core structural tables + module contract tables) into `user_transactions`, a pure function of its inputs; the `gold_progress` cursor trails `min(module cursors)` so it only processes ranges every module has already finished. Dedup key is `contract_id` for LN (folds offer/fund/claim/cancel/refund into one row), `txid` otherwise.

### Environment Configuration
Required environment variables (see `sample.env`):
- `FO_BIND`: Server bind address (e.g., "127.0.0.1:3000")
- `FO_DATABASE`: PostgreSQL connection string
- `FO_ADMIN_AUTH`: Admin authentication password
- `FO_MEMPOOL_URL`: Mempool API URL (default: "https://mempool.space/api")
- `ALLOW_CONFIG_CORS`: Enable CORS for config endpoints
- `FO_REFRESH_INTERVAL_SECS`: Materialized view refresh interval (default 60)
- `FO_GATEWAY_POLL_SECS`: LN gateway poll interval (default 300)
- `FO_DB_POOL_SIZE`: DB connection pool size (default 32; raise when observing many federations, keep below postgres max_connections)

### API Endpoints
- **Config API** (`/config/*`): Stable API for federation configuration inspection
- **Federations API** (`/federations/*`): Unstable API for federation monitoring data
- **Module APIs** (`/federations/:id/modules/<kind>/*`): Module-provided endpoints
- **Admin endpoints**: Require bearer token authentication via `FO_ADMIN_AUTH`

### Database Schema
PostgreSQL; core schema in `public`, one schema per observer module (`fmo_mint`, `fmo_mintv2`, `fmo_wallet`, `fmo_walletv2`, `fmo_ln`, `fmo_lnv2`). Key core tables:
- `federations` - Federation configurations
- `sessions` - Raw consensus sessions (bronze layer, append-only)
- `transactions`, `transaction_inputs/outputs`, `consensus_items` - structural facts; `amount_msat`/`details` filled by module dispatch
- `module_progress`, `module_versions` - per-module replay cursors and versions
- `session_time_votes` + `session_times` matview - session timestamps contributed by modules (wallet block votes, lnv2 time votes)
- `guardian_health`, `nostr_*`, `block_times` - core services
- Gold layer (cross-module denormalization, see `gold.rs` above): `user_transactions` - one row per deduplicated user transaction (grain: `contract_id` for LN, `txid` otherwise), with exact `fedimint_fee_msat` and, for outgoing LN sends only, an estimated `gateway_fee_estimate_msat`; `user_transaction_txs` - membership/drill-down from a user transaction back to its underlying fedimint tx(s) and their role (offer/fund/claim/cancel/refund/self); `gold_progress` - per-federation replay cursor; `user_tx_daily` matview - rollup by federation/day/kind/direction/status, refreshed on the same cycle as `session_times`

Old (pre-modularization, schema v8) databases are NOT migrated in place; use `fmo_server import --from <old-db-url>` to copy raw data into a fresh database and let module replay rebuild the rest.
