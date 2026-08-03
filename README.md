![](observer.png)
# Fedimint Observer
Fedimint Observer is intended to become the "mempool.space for Fedimint". Due to the privacy properties of Fedimint it
won't be able to show concrete transaction flows, but transfers in and out of single federations are visible. By making
clear to users what is and isn't visible I hope to make Fedimint more transparent and thus more trustworthy.
Furthermore, I hope that it can inform developer decisions around improving privacy by having access to easily queryable
federation data to quantify possible privacy improvements.

## Architecture

Fedimint Observer is modular (see issue [#8](https://github.com/fedimint/fedimint-observer/issues/8)): a
module-agnostic core handles fetching and structural bookkeeping, while one observer module per fedimint
module kind interprets that module's data.

* **`fmo_core`** — everything module-agnostic:
  * *Fetch layer*: downloads raw session data per federation and stores it append-only in the `sessions`
    table together with structural facts (transactions, inputs/outputs/consensus items and their module
    kind). No module-specific decoding happens here, so fetching works even for module kinds no installed
    module understands.
  * *Dispatch/replay engine*: decodes sessions and hands each item to the observer module of its kind.
    Every module has its own processing cursor per federation (`module_progress`), advanced atomically with
    the module's writes — a module added (or version-bumped) later simply replays the whole history from
    the raw sessions, no re-fetching needed.
  * *Core services*: block time sync, guardian health monitoring, nostr federation/vote sync, meta
    fetching, and the HTTP API framework.
* **`fmo_modules/fmo_module_{mint,mintv2,wallet,walletv2,ln,lnv2}`** — one crate per fedimint module kind. Each implements
  the `ObserverModule` trait: it provides its decoder, owns a Postgres schema (`fmo_<kind>`) with its own
  migration lineage, normalizes inputs/outputs/consensus items into it, and can register per-federation
  background tasks (e.g. LN gateway polling) and API routes under
  `/federations/:federation_id/modules/<kind>`.
* **`fmo_server`** — a thin binary assembling core + the standard modules:

```rust
FedimintObserverBuilder::new()
    .with_module(fmo_module_mint::MintObserver)
    .with_module(fmo_module_wallet::WalletObserver)
    .with_module(fmo_module_ln::LnObserver)
    .with_module(fmo_module_lnv2::LnV2Observer)
    .run(opts)
    .await
```

Module authors can build their own FMO flavor without forking: depend on `fmo_core`, implement
`ObserverModule` for your module kind and write a ~20 line `main.rs`
(see [`fmo_server/examples/custom_fmo.rs`](fmo_server/examples/custom_fmo.rs)). Your module's schema is
created on first start and the full session history is replayed through it automatically.

## APIs

### Federation Observer
The server scans a list of federations for their publicly available data (session log, announcements, …)
and exposes it under the `/federations` path; the route table lives in
[`fmo_core/src/api/federations.rs`](fmo_core/src/api/federations.rs). One example is the
[`/federations`](https://observer.fedimint.org/api/federations) endpoint itself that returns a list of all
federations that are being observed. Module-provided endpoints live under
`/federations/:federation_id/modules/<kind>/…`, with aliases at historical paths (e.g. `/utxos`,
`/nonces/spend`, `/gateways`) for existing consumers.

This API is also the data source for the frontend that powers https://observer.fedimint.org. The frontend
is hosted in the `fmo_frontend_react` directory and is built with React and TypeScript.

The API under `/federations` isn't stable at this point and I'd recommend subscribing to changes in
Fedimint Observer if building against it.

### Federation Inspector
The lesser-known component is an API under the `/config` path it can be used to get a JSON-encoded version of the
federation config if you have an invite code. The first time it fetches the config from the federation using the invite
code, after that it will return a version cached in memory (till the service is restarted). The endpoints can be found
in [`fmo_core/src/api/config.rs`](fmo_core/src/api/config.rs).

This service is already used by [bitcoinmints.com](https://bitcoinmints.com/?tab=mints&showFedimint=true) and can thus
be considered kinda stable.

## Importing data from a pre-modularization instance

The modular version starts a new database schema lineage; there is no in-place migration from the old
(v0.1, schema v8) database. Instead, raw data is imported and everything derived is rebuilt:

```bash
# 1. create a fresh database and import configs, raw sessions and block times
fmo_server import --from "postgresql:///fmo_old?user=fmo" --database "postgresql:///fmo?user=fmo"
# 2. start the server: structural data is already in place, modules replay
#    the full session history to rebuild their normalized tables
fmo_server serve
```

The import verifies per-federation session counts and works for federations that no longer exist, since no
network access is needed. The import is resumable (re-run it after an interruption) and tolerates a server
that is already fetching newer sessions concurrently. Keep the old database around as a backup until the
new instance is verified. If the server was already running during the import, restart it afterwards: a
running server only picks up federations at startup or when they are added via the admin API.

For the initial import + replay of a large history, two settings make a big difference (the bottleneck is
WAL fsync volume, not CPU):

```sql
-- scratch database that can be rebuilt from raw data at any time; re-enable after catch-up
ALTER DATABASE fmo SET synchronous_commit = 'off';
```

```bash
# don't refresh the (expensive) materialized views every minute while replaying history
FO_REFRESH_INTERVAL_SECS=900 fmo_server serve
```

## Development
Fedimint Observer comes with a [nix](https://nixos.org/) development environment. You can enter it by running `nix develop`.
In there you can run a variety of `just` commands (to be called as `just <COMMAND> <ARGS...>`), the most important ones are:
* `check`: run `cargo check` on the entire workspace
* `pg_start` and `pg_stop`: start/stop a postgresql instance for local testing in the background
* `pg_backup` and `pg_restore`: in case you are building a DB migration it's useful to be able to reset the DB

## Deployment

I currently run the public instance at https://observer.fedimint.org using the following nix config:

```nix
{ lib, pkgs, fedimint-observer, system, ... }: let
   fmo = fedimint-observer.packages."${system}";
 in {
  systemd.services.fedimint-observer = {
    enable = true;
    wantedBy = [ "multi-user.target" ];
    environment = {
      FO_BIND = "127.0.0.1:5000";
      FO_DATABASE = "postgresql:///fmo?user=fmo";
      # Set to your admin password, used to add federations to be observed via curl
      FO_ADMIN_AUTH = ;
      ALLOW_CONFIG_CORS = "true";
    };
    serviceConfig = {
      ExecStart = ''
        ${fmo.fmo_server}/bin/fmo_server serve
      '';
      User = "fmo";
      Group = "fmo";
      Restart = "always";
      RestartSec = "10s";
    };
  };

  services.postgresql = {
    enable = true;
    ensureDatabases = [ "fmo" ];
    ensureUsers = [
      { name = "fmo"; }
    ];
    initialScript = pkgs.writeText "backend-initScript" ''
      GRANT ALL PRIVILEGES ON DATABASE fmo TO fmo;
      \c fmo
      GRANT ALL ON SCHEMA public TO fmo;
    '';
  };

  services.nginx = {
    enable = true;
    virtualHosts."observer.fedimint.org" = {
      enableACME = true;
      forceSSL = true;
      root = fmo.fmo_frontend_react_default;
      locations."/" = {
        extraConfig = ''
          try_files $uri $uri/ /index.html;
        '';
      };
      locations."/api/" = {
        proxyPass = "http://127.0.0.1:5000/";
      };
    };
  };

  users.users."fmo" = {
    isSystemUser = true;
    group = "fmo";
  };
  users.groups."fmo" = {};
}
```
