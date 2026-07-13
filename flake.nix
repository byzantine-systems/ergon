{
  description = "Ergon's nix build and development shell";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs?ref=nixos-unstable";

    devenv = {
      url = "github:cachix/devenv";
      inputs.nixpkgs.follows = "nixpkgs";
    };

    flake-parts = {
      url = "github:hercules-ci/flake-parts";
    };

    treefmt-nix.url = "github:numtide/treefmt-nix";

    nix2container = {
      url = "github:nlewo/nix2container";
      inputs.nixpkgs.follows = "nixpkgs";
    };

    mk-shell-bin.url = "github:rrbutani/nix-mk-shell-bin";
  };

  outputs =
    inputs@{
      self,
      flake-parts,
      nixpkgs,
      ...
    }:
    flake-parts.lib.mkFlake { inherit inputs; } {
      imports = [
        inputs.devenv.flakeModule
        inputs.treefmt-nix.flakeModule
      ];
      systems = nixpkgs.lib.systems.flakeExposed;

      perSystem =
        {
          config,
          self',
          inputs',
          pkgs,
          system,
          ...
        }:
        let
          app_name = "ergon";
          app_version = "0.1.0";

          # nixpkgs' pg_cron (1.6.7) fails to compile against the PG19
          # server headers with -Wtypedef-redefinition. Upstream commit
          # c7609cce ("Support PostgreSQL 19") fixes it, pin to it until a
          # release ships with PG19 support.
          #
          # This must patch the *internal* extension set the derivation
          # closes over, not just the `.pkgs` passthru attribute: devenv's
          # postgres service assembles extensions via
          # `package.withPackages`, and wrapper.nix resolves those against
          # `finalPackage.pkgs`. A plain `postgresql_19 // { pkgs = ... }`
          # merge (the usual overlay shorthand) patches direct `.pkgs`
          # access but is bypassed by withPackages, so we overrideAttrs
          # the `passthru.pkgs` set, which the fixpoint does pick up.
          pgCronOverlay = final: prev: {
            postgresql_19 = prev.postgresql_19.overrideAttrs (old: {
              passthru = old.passthru // {
                pkgs = old.passthru.pkgs // {
                  pg_cron = old.passthru.pkgs.pg_cron.overrideAttrs (_: {
                    version = "1.6.7-unstable-2026-06-18";
                    src = final.fetchFromGitHub {
                      owner = "citusdata";
                      repo = "pg_cron";
                      rev = "c7609cce5c9f5fd8bcbab536cef08803a38bf6c1";
                      hash = "sha256-LJmYBNFSSuyarZGC4noGzMp13lXcGA6Z2CdmQomWtkA=";
                    };
                  });
                };
              };
            });
          };
          pkgs' = pkgs.extend pgCronOverlay;
        in
        {
          packages = {
            default = pkgs.beamPackages.mixRelease {
              pname = app_name;
              version = app_version;
              src = pkgs.lib.cleanSource ./.;
              mixFodDeps = pkgs.beamPackages.fetchMixDeps {
                pname = "mix-deps-${app_name}";
                src = pkgs.lib.cleanSource ./.;
                version = app_version;
                hash = "sha256-iZK2VqFYOOQqnC1cNdMf6YZMKnIAktuczXyuhGpfmBQ=";
                mixEnv = "prod";
              };
            };
          };

          # nix fmt + nix flake check (auto-wired by flakeModule)
          treefmt = {
            projectRootFile = "flake.nix";
            programs = {
              nixfmt.enable = true;
              sqruff.enable = true;
            };
          };

          devenv.shells.default = {
            # https://devenv.sh/reference/options/
            # builtins.getEnv works in --no-pure-eval (direnv), falls back to
            # the store path for pure-eval (nix flake check) to satisfy the
            # devenv assertion without attempting any filesystem writes.
            devenv.root =
              let
                r = builtins.getEnv "PWD";
              in
              if r != "" then r else builtins.toString ./.;

            packages =
              with pkgs;
              [
                gnumake
                liburing
              ]
              ++ [ config.packages.default ];

            languages.elixir = {
              enable = true;
              lsp.enable = true;
            };

            services.postgres = {
              enable = true;
              package = pkgs'.postgresql_19;
              extensions = ext: [
                ext.pg_cron
                ext.pgmq
                ext.postgis
              ];
              initdbArgs = [
                "--locale=C"
                "--encoding=UTF8"
              ];
              initialDatabases = [
                {
                  name = app_name;
                  user = app_name;
                  pass = app_name;
                }
              ];
              port = 5432;
              listen_addresses = "127.0.0.1";
              initialScript = ''
                CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
                -- The app user
                ALTER USER ${app_name} SUPERUSER CREATEROLE;
              '';

              settings = {
                shared_preload_libraries = pkgs.lib.concatStringsSep "," [
                  "auto_explain"
                  "pg_cron"
                  "pg_stat_statements"
                ];
                session_preload_libraries = "auto_explain";
                # The segregated pools total ~90 connections in dev.
                # - Ingest 20 + Process 50 + Query 20
                # the default of 100 leaves no headroom for
                # psql/tests/pg_cron. Raise it.
                max_connections = 300;
                # pg_cron's background worker only runs in a single database.
                # Point it at the app's dev DB so `CREATE EXTENSION pg_cron`
                # and cron.schedule() operate on app tables (default is
                # "postgres"). The value must match the dev DB name (`ergon`,
                # per runtime.exs / .env), `current_database() =
                # current_setting('cron.database_name', true)` is the guard
                # `Ergon.Migration.extensions/0` uses to skip pg_cron creation
                # in other databases (e.g. the test DB).
                "cron.database_name" = "${app_name}";
                "auto_explain.log_min_duration" = 150;
                "auto_explain.log_analyze" = true;
                log_min_duration_statement = 0;
                log_statement = "all";
                log_directory = "log";
                log_filename = "postgresql-%Y-%m-%d.log";
                # pg_stat_statements config, nested attr sets need to be
                # converted to strings, otherwise postgresql.conf fails
                # to be generated.
                compute_query_id = "on";
                "pg_stat_statements.max" = 10000;
                "pg_stat_statements.track" = "all";
              }
              // pkgs.lib.optionalAttrs pkgs.stdenv.isLinux {
                # Async IO, io_uring or workers
                # For io_uring method (Linux only, requires liburing)
                io_method = "io_uring";
              }
              // pkgs.lib.optionalAttrs pkgs.stdenv.isDarwin {
                # in case "io_uring" is not available
                io_method = "worker";
                # For systems with many CPU cores and high I/O latency
                io_workers = 8;
                # For smaller systems or fast local storage
                # io_workers = 2;
              };

            };

            enterShell = ''
              echo "Starting Development Environment..."
            '';
          };
        };
    };
}
