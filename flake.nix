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
          appSrc = builtins.readFile ./src/ergon.app.src;
          stripped = pkgs.lib.replaceStrings [ " " "\t" "\n" "\r" ] [ "" "" "" "" ] appSrc;
          # ergon.app.src declares `{vsn, "x.y.z"}`; once whitespace is stripped
          # that reads `{vsn,"x.y.z"}`. builtins.match anchors on the whole
          # string, so the surrounding `.*` swallow everything else and the sole
          # capture group is the version. The braces go in bracket expressions
          # because POSIX ERE, which builtins.match uses, gives `{` its own
          # meaning as an interval quantifier and rejects `\{` as an escape.
          m = builtins.match ''.*[{]vsn,"([^"]+)"[}].*'' stripped;
          app_name = "ergon";
          app_version = builtins.elemAt m 0;

          # `lib.cleanSource` strips VCS noise but keeps everything else, and
          # devenv's postgres leaves a unix socket at
          # .devenv/state/postgres/.s.PGSQL.5432. Nix cannot copy a socket into
          # the store, so evaluation fails outright with "has an unsupported
          # type" whenever the database is running, which is to say on every
          # development machine. Filter those trees out explicitly.
          src = pkgs.lib.cleanSourceWith {
            src = pkgs.lib.cleanSource ./.;
            name = "${app_name}-source";
            filter =
              path: _type:
              let
                base = baseNameOf path;
              in
              !(builtins.elem base [
                ".devenv"
                ".direnv"
                "_build"
                ".git"
              ]);
          };

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
            # Ergon is a library, so the artifact is a compiled OTP application
            # rather than a release: nothing consumes a self-contained node with
            # its own ERTS, and `nix build` earns its place as a compile gate.
            # deps.nix is generated from rebar.lock by the rebar3_nix plugin,
            # which is in the devShell:
            #
            #   rebar3 nix lock -o deps.nix
            #
            # Regenerate it after any dependency change. nixpkgs has no
            # single fixed-output fetcher for rebar3 the way it does for mix:
            # `fetchRebar3Deps` exists but nothing consumes it, and buildRebar3
            # takes only a `beamDeps` list. Generating that list beats writing
            # nine derivations by hand and keeping their hashes in step.
            # `name`, not `pname`: buildRebar3 derives pname from it and builds
            # its own store name as erlang<ver>-<name>-<version>.
            default = pkgs.beamPackages.buildRebar3 {
              name = app_name;
              version = app_version;
              inherit src;
              buildPlugins = [ pkgs.beamPackages.pc ];
              beamDeps = builtins.attrValues (
                import ./rebar-deps.nix {
                  inherit (pkgs.beamPackages) fetchHex;
                  inherit (pkgs) fetchgit fetchFromGitHub;
                  # Without a builder, deps.nix returns bare sources. Passing
                  # buildRebar3 is what turns each entry into a compiled OTP app.
                  #
                  # Wrapped rather than passed directly so every dependency gets
                  # the port compiler: `fs`, reached through migraterl, declares
                  # `pc` in its own rebar.config and fails to compile without it.
                  # Setting buildPlugins on the top-level app alone does not
                  # reach the dependency derivations.
                  builder =
                    args: pkgs.beamPackages.buildRebar3 (args // { buildPlugins = [ pkgs.beamPackages.pc ]; });
                }
              );
            };
          };

          # nix fmt + nix flake check (auto-wired by flakeModule)
          treefmt = {
            projectRootFile = "flake.nix";
            programs = {
              nixfmt.enable = true;
              erlfmt.enable = true;
            };

            # pgFormatter ships no treefmt-nix `programs.*` wrapper, so register
            # it as a custom formatter. pg_format formats many files in one call
            # as long as `--inplace` is set, which matches how treefmt invokes a
            # formatter (matched files appended as trailing arguments).
            # https://github.com/numtide/treefmt-nix#using-a-custom-formatter
            settings.formatter.pg_format = {
              command = "${pkgs.pgformatter}/bin/pg_format";
              options = [
                "--inplace"
                "-f"
                "2"
              ];
              includes = [ "*.sql" ];
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
                pgformatter
                rebar3
              ]
              ++ pkgs.lib.optionalAttrs pkgs.stdenv.isLinux [
                liburing
              ];

            # Pinned rather than left to devenv's default: the Erlang tree uses
            # OTP-28-only syntax (strict generators `<:-`, EEP-69 nominal types)
            # alongside OTP-27 sigils and `maybe`. An implicit bump would break
            # the build in ways that read as unrelated syntax errors.
            languages.erlang = {
              enable = true;
              package = pkgs.erlang_28;
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
                # the PGDATABASE default), `current_database() =
                # current_setting('cron.database_name', true)` is the guard
                # priv/migrations/bootstrap uses to skip pg_cron creation in
                # other databases (e.g. the test DB).
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
