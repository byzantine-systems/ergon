# Integration tests (tagged :integration) need a live PostgreSQL 18/19 with the
#      migrations applied, the `test` mix alias creates and migrates the database
#      first. They are included by default, run only the pure suite with
# `mix test --exclude integration`.
#
# `:cron` tests mutate `cron.job` in the *dev* database (where pg_cron is
# actually installed, the test DB lacks it by design, per Phase 1's
#      extensions/0 guard). Excluded by default, run explicitly with
# `mix test --include cron`.
ExUnit.start(exclude: [:cron])

Ecto.Adapters.SQL.Sandbox.mode(Ergon.Repo, :manual)
