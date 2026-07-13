import Config

# The one Repo Ergon owns. Host applications point it at their own database
# through the runtime configuration below.
config :ergon, ecto_repos: [Ergon.Repo]

config :ergon, Ergon.Repo,
  # All of Ergon's objects live in a dedicated `ergon` schema, but the migration
  # bookkeeping table stays in `public` so it never collides with them.
  migration_source: "ergon_schema_migrations",
  # Pin the connection's search_path to `public` only. Without this the `ergon`
  # user's default search_path (`"$user", public` = `ergon, public`) would
  # shadow `public.ergon_schema_migrations` with an empty
  # `ergon.ergon_schema_migrations` once migration #1 creates the `ergon`
  # schema, ecto would then re-apply already-applied migrations and crash on
  # "relation already exists". Ergon's own SQL files are fully schema-qualified
  # (`ergon.jobs`, etc.) so they're unaffected by this constraint.
  parameters: [search_path: "public"],
  # Decodes `tstzrange` columns (temporal validity/system periods) into
  # \`%Ergon.TemporalPeriod{}\` structs. See
  # `Ergon.PostgresTypes` for the type module wiring.
  types: Ergon.PostgresTypes

import_config "#{config_env()}.exs"
