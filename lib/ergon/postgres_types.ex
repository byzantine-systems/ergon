# Custom Postgrex type module for `Ergon.Repo`.
#
# `Postgrex.Types.define/3` generates the `Ergon.PostgresTypes` module at
# compile time. Our `tstzrange` decoder is listed first so it wins over the
# generic range handler in `Ecto.Adapters.Postgres.extensions/0`. Everything
# else falls through to Ecto's defaults. Applied to the repo via the `:types`
# key in config.
Postgrex.Types.define(
  Ergon.PostgresTypes,
  [Ergon.Postgrex.Extensions.TstzRange] ++ Ecto.Adapters.Postgres.extensions(),
  json: Jason
)
