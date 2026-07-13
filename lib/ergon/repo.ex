defmodule Ergon.Repo do
  @moduledoc """
  Ergon's Ecto repository.

  Ergon deliberately keeps its use of Ecto thin: schema is created with
  migrations, and every query is raw SQL loaded from `priv/queries/` and run
  through `Ergon.SQL`. The Repo exists so that Ecto's migrator, connection
  pool, and `Ecto.Adapters.SQL.query/4` are available.
  """
  use Ecto.Repo,
    otp_app: :ergon,
    adapter: Ecto.Adapters.Postgres
end
