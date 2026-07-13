defmodule Ergon.Health do
  @moduledoc """
  Liveness + diagnostics probe.

  `check/0` returns a flat map with everything a `/health` endpoint needs:

    * `:db`, `{:ok, _}` if the repo can serve a trivial query
    * `:extensions`, `%{name => version}` map of installed PG extensions
    * `:queues`, per-queue metrics for the configured queues

  Queue list comes from `config :ergon, queues: [...]` or the `:queues` opt
  to `check/1`. Hosts with no pgmq queues can leave it unset, `:queues`
  defaults to an empty map.

  ## Examples

      config :ergon, queues: ~w(telemetry_processing geofence_alerts)

      Ergon.Health.check()
      #=> %{
      #     db: {:ok, %Postgrex.Result{rows: [[1]]}},
      #     extensions: %{"pgmq" => "1.5.0", "btree_gist" => "1.5", ...},
      #     queues: %{
      #       "telemetry_processing" => %{queue_length: 5, queue_visible_length: 3, ...}
      #     }
      #   }
  """

  alias Ergon.{Pgmq, SQL}

  @type t :: %{
          db: {:ok, Postgrex.Result.t()} | {:error, Exception.t()},
          extensions: %{String.t() => String.t()},
          queues: %{String.t() => Pgmq.metrics()}
        }

  @doc """
  Returns the health snapshot. Pass `queues: [...]` to override the configured
  queue list for this call. Pass `repo: ...` to override the default
  (`Ergon.Repo`).
  """
  @spec check(keyword()) :: t()
  def check(opts \\ []) do
    configured = Application.get_env(:ergon, :queues, [])
    queues = Keyword.get(opts, :queues, configured)
    sql_opts = Keyword.take(opts, [:repo])
    pgmq_opts = sql_opts

    %{
      db: SQL.query({:system, :healthcheck}, [], sql_opts),
      extensions: installed_extensions(sql_opts),
      queues: Map.new(queues, &{to_string(&1), Pgmq.metrics(&1, pgmq_opts)})
    }
  end

  defp installed_extensions(opts) do
    {:ok, %Postgrex.Result{rows: rows}} = SQL.query({:system, :installed_extensions}, [], opts)

    Map.new(rows, fn [name, version] -> {name, version} end)
  end
end
