defmodule Ergon.PartitionBootCheck do
  @moduledoc """
  Boot-time partition fail-safe.

  The weekly `partition-lifecycle-<table>` pg_cron job scheduled by
  `Ergon.Migration.partitioned_table/2` is the primary creator of future
  monthly partitions, but a silently dead cron daemon, a clock-skewed
  failover, or a fresh restore of an old backup would all leave ingestion
  facing a table that can't accept inserts. This process closes that gap: on
  every boot it verifies the next `months_ahead` months of partitions exist
  and, if any are missing, runs `auto_manage_partitions_<table>()` directly.

  The check runs in `init/1`, deliberately blocking the host's supervisor
  start-up: children later in the tree (the ingest pipeline, the endpoint)
  must not come up against a table that cannot accept inserts. If partitions
  are still missing after remediation the boot fails loudly.

  ## Options

    * `:table`, the partitioned table name (required)
    * `:months_ahead`, how many months to verify ahead (default 2)
    * `:enabled`, whether to run the check at all (default from
      `config :ergon, Ergon.PartitionBootCheck, enabled: <bool>`, which is
      itself `true` by default). Tests disable it via config and start a
      supervised instance with `enabled: true`.
    * `:repo`, Ecto repo for partition queries (defaults to `Ergon.Repo`)
    * `:manage_fn`, the plpgsql function name that creates missing partitions
      (defaults to `"auto_manage_partitions_<table>"`). Override for legacy
      installs whose migrations pre-date `Ergon.Migration.partitioned_table/2`.

  Start it under the host app's supervisor:

      children = [
        {Ergon.PartitionBootCheck, table: :asset_telemetry_pings}
      ]
  """

  use GenServer

  require Logger

  alias Ergon.{Repo, SQL}

  @default_months_ahead 2

  def start_link(opts) do
    {name, opts} = Keyword.pop(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @impl true
  def init(opts) do
    table = Keyword.fetch!(opts, :table)
    months_ahead = Keyword.get(opts, :months_ahead, @default_months_ahead)
    repo = Keyword.get(opts, :repo, Repo)

    manage_fn =
      Keyword.get(opts, :manage_fn, "auto_manage_partitions_#{validate_table_name!(table)}")

    if Keyword.get(opts, :enabled, enabled?()) do
      ensure_partitions!(table, months_ahead, repo: repo, manage_fn: manage_fn)
    end

    {:ok, %{table: table, months_ahead: months_ahead, repo: repo, manage_fn: manage_fn}, :hibernate}
  end

  @doc """
  Verifies partitions exist for the current month through `months_ahead`
  months out, creating any that are missing. Raises if partitions are still
  missing after remediation.

  Options:
    * `:repo`, Ecto repo (defaults to `Ergon.Repo`)
    * `:manage_fn`, function name for partition creation (defaults to
      `"auto_manage_partitions_<table>"`)
  """
  @spec ensure_partitions!(atom() | String.t(), non_neg_integer(), keyword()) :: :ok
  def ensure_partitions!(table, months_ahead \\ @default_months_ahead, opts \\ []) do
    name = validate_table_name!(table)
    repo = Keyword.get(opts, :repo, Repo)
    manage_fn = Keyword.get(opts, :manage_fn, "auto_manage_partitions_#{name}")

    case missing_partitions(name, months_ahead, opts) do
      [] ->
        :ok

      missing ->
        Logger.warning(
          "PartitionBootCheck: #{name} partitions missing for #{inspect(missing)}. " <>
            "Running #{manage_fn}(#{months_ahead})"
        )

        run_manage!(repo, manage_fn, months_ahead)
        Logger.info("PartitionBootCheck: remediation complete for #{name}")

        case missing_partitions(name, months_ahead, opts) do
          [] ->
            :ok

          still_missing ->
            raise "PartitionBootCheck: #{name} partitions still missing after remediation: " <>
                    inspect(still_missing)
        end
    end
  end

  @doc "YYYYMM labels of months in the horizon lacking a partition."
  @spec missing_partitions(atom() | String.t(), non_neg_integer(), keyword()) :: [String.t()]
  def missing_partitions(table, months_ahead \\ @default_months_ahead, opts \\ []) do
    name = validate_table_name!(table)

    {:ok, %Postgrex.Result{rows: rows}} =
      SQL.query({:partitions, :missing}, [months_ahead, name], opts)

    List.flatten(rows)
  end

  defp run_manage!(repo, manage_fn, months_ahead) do
    # `manage_fn` must match `[a-z_][a-z0-9_]*` (validated via
    # validate_table_name! for the default, or caller-supplied verbatim).
    repo.query!("SELECT #{manage_fn}($1)", [months_ahead])
  end

  defp enabled? do
    :ergon
    |> Application.get_env(__MODULE__, [])
    |> Keyword.get(:enabled, true)
  end

  # Postgres unquoted identifiers must match [a-z_][a-z0-9_]*. Validating
  # here means it's safe to interpolate the name into the SQL in run_manage!/2
  # (parameterised queries can't address a function by name).
  defp validate_table_name!(table) when is_atom(table),
    do: validate_table_name!(Atom.to_string(table))

  defp validate_table_name!(table) when is_binary(table) do
    unless Regex.match?(~r/^[a-z_][a-z0-9_]*$/, table) do
      raise ArgumentError, "invalid partitioned table name: #{inspect(table)}"
    end

    table
  end
end
