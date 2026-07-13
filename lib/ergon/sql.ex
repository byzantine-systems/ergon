defmodule Ergon.SQL do
  @moduledoc """
  Filesystem-backed SQL loader and executor, the boot-cached
  replacement for the per-call `File.read!` loader.

  ## Convention

  Statements live at `priv/queries/<domain>/<operation>.sql` and use PostgreSQL
  positional parameters (`$1..$n`). Each file is keyed by `{<domain>,
  <operation>}`, e.g. `priv/queries/jobs/insert.sql` → `{:jobs, :insert}`.

  This GenServer walks `priv/queries` once at boot and caches every statement
  in a read-optimised ETS table, so execution is a lock-free `:ets.lookup/2`
  plus a `Repo.query/3`. It is supervised ahead of any consumer that depends
  on it (`Ergon.WorkerSupervisor`, host pipelines).

      Ergon.SQL.query({:jobs, :insert}, [queue, worker, payload, ...])

  ## Host query directories

  Host applications can register additional `priv/queries`-style directories
  via application env, so a single loader serves both ergon's own statements
  and host-owned domains (assets, telemetry, spatial, …). The default is
  ergon's own `priv/queries` only:

      config :ergon, Ergon.SQL,
        extra_roots: [
          {:app, :my_app, "priv/queries"},         # resolved via Application.app_dir/2
          "/absolute/path/to/queries"               # used verbatim
        ]

  Keys from extra roots share the same `{domain, operation}` namespace;
  collisions across roots are rejected at load time just like in-ergon ones.
  """

  use GenServer

  require Logger

  @table __MODULE__
  @default_repo Ergon.Repo

  @type key :: {atom(), atom()}

  ## Public API

  def start_link(opts) do
    opts = Keyword.merge(Application.get_env(:ergon, __MODULE__, []), opts)
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Returns the raw SQL string for `key`, or `:error` if unknown."
  @spec fetch(key()) :: {:ok, String.t()} | :error
  def fetch(key) do
    case :ets.lookup(@table, key) do
      [{^key, sql}] -> {:ok, sql}
      [] -> :error
    end
  end

  @doc "Like `fetch/1` but raises for an unknown `key`."
  @spec fetch!(key()) :: String.t()
  def fetch!(key) do
    case fetch(key) do
      {:ok, sql} ->
        sql

      :error ->
        raise KeyError,
          key: key,
          term: __MODULE__,
          message:
            "no SQL registered for #{inspect(key)}. Known keys: #{inspect(keys())}. " <>
              "Expected a file at priv/queries/#{elem_or(key, 0)}/#{elem_or(key, 1)}.sql"
    end
  end

  @doc "All registered `{domain, operation}` keys."
  @spec keys() :: [key()]
  def keys do
    @table |> :ets.tab2list() |> Enum.map(&elem(&1, 0)) |> Enum.sort()
  end

  @doc """
  Executes the statement registered for `key` with positional `params`.

  Options:

    * `:repo`, the Ecto repo (pool) to run on. Defaults to
      `#{inspect(@default_repo)}`.

  Remaining options are forwarded to `Ecto.Repo.query/4`. Returns
  `{:ok, %Postgrex.Result{}}` or `{:error, exception}`.
  """
  @spec query(key(), [term()], keyword()) ::
          {:ok, Postgrex.Result.t()} | {:error, Exception.t()}
  def query(key, params \\ [], opts \\ []) do
    {repo, opts} = Keyword.pop(opts, :repo, @default_repo)
    repo.query(fetch!(key), params, opts)
  end

  @doc "Like `query/3` but raises on error."
  @spec query!(key(), [term()], keyword()) :: Postgrex.Result.t()
  def query!(key, params \\ [], opts \\ []) do
    {repo, opts} = Keyword.pop(opts, :repo, @default_repo)
    repo.query!(fetch!(key), params, opts)
  end

  @doc """
  Re-reads `priv/queries` from disk into the cache. Useful in dev/tests after
  editing statements. Returns the number of statements loaded.
  """
  @spec reload() :: non_neg_integer()
  def reload, do: GenServer.call(__MODULE__, :reload)

  ## GenServer

  @impl true
  def init(opts) do
    :ets.new(@table, [:named_table, :set, :protected, read_concurrency: true])
    count = load(opts)
    Logger.info("Ergon.SQL loaded #{count} statement(s) from #{inspect(roots(opts))}")
    {:ok, opts}
  end

  @impl true
  def handle_call(:reload, _from, opts) do
    {:reply, load(opts), opts}
  end

  ## Loading

  # Upserts atomically instead of clear-then-rebuild: `:ets.insert/2` is
  # guaranteed atomic even for a whole list of objects, so any key that exists
  # both before and after a reload is never observably absent in between. That
  # matters because `@table` is a single named table shared by every test
  # process, a prior `:ets.delete_all_objects/1` followed by a separate
  # `:ets.insert/2` left a real window where a concurrent `fetch!/1` (in
  # another async test) would see an empty cache and raise. Only keys whose
  # backing file has disappeared since the last load (the dev-edit case
  # `reload/0` exists for) are explicitly deleted, after the new data is in.
  defp load(opts) do
    roots = roots(opts)

    entries =
      Enum.flat_map(roots, fn root ->
        files = sql_files(root)

        Enum.map(files, fn path ->
          {key_for(path, root), File.read!(path)}
        end)
      end)

    detect_collisions!(entries)

    new_keys = MapSet.new(entries, &elem(&1, 0))

    stale_keys =
      @table
      |> :ets.tab2list()
      |> Enum.map(&elem(&1, 0))
      |> Enum.reject(&MapSet.member?(new_keys, &1))

    :ets.insert(@table, entries)
    Enum.each(stale_keys, &:ets.delete(@table, &1))

    length(entries)
  end

  # The list of roots this loader instance scans. Defaults to ergon's own
  # `priv/queries`. Hosts extend it via the `:extra_roots` opt (or app env of
  # the same name) to register host-owned domains (assets, telemetry, …)
  # without spinning up a second loader. Root entries are either:
  #
  #   * a binary path, used verbatim
  #   * `{:app, app_name, sub}`, resolved through `Application.app_dir/2`,
  #     release-safe and portable across `mix`, `mix phx.server`, and releases
  #
  # An explicit `:root` opt (used by ergon's own test suite to point at a
  # fixture directory) overrides the default ergon root, preserving the
  # original single-root behaviour.
  defp roots(opts) do
    default =
      case Keyword.fetch(opts, :root) do
        {:ok, path} -> [path]
        :error -> [Application.app_dir(:ergon, "priv/queries")]
      end

    extra =
      opts
      |> Keyword.get_values(:extra_roots)
      |> List.flatten()
      |> Enum.map(&resolve_root/1)

    default ++ extra
  end

  defp resolve_root({:app, app, sub}), do: Application.app_dir(app, sub)
  defp resolve_root(path) when is_binary(path), do: path

  defp sql_files(dir) do
    dir
    |> Path.join("**/*.sql")
    |> Path.wildcard()
    |> Enum.sort()
  end

  # priv/queries/jobs/insert.sql -> {:jobs, :insert}
  defp key_for(path, dir) do
    relative = Path.relative_to(path, dir)
    operation = relative |> Path.basename(".sql")
    domain = relative |> Path.dirname() |> Path.split() |> List.last()

    if domain in [nil, ".", ""] do
      raise ArgumentError,
            "SQL file #{path} must live under priv/queries/<domain>/<operation>.sql. " <>
              "Top-level files are not allowed"
    end

    {String.to_atom(domain), String.to_atom(operation)}
  end

  defp detect_collisions!(entries) do
    entries
    |> Enum.group_by(&elem(&1, 0))
    |> Enum.find(fn {_key, group} -> length(group) > 1 end)
    |> case do
      nil -> :ok
      {key, _group} -> raise ArgumentError, "duplicate SQL key #{inspect(key)} in priv/queries"
    end
  end

  defp elem_or(tuple, index) when is_tuple(tuple) and index < tuple_size(tuple),
    do: elem(tuple, index)

  defp elem_or(_tuple, _index), do: "?"
end
