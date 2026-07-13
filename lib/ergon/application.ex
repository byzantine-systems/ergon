defmodule Ergon.Application do
  @moduledoc """
  Ergon's supervision tree.

  Children started: the `Ergon.Repo` connection pool, the `Ergon.SQL` cache,
  the `Ergon.WorkerRegistry` and (optionally) `Ergon.JobNotifier` that drive
  the `LISTEN`/`NOTIFY` wake-up fast path, and the `Ergon.WorkerSupervisor`
  `DynamicSupervisor` under which queue workers are spawned at runtime. Workers
  are added with `Ergon.start_worker/2` rather than being listed here, because
  which queues to drain is the host application's decision.
  """
  use Application

  @impl true
  def start(_type, _args) do
    children =
      [
        Ergon.Repo,
        # Filesystem SQL cache. Must be up before any consumer
        # that calls `Ergon.SQL.query/3`, including the WorkerSupervisor below.
        Ergon.SQL,
        # Wake-up routing registry. Ahead of both the notifier and the workers
        # so a worker can register the moment it boots.
        Ergon.WorkerRegistry
      ] ++
        job_notifier() ++
        [Ergon.WorkerSupervisor]

    Supervisor.start_link(children, strategy: :one_for_one, name: Ergon.Supervisor)
  end

  # The reactive `LISTEN` fast path is optional (mirroring the pgmq producer's
  # `:notify_channel`): disabled, workers still drain correctly via their
  # fallback poll, only slower. Disable with
  # `config :ergon, Ergon.JobNotifier, enabled: false`.
  defp job_notifier do
    if Application.get_env(:ergon, Ergon.JobNotifier, [])[:enabled] == false do
      []
    else
      [Ergon.JobNotifier]
    end
  end
end
