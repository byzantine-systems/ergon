defmodule Ergon.WorkerRegistry do
  @moduledoc """
  Routes `NOTIFY` wake-ups from `Ergon.JobNotifier` to the workers that care.

  A `Registry` in `:duplicate` mode keyed by queue name: every `Ergon.Worker`
  registers under its `queue.name` on init, and several workers draining the
  same queue all coexist under the one key. When a job lands on a queue,
  `Ergon.JobNotifier` calls `wake/1`, which fans a `:wake` message out to every
  worker registered for that queue. They then race to drain it, sorting the
  contention out via `FOR UPDATE SKIP LOCKED` in `checkout` exactly as they do
  on a periodic poll, so a duplicate or spurious wake is always harmless.

  This is purely the fast-path plumbing. If the notifier is disabled (or a
  wake is lost), workers still drain on their periodic fallback poll, so the
  registry never being reached costs latency, never correctness.
  """

  @doc """
  Child spec so the registry can be listed directly in a supervision tree.

  Started ahead of both `Ergon.JobNotifier` and `Ergon.WorkerSupervisor` in
  `Ergon.Application` so that workers can register the moment they boot.
  """
  def child_spec(_opts) do
    Registry.child_spec(keys: :duplicate, name: __MODULE__)
  end

  @doc """
  Registers the calling worker process under `queue_name`.

  Called from `Ergon.Worker.init/1`. `:duplicate` keys allow any number of
  workers on the same queue.
  """
  @spec register(String.t()) :: {:ok, pid()} | {:error, term()}
  def register(queue_name) when is_binary(queue_name) do
    Registry.register(__MODULE__, queue_name, nil)
  end

  @doc """
  Sends `:wake` to every worker registered for `queue_name`.

  A no-op when no worker is registered (e.g. the notifier saw a queue this node
  doesn't drain).
  """
  @spec wake(String.t()) :: :ok
  def wake(queue_name) when is_binary(queue_name) do
    Registry.dispatch(__MODULE__, queue_name, fn entries ->
      for {pid, _value} <- entries, do: send(pid, :wake)
    end)
  end
end
