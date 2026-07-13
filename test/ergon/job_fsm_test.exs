defmodule Ergon.JobFSMTest do
  use ExUnit.Case, async: true

  alias Ergon.FSM.InvalidTransition
  alias Ergon.{Job, JobFSM}

  defp job(state \\ :available, attempt \\ 0) do
    %Job{
      id: 7,
      queue: "default",
      worker: "test",
      payload: "{}",
      state: state,
      fingerprint: "abc",
      attempt: attempt,
      max_attempts: 3,
      last_error: nil,
      scheduled_at: DateTime.utc_now(),
      inserted_at: DateTime.utc_now()
    }
  end

  test "drives an available job through a successful lifecycle" do
    {:ok, pid} = JobFSM.start_link(job())

    assert {:ok, running} = JobFSM.start_execution(pid)
    assert running.state == :executing
    assert running.attempt == 1

    assert {:ok, done} = JobFSM.complete(pid)
    assert done.state == :completed

    assert JobFSM.job(pid).state == :completed
    JobFSM.stop(pid)
  end

  test "rejects an illegal transition without changing state" do
    {:ok, pid} = JobFSM.start_link(job())

    assert {:error, %InvalidTransition{from: :available, event: :succeeded}} =
             JobFSM.complete(pid)

    assert JobFSM.job(pid).state == :available
    JobFSM.stop(pid)
  end

  test "a failed run retries while attempts remain" do
    {:ok, pid} = JobFSM.start_link(job(:executing, 1))

    assert {:ok, retried} = JobFSM.fail(pid, "boom")
    assert retried.state == :available
    assert retried.last_error == "boom"
    JobFSM.stop(pid)
  end
end
