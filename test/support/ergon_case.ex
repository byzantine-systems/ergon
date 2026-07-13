defmodule Ergon.Case do
  @moduledoc """
  Test case for integration tests that hit the database. Each test runs inside
  a Sandbox transaction that is rolled back afterwards, so tests are isolated
  and leave no rows behind.

  Tests using this case are tagged `:integration` and require a live
  PostgreSQL 18/19 with Ergon's migrations applied.
  """
  use ExUnit.CaseTemplate

  using do
    quote do
      alias Ergon.Repo
    end
  end

  setup tags do
    pid = Ecto.Adapters.SQL.Sandbox.start_owner!(Ergon.Repo, shared: not tags[:async])
    on_exit(fn -> Ecto.Adapters.SQL.Sandbox.stop_owner(pid) end)
    :ok
  end
end
