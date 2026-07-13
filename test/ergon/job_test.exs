defmodule Ergon.JobTest do
  use ExUnit.Case, async: true

  alias Ergon.Job

  test "every state round-trips through its string form" do
    for state <- [:available, :executing, :completed, :failed, :discarded] do
      assert Job.state_to_string(state) |> Job.state_from_string() == {:ok, state}
    end
  end

  test "an unknown state string is rejected" do
    assert Job.state_from_string("nonsense") == :error
  end

  test "from_row/1 decodes a projected row into a struct" do
    now = DateTime.utc_now()

    row = [
      42,
      "mailers",
      "send_email",
      ~s({"to":"a@b.com"}),
      "available",
      "abc123",
      0,
      20,
      nil,
      now,
      now
    ]

    assert %Job{
             id: 42,
             queue: "mailers",
             worker: "send_email",
             payload: ~s({"to":"a@b.com"}),
             state: :available,
             fingerprint: "abc123",
             attempt: 0,
             max_attempts: 20,
             last_error: nil
           } = Job.from_row(row)
  end
end
