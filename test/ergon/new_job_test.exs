defmodule Ergon.NewJobTest do
  use ExUnit.Case, async: true

  alias Ergon.NewJob

  test "new/2 sets sensible defaults" do
    job = NewJob.new("resize")
    assert job.queue == "default"
    assert job.worker == "resize"
    assert job.payload == %{}
    assert job.max_attempts == 20
    assert job.uniqueness == :not_unique
  end

  test "the builders set their fields" do
    job =
      NewJob.new("resize", %{"file" => "a.png"})
      |> NewJob.on_queue("images")
      |> NewJob.with_max_attempts(5)
      |> NewJob.unique_for(60)

    assert job.queue == "images"
    assert job.payload == %{"file" => "a.png"}
    assert job.max_attempts == 5
    assert job.uniqueness == {:unique_for, 60}
  end
end
