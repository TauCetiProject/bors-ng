defmodule BorsNG.Worker.BatcherMessageAutomaticRequeueTest do
  # Not async: these tests set a process-wide environment variable.
  use ExUnit.Case, async: false

  alias BorsNG.Worker.Batcher.Message

  setup do
    previous = System.get_env("BORS_AUTOMATIC_REQUEUE")
    System.put_env("BORS_AUTOMATIC_REQUEUE", "true")

    on_exit(fn ->
      if previous,
        do: System.put_env("BORS_AUTOMATIC_REQUEUE", previous),
        else: System.delete_env("BORS_AUTOMATIC_REQUEUE")
    end)
  end

  test "terminal failures do not ask for a manual bors r+" do
    for message <- [
          {:failed, [%{identifier: "ci", url: nil}]},
          {:conflict, :failed, "main"},
          {:timeout, :failed},
          {:canceled, :failed, :eligibility_changed},
          {:canceled, :failed, :something_else}
        ] do
      text = Message.generate_message(message)
      assert text =~ "You don't need to run `bors r+`", inspect(message)
      refute text =~ "someone with permission", inspect(message)
    end
  end

  test "an explicit request to stop still says how to re-queue" do
    assert Message.generate_message({:canceled, :failed, :requested}) =~
             "Someone with permission can run `bors r+`"
  end

  test "a recovery handoff points at the recovery comment instead" do
    text = Message.generate_message({:canceled, :failed, :source_compatibility})
    assert text =~ "merge-queue recovery comment"
    refute text =~ "bors r+"
  end

  test "retry messages are the same in both modes" do
    assert Message.generate_message({:retrying, [%{identifier: "ci", url: nil}]}) =~
             "retrying it in a smaller batch"
  end
end
