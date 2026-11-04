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

  test "repairable failures do not ask for a manual bors r+" do
    for message <- [
          {:failed, [%{identifier: "ci", url: nil}]},
          {:conflict, :failed, "main"},
          {:withdrawn_suspect, [%{identifier: "ci", url: nil}]}
        ] do
      text = Message.generate_message(message)
      assert text =~ "automatic repair usually takes this on", inspect(message)
      assert text =~ "`keep`", inspect(message)
      refute text =~ "someone with permission", inspect(message)
    end
  end

  test "eligibility cancellations say the review bot re-queues the PR" do
    text = Message.generate_message({:canceled, :failed, :eligibility_changed})
    assert text =~ "the review bot re-queues the PR"
    refute text =~ "someone with permission"
  end

  test "timeouts and unknown cancellations keep the manual next step" do
    for message <- [{:timeout, :failed}, {:canceled, :failed, :something_else}] do
      assert Message.generate_message(message) =~ "someone with permission", inspect(message)
    end
  end

  test "an explicit request to stop still says how to re-queue" do
    for reason <- [:requested, :cancel_all] do
      assert Message.generate_message({:canceled, :failed, reason}) =~
               "Someone with permission can run `bors r+`"
    end
  end

  test "a recovery handoff does not promise a recovery comment" do
    text = Message.generate_message({:canceled, :failed, :source_compatibility})
    assert text =~ "If merge-queue recovery takes it on"
    assert text =~ "`keep`"
    refute text =~ "bors r+"
  end

  test "retry messages are the same in both modes" do
    assert Message.generate_message({:retrying, [%{identifier: "ci", url: nil}]}) =~
             "retrying it in a smaller batch"
  end
end
