defmodule BorsNG.Worker.Orchestration.PayloadTest do
  use ExUnit.Case, async: true

  test "a cold VM restores producer-defined payload atoms while retaining safe decoding" do
    payloads = [
      {:recover_queue_slice, 3, [1]},
      {:backend_holds_slice, 2, [1, 2]},
      %{members_digest: "frozen", kind: :local_git}
    ]

    encoded = Enum.map(payloads, &Base.encode64(:erlang.term_to_binary(&1)))
    unknown = "untrusted_stored_atom_#{System.unique_integer([:positive])}"
    unknown_binary = <<131, 119, byte_size(unknown), unknown::binary>> |> Base.encode64()

    # Atom literals in this test VM cannot mask a lazy module load in the child.
    script = """
    false = :code.is_loaded(BorsNG.Worker.Batcher)
    false = :code.is_loaded(BorsNG.Worker.CandidateAssembler)
    decoded = Enum.map(#{inspect(encoded)}, fn data ->
      BorsNG.Worker.Orchestration.Outbox.payload(%{payload: Base.decode64!(data)})
    end)
    IO.inspect(decoded)
    try do
      BorsNG.Worker.Orchestration.Outbox.payload(%{payload: Base.decode64!(#{inspect(unknown_binary)})})
      raise "unknown atom accepted"
    rescue
      ArgumentError -> IO.puts("safe_rejected")
    end
    try do
      String.to_existing_atom(#{inspect(unknown)})
      raise "unknown atom interned"
    rescue
      ArgumentError -> IO.puts("still_unknown")
    end
    """

    {output, status} =
      System.cmd(
        System.find_executable("elixir"),
        [
          "-pa",
          Application.app_dir(:bors, "ebin"),
          "-e",
          script
        ],
        stderr_to_stdout: true
      )

    assert status == 0, output
    assert output =~ "recover_queue_slice"
    assert output =~ "backend_holds_slice"
    assert output =~ "members_digest"
    assert output =~ "safe_rejected"
    assert output =~ "still_unknown"
  end
end
