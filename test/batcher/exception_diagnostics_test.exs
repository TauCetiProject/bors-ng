defmodule BorsNG.Worker.Orchestration.ExceptionDiagnosticsTest do
  use ExUnit.Case, async: true
  alias BorsNG.Worker.Orchestration.ExceptionDiagnostics

  defp bad_boolean(value), do: value and true

  test "BadBooleanError records the failing function and line without its secret argument" do
    secret = "private-error-value-do-not-log"

    details =
      try do
        bad_boolean(secret)
      rescue
        e in BadBooleanError -> ExceptionDiagnostics.describe(e, __STACKTRACE__)
      end

    assert details.exception == "BadBooleanError"
    assert hd(details.frames).function == "bad_boolean"
    assert hd(details.frames).arity == 1
    assert is_integer(hd(details.frames).line)
    refute Jason.encode!(details) =~ secret
  end

  test "stack arguments, error text and directory prefixes are excluded and frames bounded" do
    secret = "sensitive-source-or-credential"

    trace =
      List.duplicate(
        {BorsNG.Worker.Batcher, :handle_info, [secret, 1],
         [file: ~c"/private/location/batcher.ex", line: 42]},
        50
      )

    details = ExceptionDiagnostics.describe(%RuntimeError{message: secret}, trace)
    assert length(details.frames) == 8

    assert Enum.all?(
             details.frames,
             &(&1.file == "batcher.ex" and &1.arity == 2 and &1.line == 42)
           )

    refute Jason.encode!(details) =~ secret
    refute Jason.encode!(details) =~ "/private"
  end
end
