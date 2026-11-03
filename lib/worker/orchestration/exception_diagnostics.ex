defmodule BorsNG.Worker.Orchestration.ExceptionDiagnostics do
  @moduledoc "Exception locations without messages, arguments, payloads or local path prefixes."
  require Logger
  alias BorsNG.Worker.Orchestration.Outbox

  def describe(error, stacktrace) do
    frames =
      stacktrace
      |> Enum.take(8)
      |> Enum.flat_map(fn
        {module, function, args, location} when is_atom(module) and is_atom(function) ->
          arity = if is_list(args), do: length(args), else: args

          [
            %{
              module: inspect(module),
              function: Atom.to_string(function),
              arity: arity,
              file: location |> Keyword.get(:file, ~c"unknown") |> to_string() |> Path.basename(),
              line: Keyword.get(location, :line)
            }
          ]

        _ ->
          []
      end)

    %{exception: inspect(error.__struct__), frames: frames}
  end

  def record(job, error, stacktrace) do
    details = describe(error, stacktrace)

    Logger.error(
      "outbox_exception job=#{job.id} project=#{job.project_id} kind=#{job.kind} attempt=#{job.attempts} location=#{Jason.encode!(details)}"
    )

    Outbox.record_exception(job, details)
    :ok
  rescue
    _ -> :ok
  end
end
