defmodule BorsNG.Worker.StagingFailure do
  @moduledoc "Bind an inconclusive Actions result to the exact trusted staging build."

  def build_source(%{"sha" => head, "statuses" => statuses}, head, repo) when is_list(statuses) do
    latest = statuses |> Enum.filter(&is_map/1) |> Enum.uniq_by(& &1["context"])

    case Enum.find(
           latest,
           &(&1["context"] in ["build", "bump-guard", "scope"] and
               &1["state"] in ["failure", "error"])
         ) do
      %{"state" => state, "creator" => %{"id" => 41_898_282}, "target_url" => url}
      when state in ["failure", "error"] and is_binary(url) ->
        uri = URI.parse(url)
        prefix = "/#{repo}/actions/runs/"

        if uri.scheme == "https" and uri.host == "github.com" and
             is_binary(uri.path) and String.starts_with?(uri.path, prefix) and
             is_nil(uri.query) and is_nil(uri.fragment) do
          case Integer.parse(String.replace_prefix(uri.path, prefix, "")) do
            {id, ""} when id > 0 -> {:lookup, id}
            _ -> :unavailable
          end
        else
          :unavailable
        end

      nil ->
        # A newer status superseded the database failure, or the finalizer
        # has not posted it yet. This is propagation, not a failed retry.
        :pending

      _ ->
        :unavailable
    end
  end

  def build_source(_, _, _), do: :unavailable

  def classify_run(run, batch, head, branch, base) when is_map(run) do
    title = "bors branch=#{branch} batch=#{batch} head=#{head} base=#{base}"

    older =
      ~r/^bors branch=#{Regex.escape(branch)} batch=(\d+) head=#{Regex.escape(head)} base=#{Regex.escape(base || "")}$/

    if run["event"] == "repository_dispatch" and
         run["path"] == ".github/workflows/pr-build.yml" and
         run["display_title"] == title do
      case {run["status"], run["conclusion"]} do
        {"completed", "cancelled"} -> :cancelled
        {"completed", result} when result in ["success", "failure", "timed_out"] -> :conclusive
        {status, _} when status in ["queued", "pending", "in_progress", "waiting"] -> :pending
        _ -> :unavailable
      end
    else
      case Regex.run(older, run["display_title"] || "") do
        [_, previous] ->
          if run["event"] == "repository_dispatch" and
               run["path"] == ".github/workflows/pr-build.yml" and
               String.to_integer(previous) < batch,
             do: :pending,
             else: :unavailable

        _ ->
          :unavailable
      end
    end
  end

  def classify_run(_, _, _, _, _), do: :unavailable
end
