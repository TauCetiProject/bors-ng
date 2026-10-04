defmodule BorsNG.ServerController do
  @moduledoc """
  The controller for server-related actions such as health checking
  """

  use BorsNG.Web, :controller

  def health(conn, _params) do
    conn |> send_resp(200, "healthy")
  end

  def merge_reconcile(conn, _params) do
    expected = System.get_env("GITHUB_WEBHOOK_SECRET") || ""
    supplied = List.first(get_req_header(conn, "x-bors-internal-secret")) || ""

    if expected != "" and Plug.Crypto.secure_compare(expected, supplied) do
      case BorsNG.Worker.MergeReconciler.tick() do
        nil -> conn |> put_status(503) |> json(%{error: "observation unavailable"})
        observation -> json(conn, Map.put(observation, :schema, "tauceti-merge.observation/v1"))
      end
    else
      send_resp(conn, 401, "unauthorized")
    end
  end
end
