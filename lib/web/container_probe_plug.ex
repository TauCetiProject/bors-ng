defmodule BorsNG.ContainerProbePlug do
  @moduledoc false

  import Plug.Conn

  def init(options), do: options

  def call(%Plug.Conn{host: host, request_path: "/"} = conn, _options)
      when host in ["containerstarthealthcheck", "ping"] do
    conn |> send_resp(200, "healthy") |> halt()
  end

  def call(conn, _options), do: conn
end
