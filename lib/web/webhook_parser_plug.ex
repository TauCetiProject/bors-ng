defmodule BorsNG.WebhookParserPlug do
  @moduledoc """
  Parse the GitHub webhook payload and verify its HMAC signature.
  """

  import Plug.Conn

  def init(options) do
    options
  end

  def call(conn, options) do
    if conn.path_info == ["webhook", "github"] do
      key = Keyword.get(options, :secret)
      run(conn, options, key)
    else
      conn
    end
  end

  def run(conn, _options, nil) do
    conn
  end

  def run(conn, _options, key) do
    {:ok, body, _} = read_body(conn)

    if valid_signature?(conn, key, body) do
      %Plug.Conn{conn | body_params: Jason.decode!(body)}
    else
      conn
      |> put_resp_content_type("text/plain")
      |> send_resp(401, "Invalid signature")
      |> halt()
    end
  end

  defp valid_signature?(conn, key, body) do
    case get_req_header(conn, "x-hub-signature-256") do
      ["sha256=" <> hex] ->
        compare_mac(hex, :sha256, key, body)

      [] ->
        case get_req_header(conn, "x-hub-signature") do
          ["sha1=" <> hex] -> compare_mac(hex, :sha, key, body)
          _ -> false
        end

      _ ->
        false
    end
  end

  defp compare_mac(hex, algorithm, key, body) do
    case Base.decode16(hex, case: :mixed) do
      {:ok, signature} ->
        mac = :crypto.mac(:hmac, algorithm, key, body)
        byte_size(mac) == byte_size(signature) and Plug.Crypto.secure_compare(mac, signature)

      :error ->
        false
    end
  end
end
