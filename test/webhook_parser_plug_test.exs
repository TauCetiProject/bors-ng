defmodule BorsNG.WebhookParserPlugTest do
  use ExUnit.Case, async: true

  alias BorsNG.WebhookParserPlug

  @secret "webhook-test-secret"
  @body ~s({"zen":"okay"})

  test "accepts GitHub's SHA-256 signature" do
    conn = signed_conn("x-hub-signature-256", "sha256=", :sha256)

    assert %{"zen" => "okay"} = WebhookParserPlug.call(conn, secret: @secret).body_params
  end

  test "continues to accept a legacy SHA-1 signature" do
    conn = signed_conn("x-hub-signature", "sha1=", :sha)

    assert %{"zen" => "okay"} = WebhookParserPlug.call(conn, secret: @secret).body_params
  end

  test "rejects an invalid SHA-256 signature even when SHA-1 is valid" do
    conn =
      signed_conn("x-hub-signature", "sha1=", :sha)
      |> Plug.Conn.put_req_header("x-hub-signature-256", "sha256=bad")

    assert %{status: 401, halted: true} = WebhookParserPlug.call(conn, secret: @secret)
  end

  defp signed_conn(header, prefix, algorithm) do
    signature =
      :crypto.mac(:hmac, algorithm, @secret, @body)
      |> Base.encode16(case: :lower)

    Plug.Test.conn("POST", "/webhook/github", @body)
    |> Plug.Conn.put_req_header(header, prefix <> signature)
  end
end
