defmodule BorsNG.ServerControllerTest do
  use BorsNG.ConnCase

  test "health endpoint should return 200 status code", %{conn: conn} do
    conn = get(conn, "/health")
    assert conn.status == 200
  end

  test "health reports the backend's effective speculative successor limit", %{conn: conn} do
    old = System.get_env("BORS_SPECULATION_CANARY_SUCCESSORS")
    System.put_env("BORS_SPECULATION_CANARY_SUCCESSORS", "10")

    on_exit(fn ->
      if old,
        do: System.put_env("BORS_SPECULATION_CANARY_SUCCESSORS", old),
        else: System.delete_env("BORS_SPECULATION_CANARY_SUCCESSORS")
    end)

    conn = get(conn, "/health")
    assert conn.status == 200
    assert get_resp_header(conn, "x-bors-speculative-successors") == ["10"]
  end
end
