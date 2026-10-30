defmodule BorsNG.GitHub.BatchReadTest do
  use ExUnit.Case
  alias BorsNG.GitHub.{BatchRead, Commit, Pr, ServerMock}
  @head String.duplicate("a", 40)

  defp raw do
    %{
      "number" => 1,
      "headRefOid" => @head,
      "baseRefName" => "main",
      "state" => "OPEN",
      "isDraft" => false,
      "title" => "Title",
      "body" => "Body",
      "author" => %{
        "__typename" => "User",
        "databaseId" => 7,
        "login" => "ada",
        "name" => "Ada",
        "email" => "",
        "avatarUrl" => "url"
      },
      "commits" => %{
        "totalCount" => 1,
        "pageInfo" => %{"hasNextPage" => false},
        "nodes" => [
          %{
            "commit" => %{
              "oid" => @head,
              "author" => %{"name" => "Ada", "email" => "coauthor@example.invalid"}
            }
          }
        ]
      }
    }
  end

  defp decode(raw, mode \\ :identity),
    do:
      BatchRead.decode(
        %{"data" => %{"repository" => %{"databaseId" => 14, "p1" => raw}}},
        14,
        [1],
        mode
      )

  test "live identity requires complete exact repository/PR/head/open/draft/base fields" do
    assert {:ok, %{1 => %Pr{head_sha: @head, state: :open, draft: false, base_ref: "main"}}} =
             decode(raw())

    for state <- ["CLOSED", "MERGED"] do
      assert {:ok, %{1 => %Pr{state: :closed}}} = decode(Map.put(raw(), "state", state))
    end

    for {key, value} <- [
          {"headRefOid", nil},
          {"headRefOid", "other"},
          {"number", 2},
          {"isDraft", nil},
          {"baseRefName", ""},
          {"state", "UNKNOWN"}
        ] do
      assert {:ok, %{1 => :rest}} == decode(Map.put(raw(), key, value))
    end
  end

  test "partial responses recover only affected aliases; global errors have a specific cause" do
    response = %{"data" => %{"repository" => %{"databaseId" => 14, "p1" => raw()}}}
    assert {:ok, %{1 => %Pr{}, 2 => :rest}} = BatchRead.decode(response, 14, [1, 2], :identity)

    partial =
      Map.put(response, "errors", [%{"message" => "partial", "path" => ["repository", "p2"]}])

    assert {:ok, %{1 => %Pr{}, 2 => :rest}} = BatchRead.decode(partial, 14, [1, 2], :identity)

    partial = put_in(partial, ["errors"], [%{"path" => ["repository", "p1", "commits"]}])
    assert {:ok, %{1 => :rest}} = BatchRead.decode(partial, 14, [1], :metadata)

    assert {:error, :batch_graphql_error} =
             BatchRead.decode(
               Map.put(response, "errors", [%{"message" => "partial"}]),
               14,
               [1],
               :identity
             )

    assert {:error, :batch_repository_mismatch} =
             BatchRead.decode(
               put_in(response, ["data", "repository", "databaseId"], 15),
               14,
               [1],
               :identity
             )

    assert {:error, :batch_response_invalid} =
             BatchRead.decode(%{"data" => nil}, 14, [1], :identity)
  end

  test "REST identity recovery requires every authority field explicitly" do
    raw = %{
      "number" => 1,
      "head" => %{"sha" => @head},
      "base" => %{"ref" => "main", "repo" => %{"id" => 14}},
      "state" => "open",
      "draft" => false
    }

    assert {:ok, %Pr{number: 1, head_sha: @head}} = BatchRead.decode_rest(raw, 14, 1)

    for key <- Map.keys(raw) do
      assert {:error, :identity_response_invalid} =
               BatchRead.decode_rest(Map.delete(raw, key), 14, 1)
    end

    assert {:error, :identity_response_invalid} = BatchRead.decode_rest(raw, 15, 1)
    assert {:error, :identity_response_invalid} = BatchRead.decode_rest(raw, 14, 2)
  end

  test "complete metadata retains exact author and full commit attribution" do
    assert {:ok,
            %{
              1 => %{
                pr: %Pr{
                  title: "Title",
                  body: "Body",
                  head_sha: @head,
                  user: %BorsNG.GitHub.User{id: 7, login: "ada"}
                },
                commits: [%Commit{sha: @head, author_email: "coauthor@example.invalid"}]
              }
            }} = decode(raw(), :metadata)
  end

  test "truncated histories, unsupported authors and mismatched metadata retain complete REST lookup" do
    for changed <- [
          put_in(raw(), ["commits", "totalCount"], 101),
          put_in(raw(), ["commits", "pageInfo", "hasNextPage"], true),
          put_in(raw(), ["commits", "nodes"], []),
          put_in(raw(), ["commits", "nodes"], [
            %{"commit" => %{"oid" => String.duplicate("b", 40)}}
          ]),
          Map.put(raw(), "author", nil),
          put_in(raw(), ["author", "__typename"], "Bot")
        ] do
      assert {:ok, %{1 => :rest}} == decode(changed, :metadata)
    end
  end

  test "request bound and integer aliases prevent unbounded or interpolated requests" do
    req = BatchRead.request("TauCetiProject/TauCeti", [1, 20], :metadata)
    assert req.variables == %{owner: "TauCetiProject", name: "TauCeti"}
    assert req.query =~ "p20:pullRequest(number:20)"
    assert req.query =~ "commits(first:100)"

    assert_raise MatchError, fn ->
      BatchRead.request("owner/repo", Enum.to_list(1..21), :identity)
    end

    assert_raise MatchError, fn ->
      BatchRead.request("owner/repo", ["1) { injected"], :identity)
    end
  end

  test "forty-one live PRs use three chunked serial requests without a mutable cache" do
    conn = {{:raw, "test"}, 14}
    pulls = Map.new(1..41, &{&1, %Pr{number: &1, head_sha: @head, state: :open}})
    ServerMock.put_state(%{conn => %{pulls: pulls}})

    assert {:ok, live} =
             BatchRead.fetch(conn, "TauCetiProject/TauCeti", Enum.to_list(1..41), :identity)

    assert map_size(live) == 41

    assert Enum.map(ServerMock.get_state().batch_read_calls, fn {_, numbers} ->
             length(numbers)
           end) == [1, 20, 20]

    ServerMock.put_state(
      put_in(
        ServerMock.get_state(),
        [conn, :pulls, 1, Access.key(:head_sha)],
        String.duplicate("b", 40)
      )
    )

    assert {:ok, %{1 => %Pr{head_sha: head}}} =
             BatchRead.fetch(conn, "TauCetiProject/TauCeti", [1], :identity)

    assert head != @head
  end
end
