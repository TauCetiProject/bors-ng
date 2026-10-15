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
      assert {:error, :batch_read_unavailable} == decode(Map.put(raw(), key, value))
    end
  end

  test "partial GraphQL errors, missing PRs and cross-repository responses fail closed" do
    response = %{"data" => %{"repository" => %{"databaseId" => 14, "p1" => raw()}}}

    for invalid <- [
          Map.put(response, "errors", [%{"message" => "partial"}]),
          put_in(response, ["data", "repository", "databaseId"], 15),
          put_in(response, ["data", "repository", "p1"], nil),
          %{"data" => nil}
        ] do
      assert {:error, :batch_read_unavailable} == BatchRead.decode(invalid, 14, [1], :identity)
    end

    assert {:error, :batch_read_unavailable} == BatchRead.decode(response, 14, [1, 2], :identity)
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

  test "forty-one live PRs use three bounded serial requests without a mutable cache" do
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
