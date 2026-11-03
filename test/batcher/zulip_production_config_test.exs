defmodule BorsNG.Worker.ZulipProductionConfigTest do
  use ExUnit.Case, async: false
  alias BorsNG.Worker.Zulip

  setup do
    keys = [
      :is_test,
      :zulip_topic,
      :zulip_api_url,
      :zulip_bot_email,
      :zulip_bot_api_key,
      :zulip_channel_name
    ]

    saved = Map.new(keys, &{&1, Application.fetch_env(:bors, &1)})

    on_exit(fn ->
      Enum.each(saved, fn
        {key, {:ok, value}} -> Application.put_env(:bors, key, value)
        {key, :error} -> Application.delete_env(:bors, key)
      end)
    end)

    Application.delete_env(:bors, :is_test)
    # Entirely local: these tests cannot contact Zulip.
    for key <- tl(keys), do: Application.put_env(:bors, key, "")
    :ok
  end

  test "production's missing test flag safely skips disabled notifications" do
    assert :ok == Zulip.send_message("disabled local notification")
  end

  test "missing test flag and incomplete notification config return an error rather than crash" do
    Application.put_env(:bors, :zulip_topic, "configured-topic")
    assert :error == Zulip.send_message("undeliverable local notification")
  end
end
