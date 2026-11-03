defmodule BorsNG.Worker.Zulip do
  @moduledoc """
  A helper module containing code for sending notifications to Zulip,
  as configured by the environment variables:

  - ZULIP_API_URL
  - ZULIP_BOT_EMAIL
  - ZULIP_BOT_API_KEY
  - ZULIP_CHANNEL_NAME
  - ZULIP_TOPIC
  """
  require Logger

  def send_message(message) do
    if Application.get_env(:bors, :is_test) or Confex.fetch_env!(:bors, :zulip_topic) == "" do
      :ok
    else
      case deliver(message) do
        {:ok, _} ->
          :ok

        {:error, reason} ->
          Logger.warning("Zulip delivery pending: #{inspect(reason)}")
          :error
      end
    end
  end

  def deliver(message, options \\ []) do
    try do
      zulip_api_url = Confex.fetch_env!(:bors, :zulip_api_url)
      bot_email = Confex.fetch_env!(:bors, :zulip_bot_email)
      bot_api_key = Confex.fetch_env!(:bors, :zulip_bot_api_key)
      channel_name = Confex.fetch_env!(:bors, :zulip_channel_name)
      topic = Keyword.get(options, :topic, Confex.fetch_env!(:bors, :zulip_topic))

      # Skip if any required config is empty
      if empty_string?(zulip_api_url) or empty_string?(bot_email) or empty_string?(bot_api_key) or
           empty_string?(channel_name) or empty_string?(topic) do
        {:error, :not_configured}
      else
        body = %{
          "type" => "channel",
          "to" => channel_name,
          "topic" => topic,
          "content" => message
        }

        client =
          Tesla.client(
            [
              {Tesla.Middleware.BasicAuth, username: bot_email, password: bot_api_key},
              Tesla.Middleware.FormUrlencoded
            ],
            {Tesla.Adapter.Hackney,
             ssl_options: BorsNG.TLS.hackney_ssl_options(),
             connect_timeout: 5000,
             recv_timeout: 5000}
          )

        existing = Keyword.get(options, :message_id)

        response =
          if existing,
            do:
              Tesla.patch(client, zulip_api_url <> "messages/#{existing}", %{"content" => message}),
            else: Tesla.post(client, zulip_api_url <> "messages", body)

        delivery_result(response, existing)
      end
    rescue
      _ -> {:error, :delivery_failed}
    end
  end

  @doc false
  def delivery_result(response, existing \\ nil) do
    with {:ok, %{status: status, body: payload}} when status in 200..299 <- response,
         {:ok, %{"result" => "success"} = result} <- Jason.decode(payload),
         id when is_integer(id) and id > 0 <- existing || result["id"] do
      {:ok, id}
    else
      {:ok, %{status: status}} -> {:error, {:http_status, status}}
      _ -> {:error, :delivery_failed}
    end
  end

  defp empty_string?(""), do: true
  defp empty_string?(_), do: false
end
