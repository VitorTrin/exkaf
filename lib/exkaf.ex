defmodule Exkaf do
  @moduledoc """
  Documentation for `Exkaf`.
  """

  alias Exkaf.CacheClient
  alias Exkaf.Config

  def create_consumer_group(client_id, group_id, topics, client_config, default_topics_config) do
    with {:cache, :undefined} <- {:cache, CacheClient.get(client_id)},
         :ok <- validate_consumer_topics(topics),
         {:ok, ek_client_config, rdk_client_config} <- Config.convert_kafka_config(client_config),
         {:ok, ek_topic_config, rdk_topic_config} <-
           Config.convert_topic_config(default_topics_config) do
      Exkaf.Application.add_client(client_id, Exkaf.ConsumerGroup, [
        client_id,
        group_id,
        topics,
        ek_client_config,
        rdk_client_config,
        ek_topic_config,
        rdk_topic_config
      ])
    else
      {:cache, {:ok, _, _}} ->
        {:error, :client_already_exists}

      error ->
        error
    end
  end

  defp validate_consumer_topics([topic_config | rest]) do
    with {key, value} when is_binary(key) and is_list(value) <- topic_config,
         module when module != :undefined and is_atom(module) <-
           Keyword.get(value, :callback_module, :undefined) do
      validate_consumer_topics(rest)
    else
      _ ->
        {:error, :invalid_consumer_topics}
    end
  end

  defp validate_consumer_topics([]), do: :ok
end
