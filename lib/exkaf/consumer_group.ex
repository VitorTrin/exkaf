defmodule Exkaf.ConsumerGroup do
  @moduledoc false

  use GenServer

  def init([
        client_id,
        group_id,
        topics,
        ek_client_config,
        rdk_client_config,
        ek_topic_config,
        rdk_topic_config
      ]) do
        Process.flag(:trap_exit, true)
      end
  end
end
