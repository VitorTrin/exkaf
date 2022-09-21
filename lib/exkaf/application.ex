defmodule Exkaf.Application do
  @moduledoc false

  use Application

  alias Exkaf.CacheClient

  def start(_type, _args) do
    :ok = CacheClient.create()

    children = [
      {DynamicSupervisor, strategy: :one_for_one, name: Exkaf.DynamicSupervisor}
    ]

    Supervisor.start_link(children, strategy: :one_for_one)
  end

  def add_client(client_id, module, args) do
    DynamicSupervisor.start_child(
      Exkaf.DynamicSupervisor,
      {client_id, {module, :start_link, args}, :transient, :infinity, :worker, [module]}
    )
  end
end
