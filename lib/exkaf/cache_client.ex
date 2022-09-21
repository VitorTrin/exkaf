defmodule Exkaf.CacheClient do
  @moduledoc false

  @table :exkaf_cache

  @spec create :: :ok
  def create do
    :ets.new(@table, [:set, :named_table, :public, {:read_concurrency, true}])

    :ok
  end

  @spec set(any, any, any) :: :ok
  def set(client_id, client_ref, client_pid) do
    true = :ets.insert(@table, {client_id, {client_ref, client_pid}})

    :ok
  end

  @spec get(any) :: :undefined | {:ok, any, any}
  def get(client_id) do
    case :ets.lookup(@table, client_id) do
      [{_client_id, {client_ref, client_pid}}] ->
        {:ok, client_ref, client_pid}

      [] ->
        :undefined
    end
  end

  @spec take(any) :: [tuple]
  def take(client_id) do
    :ets.take(@table, client_id)
  end

  @spec to_list :: [tuple]
  def to_list do
    :ets.tab2list(@table)
  end
end
