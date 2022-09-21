defmodule Exkaf.Config do
  @moduledoc false

  @valid_configs [
    :builtin_features,
    :debug,
    :client_id,
    :bootstrap_servers,
    :message_max_bytes,
    :message_copy_max_bytes,
    :receive_message_max_bytes,
    :max_in_flight,
    :metadata_request_timeout_ms,
    :topic_metadata_refresh_interval_ms,
    :metadata_max_age_ms,
    :topic_metadata_refresh_fast_interval_ms,
    :topic_metadata_refresh_sparse,
    :topic_metadata_propagation_max_ms,
    :topic_blacklist,
    :socket_timeout_ms,
    :socket_send_buffer_bytes,
    :socket_receive_buffer_bytes,
    :socket_keepalive_enable,
    :socket_nagle_disable,
    :socket_max_fails,
    :broker_address_ttl,
    :broker_address_family,
    :reconnect_backoff_ms,
    :reconnect_backoff_max_ms,
    :statistics_interval_ms,
    :enabled_events,
    :log_level,
    :log_queue,
    :log_thread_name,
    :enable_random_seed,
    :log_connection_close,
    :api_version_request,
    :api_version_request_timeout_ms,
    :api_version_fallback_ms,
    :broker_version_fallback,
    :security_protocol,
    :ssl_cipher_suites,
    :ssl_curves_list,
    :ssl_sigalgs_list,
    :ssl_key_location,
    :ssl_key_password,
    :ssl_key_pem,
    :ssl_certificate_location,
    :ssl_certificate_pem,
    :ssl_ca_location,
    :ssl_crl_location,
    :ssl_keystore_location,
    :ssl_keystore_password,
    :enable_ssl_certificate_verification,
    :ssl_endpoint_identification_algorithm,
    :sasl_mechanisms,
    :sasl_kerberos_service_name,
    :sasl_kerberos_principal,
    :sasl_kerberos_kinit_cmd,
    :sasl_kerberos_keytab,
    :sasl_kerberos_min_time_before_relogin,
    :sasl_username,
    :sasl_password,
    :sasl_oauthbearer_config,
    :enable_sasl_oauthbearer_unsecure_jwt,
    :group_instance_id,
    :session_timeout_ms,
    :partition_assignment_strategy,
    :heartbeat_interval_ms,
    :group_protocol_type,
    :coordinator_query_interval_ms,
    :max_poll_interval_ms,
    :auto_commit_interval_ms,
    :queued_min_messages,
    :queued_max_messages_kbytes,
    :fetch_wait_max_ms,
    :fetch_message_max_bytes,
    :fetch_max_bytes,
    :fetch_min_bytes,
    :fetch_error_backoff_ms,
    :allow_auto_create_topics,
    :client_rack,
    :transactional_id,
    :transaction_timeout_ms,
    :check_crcs,
    :isolation_level,
    :enable_idempotence,
    :enable_gapless_guarantee,
    :queue_buffering_max_messages,
    :queue_buffering_max_kbytes,
    :queue_buffering_max_ms,
    :message_send_max_retries,
    :retry_backoff_ms,
    :queue_buffering_backpressure_threshold,
    :compression_codec,
    :batch_num_messages,
    :batch_size,
    :delivery_report_only_error,
    :plugin_library_paths,
    :sticky_partitioning_linger_ms
  ]

  @valid_topic_configs [
    :request_required_acks,
    :request_timeout_ms,
    :message_timeout_ms,
    :partitioner,
    :compression_codec,
    :compression_level,
    :auto_commit_interval_ms,
    :auto_offset_reset,
    :offset_store_path,
    :offset_store_sync_interval_ms,
    :consume_callback_max_messages
  ]

  def convert_kafka_config(config) do
    result =
      Enum.reduce_while(config, {[], []}, fn {key, value}, {exkaf_acc, rd_kafka_acc} ->
        with false <- is_exkaf_config(key, value),
             {converted_key, converted_value} when converted_key != :error <-
               to_lib_rdkafka_config(key, value) do
          {:cont, {exkaf_acc, [{converted_key, converted_value} | rd_kafka_acc]}}
        else
          true ->
            {:cont, {[{key, value} | exkaf_acc], rd_kafka_acc}}

          {:error, {:options, _}} = error ->
            {:halt, error}
        end
      end)

    case result do
      {:error, error} ->
        {:error, error}

      {exkaf_config, rd_kafka_config} ->
        {:ok, exkaf_config, rd_kafka_config}
    end
  end

  def convert_topic_config(config) do
    result =
      Enum.reduce_while(config, [], fn {key, value}, acc ->
        case to_lib_rdkafka_topic_config(key, value) do
          {converted_key, converted_value} when converted_key != :error ->
            {:cont, [{converted_key, converted_value} | acc]}

          {:error, {:options, _}} = error ->
            {:halt, error}
        end
      end)

    case result do
      {:error, error} ->
        {:error, error}

      result_list ->
        {:ok, [], result_list}
    end
  end

  defp is_exkaf_config(:delivery_report_callback = key, value) do
    check_callback(key, value)
  end

  defp is_exkaf_config(:stats_callback = key, value) do
    check_callback(key, value)
  end

  defp is_exkaf_config(:queue_buffering_overflow_strategy = key, value) do
    if value in [:local_disk_queue, :block_calling_process, :drop_records] do
      true
    else
      {:error, {:options, {key, value}}}
    end
  end

  defp is_exkaf_config(_key, _value), do: false

  defp check_callback(key, value) do
    if is_function(value, 2) or is_atom(value) do
      true
    else
      {:error, {:options, {key, value}}}
    end
  end

  defp to_lib_rdkafka_config(key, value) do
    if key in @valid_configs do
      {
        key |> Atom.to_string() |> String.replace("_", "."),
        inspect(value)
      }
    else
      {:error, {:options, {key, value}}}
    end
  end

  defp to_lib_rdkafka_topic_config(key, value) do
    if key in @valid_topic_configs do
      {
        key |> Atom.to_string() |> String.replace("_", "."),
        inspect(value)
      }
    else
      {:error, {:options, {key, value}}}
    end
  end
end
