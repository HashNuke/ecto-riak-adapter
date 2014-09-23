defmodule Ecto.Adapters.Riak.Connection do

  @default_content_type "application/json"

  def start_link(opts) do
    host = format_host(opts[:host])
    port = opts[:port]
    new_opts = Keyword.delete(opts, :host) |> Keyword.delete(:port)

    :riakc_pb_socket.start_link(host, port, new_opts)
  end


  def ping(pid, timeout) do
    :riakc_pb_socket.ping(pid, timeout)
  end


  def create_search_index(pid, name, schema, options) do
    :riakc_pb_socket.create_search_index(pid, name, schema, options)
  end


  def delete_search_index(pid, name, schema, options) do
    :riakc_pb_socket.delete_search_index(pid, name, schema, options)
  end


  def insert(pid, bucket, model, opts, timeout) do
    put_object(pid, :insert, bucket, model, opts, timeout)
  end


  def update(pid, bucket, model, opts, timeout) do
    put_object(pid, :update, bucket, model, opts, timeout)
  end


  def run_custom(pid, fun) do
    fun.(pid)
  end


  defp put_object(pid, _action, bucket, model, opts, timeout) do
    module      = model.__struct__
    primary_key_field = module.__schema__(:primary_key)

    obj_key    = Ecto.Model.primary_key(model) || :undefined
    model_data = fields(model, primary_key_field)

    encoded_data = encode_data(model_data, @default_content_type)
    object = :riakc_obj.new(bucket, obj_key, encoded_data, @default_content_type)
    case :riakc_pb_socket.put(pid, object, opts, timeout) do
      :ok ->
        return_values = module.__schema__(:keywords, model)
        { :ok, model }
      {:ok, return_obj} ->
        key = get_key_from_obj(return_obj)
        model = Ecto.Model.put_primary_key(model, key)
        return_values = module.__schema__(:keywords, model)
        { :ok, return_values }
      {:error, error} ->
        { :error, error }
    end
  end


  def stop(pid) do
    :riakc_pb_socket.stop(pid)
  end


  defp format_host(host) when is_list(host) or is_tuple(host) do
    host
  end

  defp format_host(host), do: '#{host}'


  defp get_key_from_obj(obj) do
    {:riakc_obj, _bucket_name, key, _, _, _, _} = obj
    key
  end


  defp fields(model, primary_key_field) do
    module = model.__struct__
    non_virtual_fields = module.__schema__(:keywords, model)
    |> Dict.delete(primary_key_field)

    map_fields = Enum.reduce module.__schema__(:field_names), %{}, fn(field_name, acc)->
      case Regex.match?(~r/(.+)_map/, "#{field_name}") && !Dict.has_key?(non_virtual_fields, field_name) do
        true  -> add_map_field(model, field_name, acc)
        false -> acc
      end
    end

    :maps.from_list(non_virtual_fields)
    |> Map.merge(map_fields)
  end


  def add_map_field(model, field_name, data) do
    real_field_name = String.replace(field_name, ~r/_map$/, "")
    case Map.get(model, field_name) do
      nil   -> data
      value -> Map.put(data, real_field_name, value)
    end
  end


  defp encode_data(data, "application/json") do
    {:ok, json_string} = Poison.encode data
    json_string
  end

end
