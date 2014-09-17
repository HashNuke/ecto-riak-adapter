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


  def insert(pid, model, opts, timeout) do
    module      = model.__struct__
    bucket_name = module.__schema__(:source)
    primary_key_field = module.__schema__(:primary_key)

    obj_key    = Ecto.Model.primary_key(model) || :undefined
    model_data = map_of_non_virtual_fields(model, primary_key_field)

    encoded_data = encode_data(model_data, @default_content_type)
    object = :riakc_obj.new(bucket_name, obj_key, encoded_data, @default_content_type)
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


  defp map_of_non_virtual_fields(model, primary_key_field) do
    module = model.__struct__
    non_virtual_field_keys = module.__schema__(:keywords, model)
    |> Keyword.keys
    |> List.delete(primary_key_field)

    Map.from_struct(model)
    |> Map.take(non_virtual_field_keys)
  end


  defp encode_data(data, "application/json") do
    {:ok, json_string} = Poison.encode data
    json_string
  end

end
