defmodule RiakAdapter.Connection do
  def start_link(opts) do
    host = format_host(opts[:host])
    port = opts[:port]
    new_opts = Keyword.delete(opts, :host) |> Keyword.delete(:port)

    :riakc_pb_socket.start_link(host, port, new_opts)
  end


  def ping(pid, timeout) do
    :riakc_pb_socket.ping(pid, timeout)
  end


  def stop(pid) do
    :riakc_pb_socket.stop(pid)
  end


  defp format_host(host) when is_list(host) or is_tuple(host) do
    host
  end

  defp format_host(host), do: '#{host}'
end
