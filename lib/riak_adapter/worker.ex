defmodule RiakAdapter.Worker do
  use GenServer

  @timeout 5000

  def start(args) do
    :gen_server.start(__MODULE__, args, [])
  end

  def start_link(args) do
    :gen_server.start_link(__MODULE__, args, [])
  end


  def ping!(worker, timeout \\ @timeout) do
    case :gen_server.call(worker, {:ping, timeout}, timeout) do
      :pong -> :pong
      {:error, %RiakAdapter.Error{} = err} -> raise err
    end
  end


  @spec create_search_index!(pid, String.t)
  def create_search_index!(worker, name) do
    create_search_index!(worker, name, "_yz_default", [], @timeout)
  end

  @spec create_search_index!(pid, String.t, String.t)
  def create_search_index!(worker, name, schema) do
    create_search_index!(worker, name, schema, [], @timeout)
  end

  @spec create_search_index!(pid, String.t, [Keyword], String.t, integer)
  def create_search_index!(worker, name, schema, search_admin_opts, timeout) do
    case :gen_server.call(worker, {:create_search_index, name, timeout}, timeout) do
      :pong -> :pong
      {:error, %RiakAdapter.Error{} = err} -> raise err
    end
  end


  def query!(worker, sql, params, timeout \\ @timeout) do
    case :gen_server.call(worker, {:query, sql, params, timeout}, timeout) do
      {:ok, res} -> res
      {:error, %RiakAdapter.Error{} = err} -> raise err
    end
  end

  def monitor_me(worker) do
    :gen_server.cast(worker, {:monitor, self})
  end

  def demonitor_me(worker) do
    :gen_server.cast(worker, {:demonitor, self})
  end

  def init(opts) do
    Process.flag(:trap_exit, true)

    eager? = Keyword.get(opts, :lazy, true) in [false, "false"]

    if eager? do
      case RiakAdapter.Connection.start_link(opts) do
        {:ok, conn} ->
          conn = conn
        _ ->
          :ok
      end
    end

    {:ok, Map.merge(new_state, %{conn: conn, params: opts})}
  end

  # Connection is disconnected, reconnect before continuing
  def handle_call(request, from, %{conn: nil, params: params} = s) do
    case RiakAdapter.Connection.start_link(params) do
      {:ok, conn} ->
        handle_call(request, from, %{s | conn: conn})
      {:error, err} ->
        {:reply, {:error, err}, s}
    end
  end


  def handle_call({:create_search_index, name, timeout}, _from, %{conn: conn} = s) do
    {:reply, RiakAdapter.Connection.create_search_index(conn, timeout), s}
  end


  def handle_call({:ping, timeout}, _from, %{conn: conn} = s) do
    {:reply, RiakAdapter.Connection.ping(conn, timeout), s}
  end

  def handle_call({:query, sql, params, timeout}, _from, %{conn: conn} = s) do
    {:reply, RiakAdapter.Connection.query(conn, sql, params, timeout), s}
  end

  def handle_cast({:monitor, pid}, %{monitor: nil} = s) do
    ref = Process.monitor(pid)
    {:noreply, %{s | monitor: {pid, ref}}}
  end

  def handle_cast({:demonitor, pid}, %{monitor: {pid, ref}} = s) do
    Process.demonitor(ref)
    {:noreply, %{s | monitor: nil}}
  end

  def handle_info({:EXIT, conn, _reason}, %{conn: conn} = s) do
    {:noreply, %{s | conn: nil}}
  end

  def handle_info({:DOWN, ref, :process, pid, _info}, %{monitor: {pid, ref}} = s) do
    {:stop, :normal, s}
  end

  def handle_info(_info, s) do
    {:noreply, s}
  end

  def terminate(_reason, %{conn: nil}) do
    :ok
  end

  def terminate(_reason, %{conn: conn}) do
    RiakAdapter.Connection.stop(conn)
  end

  defp new_state do
    %{conn: nil, params: nil, monitor: nil}
  end
end
