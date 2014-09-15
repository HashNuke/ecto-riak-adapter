defmodule RiakAdapter do
  @moduledoc """
  This is the adapter module for PostgreSQL. It handles and pools the
  connections to the postgres database with poolboy.

  ## Options

  The options should be given via `Ecto.Repo.conf/0`.

  `:hostname` - Server hostname;
  `:port` - Server port (default: 5432);
  `:username` - Username;
  `:password` - User password;
  `:size` - The number of connections to keep in the pool;
  `:max_overflow` - The maximum overflow of connections (see poolboy docs);
  `:parameters` - Keyword list of connection parameters;
  `:ssl` - Set to true if ssl should be used (default: false);
  `:ssl_opts` - A list of ssl options, see ssl docs;
  `:lazy` - If false all connections will be started immediately on Repo startup (default: true)
  """

  @behaviour Ecto.Adapter
  @behaviour Ecto.Adapter.Storage
  # @behaviour Ecto.Adapter.Transactions
  alias RiakAdapter.Worker

  @default_host "localhost"
  @default_port 8087
  @default_solr_port 8093
  @timeout 5000


  # lib/riak_adapter.ex:1: warning: undefined behaviour macro __using__/1 (for behaviour Ecto.Adapter)
  defmacro __using__(_opts) do
    quote do
      def __riak__(:pool_name) do
        __MODULE__.Pool
      end
    end
  end


  # lib/riak_adapter.ex:1: warning: undefined behaviour function start_link/2 (for behaviour Ecto.Adapter)
  def start_link(repo, opts) do
    {pool_opts, worker_opts} = prepare_start(repo, opts)
    :poolboy.start_link(pool_opts, worker_opts)
  end


  # lib/riak_adapter.ex:1: warning: undefined behaviour function stop/1 (for behaviour Ecto.Adapter)
  def stop(repo) do
    pool = repo_pool(repo)
    :poolboy.stop(pool)
  end


  def create_search_index(repo, name, schema, opts \\ []) do
    pool = repo_pool(repo)
    timeout = opts[:timeout] || @timeout

    repo.log(:ping, fn ->
      use_worker(pool, timeout, fn worker ->
        Worker.create_search_index!(worker, name, schema, opts)
      end)
    end)
  end


  def ping(repo, opts \\ []) do
    pool = repo_pool(repo)
    timeout = opts[:timeout] || @timeout

    repo.log(:ping, fn ->
      use_worker(pool, timeout, fn worker ->
        Worker.ping!(worker, timeout)
      end)
    end)
  end


  # def query(repo, sql, params, opts \\ []) do
  #   pool = repo_pool(repo)
  #
  #   timeout = opts[:timeout] || @timeout
  #   repo.log(:ping, fn ->
  #     use_worker(pool, timeout, fn worker ->
  #       Worker.query!(worker, sql, params, timeout)
  #     end)
  #   end)
  # end


  # lib/riak_adapter.ex:1: warning: undefined behaviour function all/3 (for behaviour Ecto.Adapter)
  # def all(repo, query, opts) do
  #   pg_query = %{query | select: normalize_select(query.select)}
  #
  #   {sql, params} = SQL.select(pg_query)
  #   %Postgrex.Result{rows: rows} = query(repo, sql, params, opts)
  #
  #   # Transform each row based on select expression
  #   transformed =
  #     Enum.map(rows, fn row ->
  #       values = Tuple.to_list(row)
  #       transform_row(pg_query.select.expr, values, pg_query.sources) |> elem(0)
  #     end)
  #
  #   transformed
  #   |> preload(repo, query)
  # end
  #

  # lib/riak_adapter.ex:1: warning: undefined behaviour function insert/3 (for behaviour Ecto.Adapter)
  def insert(repo, model, opts) do
    module    = model.__struct__
    returning = module.__schema__(:keywords, model)
      |> Enum.filter(fn {_, val} -> val == nil end)
      |> Keyword.keys



    {sql, params} = SQL.insert(model, returning)

    # case query(repo, sql, params, opts) do
    #   %Postgrex.Result{rows: [values]} ->
    #     Enum.zip(returning, Tuple.to_list(values))
    #   _ ->
    #     []
    # end
  end
  #
  #
  #
  # # lib/riak_adapter.ex:1: warning: undefined behaviour function update/3 (for behaviour Ecto.Adapter)
  # def update(repo, model, opts) do
  #   {sql, params} = SQL.update(model)
  #   %Postgrex.Result{num_rows: nrows} = query(repo, sql, params, opts)
  #   nrows
  # end
  #
  #
  # # lib/riak_adapter.ex:1: warning: undefined behaviour function update_all/4 (for behaviour Ecto.Adapter)
  # def update_all(repo, query, values, external, opts) do
  #   {sql, params} = SQL.update_all(query, values, external)
  #   %Postgrex.Result{num_rows: nrows} = query(repo, sql, params, opts)
  #   nrows
  # end
  #
  #
  # # lib/riak_adapter.ex:1: warning: undefined behaviour function delete/3 (for behaviour Ecto.Adapter)
  # def delete(repo, model, opts) do
  #   {sql, params} = SQL.delete(model)
  #   %Postgrex.Result{num_rows: nrows} = query(repo, sql, params, opts)
  #   nrows
  # end
  #
  #
  # # lib/riak_adapter.ex:1: warning: undefined behaviour function delete_all/3 (for behaviour Ecto.Adapter)
  # def delete_all(repo, query, opts) do
  #   {sql, params} = SQL.delete_all(query)
  #   %Postgrex.Result{num_rows: nrows} = query(repo, sql, params, opts)
  #   nrows
  # end


  # # lib/riak_adapter.ex:1: warning: undefined behaviour function storage_up/1 (for behaviour Ecto.Adapter.Storage)
  # def storage_up(opts) do
  #   # TODO: allow the user to specify those options either in the Repo or on command line
  #   database_options = ~s(TEMPLATE=template0 ENCODING='UTF8' LC_COLLATE='en_US.UTF-8' LC_CTYPE='en_US.UTF-8')
  #
  #   output = run_with_psql opts, "CREATE DATABASE #{opts[:database]} " <> database_options
  #
  #   cond do
  #     String.length(output) == 0                 -> :ok
  #     String.contains?(output, "already exists") -> {:error, :already_up}
  #     true                                       -> {:error, output}
  #   end
  # end
  #
  #
  # # lib/riak_adapter.ex:1: warning: undefined behaviour function storage_down/1 (for behaviour Ecto.Adapter.Storage)
  # def storage_down(opts) do
  #   output = run_with_psql(opts, "DROP DATABASE #{opts[:database]}")
  #
  #   cond do
  #     String.length(output) == 0                 -> :ok
  #     String.contains?(output, "does not exist") -> {:error, :already_down}
  #     true                                       -> {:error, output}
  #   end
  # end


  # Other stuff


  # defp decoder(%TypeInfo{sender: "interval"}, :binary, default, param) do
  #   {mon, day, sec} = default.(param)
  #   %Ecto.Interval{year: 0, month: mon, day: day, hour: 0, min: 0, sec: sec}
  # end
  #
  # defp decoder(%TypeInfo{sender: sender}, :binary, default, param) when sender in ["timestamp", "timestamptz"] do
  #   default.(param)
  #   |> Ecto.DateTime.from_erl
  # end
  #
  # defp decoder(%TypeInfo{sender: "date"}, :binary, default, param) do
  #   default.(param)
  #   |> Ecto.Date.from_erl
  # end
  #
  # defp decoder(%TypeInfo{sender: sender}, :binary, default, param) when sender in ["time", "timetz"] do
  #   default.(param)
  #   |> Ecto.Time.from_erl
  # end

  defp decoder(_type, _format, default, param) do
    default.(param)
  end


  # defp encoder(_type, default, %Ecto.Interval{} = interval) do
  #   mon = interval.year * 12 + interval.month
  #   day = interval.day
  #   sec = interval.hour * 3600 + interval.min * 60 + interval.sec
  #   default.({mon, day, sec})
  # end
  #
  # defp encoder(_type, default, %Ecto.DateTime{} = datetime) do
  #   Ecto.DateTime.to_erl(datetime)
  #   |> default.()
  # end
  #
  # defp encoder(_type, default, %Ecto.Date{} = date) do
  #   Ecto.Date.to_erl(date)
  #   |> default.()
  # end
  #
  # defp encoder(_type, default, %Ecto.Time{} = time) do
  #   Ecto.Time.to_erl(time)
  #   |> default.()
  # end
  #
  defp encoder(_type, default, param) do
    default.(param)
  end

  defp prepare_start(repo, opts) do
    pool_name = repo.__riak__(:pool_name)
    {pool_opts, worker_opts} = Dict.split(opts, [:size, :max_overflow])

    pool_opts = pool_opts
      |> Keyword.update(:size, 5, &String.to_integer(&1))
      |> Keyword.update(:max_overflow, 10, &String.to_integer(&1))

    pool_opts = [
      name: {:local, pool_name},
      worker_module: RiakAdapter.Worker ] ++ pool_opts

    # TODO accomodate riakc options
    worker_opts = worker_opts
      |> Keyword.put_new(:port, @default_port)

    {pool_opts, worker_opts}
  end


  defp repo_pool(repo) do
    pid = repo.__riak__(:pool_name) |> Process.whereis

    if is_nil(pid) or not Process.alive?(pid) do
      raise ArgumentError, message: "repo #{inspect repo} is not started"
    end

    pid
  end


  defp use_worker(pool, timeout, fun) do
    key = {:ecto_transaction_pid, pool}

    if value = Process.get(key) do
      in_transaction = true
      worker = elem(value, 0)
    else
      worker = :poolboy.checkout(pool, true, timeout)
    end

    try do
      fun.(worker)
    after
      if !in_transaction do
        :poolboy.checkin(pool, worker)
      end
    end
  end


  # defp checkout_worker(pool, timeout) do
  #   key = {:ecto_transaction_pid, pool}
  #
  #   case Process.get(key) do
  #     {worker, counter} ->
  #       Process.put(key, {worker, counter + 1})
  #       worker
  #     nil ->
  #       worker = :poolboy.checkout(pool, true, timeout)
  #       Worker.monitor_me(worker)
  #       Process.put(key, {worker, 1})
  #       worker
  #   end
  # end
  #
  # defp checkin_worker(pool) do
  #   key = {:ecto_transaction_pid, pool}
  #
  #   case Process.get(key) do
  #     {worker, 1} ->
  #       Worker.demonitor_me(worker)
  #       :poolboy.checkin(pool, worker)
  #       Process.delete(key)
  #     {worker, counter} ->
  #       Process.put(key, {worker, counter - 1})
  #   end
  #   :ok
  # end

end
