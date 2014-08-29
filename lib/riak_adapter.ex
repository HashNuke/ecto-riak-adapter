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


  # lib/riak_adapter.ex:1: warning: undefined behaviour function all/3 (for behaviour Ecto.Adapter)
  def all(repo, query, opts) do
    pg_query = %{query | select: normalize_select(query.select)}

    {sql, params} = SQL.select(pg_query)
    %Postgrex.Result{rows: rows} = query(repo, sql, params, opts)

    # Transform each row based on select expression
    transformed =
      Enum.map(rows, fn row ->
        values = Tuple.to_list(row)
        transform_row(pg_query.select.expr, values, pg_query.sources) |> elem(0)
      end)

    transformed
    |> Ecto.Associations.Assoc.run(query)
    |> preload(repo, query)
  end

  # lib/riak_adapter.ex:1: warning: undefined behaviour function delete/3 (for behaviour Ecto.Adapter)
  def delete(repo, model, opts) do
    {sql, params} = SQL.delete(model)
    %Postgrex.Result{num_rows: nrows} = query(repo, sql, params, opts)
    nrows
  end


  # lib/riak_adapter.ex:1: warning: undefined behaviour function delete_all/3 (for behaviour Ecto.Adapter)
  def delete_all(repo, query, opts) do
    {sql, params} = SQL.delete_all(query)
    %Postgrex.Result{num_rows: nrows} = query(repo, sql, params, opts)
    nrows
  end


  # lib/riak_adapter.ex:1: warning: undefined behaviour function insert/3 (for behaviour Ecto.Adapter)
  def insert(repo, model, opts) do
    module    = model.__struct__
    returning = module.__schema__(:keywords, model)
      |> Enum.filter(fn {_, val} -> val == nil end)
      |> Keyword.keys

    {sql, params} = SQL.insert(model, returning)

    case query(repo, sql, params, opts) do
      %Postgrex.Result{rows: [values]} ->
        Enum.zip(returning, Tuple.to_list(values))
      _ ->
        []
    end
  end


  # lib/riak_adapter.ex:1: warning: undefined behaviour function storage_up/1 (for behaviour Ecto.Adapter.Storage)
  def storage_up(opts) do
    # TODO: allow the user to specify those options either in the Repo or on command line
    database_options = ~s(TEMPLATE=template0 ENCODING='UTF8' LC_COLLATE='en_US.UTF-8' LC_CTYPE='en_US.UTF-8')

    output = run_with_psql opts, "CREATE DATABASE #{opts[:database]} " <> database_options

    cond do
      String.length(output) == 0                 -> :ok
      String.contains?(output, "already exists") -> {:error, :already_up}
      true                                       -> {:error, output}
    end
  end


  # lib/riak_adapter.ex:1: warning: undefined behaviour function storage_down/1 (for behaviour Ecto.Adapter.Storage)
  def storage_down(opts) do
    output = run_with_psql(opts, "DROP DATABASE #{opts[:database]}")

    cond do
      String.length(output) == 0                 -> :ok
      String.contains?(output, "does not exist") -> {:error, :already_down}
      true                                       -> {:error, output}
    end
  end


  # lib/riak_adapter.ex:1: warning: undefined behaviour function update/3 (for behaviour Ecto.Adapter)
  def update(repo, model, opts) do
    {sql, params} = SQL.update(model)
    %Postgrex.Result{num_rows: nrows} = query(repo, sql, params, opts)
    nrows
  end


  # lib/riak_adapter.ex:1: warning: undefined behaviour function update_all/4 (for behaviour Ecto.Adapter)
  def update_all(repo, query, values, external, opts) do
    {sql, params} = SQL.update_all(query, values, external)
    %Postgrex.Result{num_rows: nrows} = query(repo, sql, params, opts)
    nrows
  end
end
