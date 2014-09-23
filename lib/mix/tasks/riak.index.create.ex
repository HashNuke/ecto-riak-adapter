defmodule Mix.Tasks.Riak.Index.Create do
  use Mix.Task

  @shortdoc "Creates a search index for the repo"

  @moduledoc """
  Creates a search index for the given repository.

  ## Examples

  To create a search index with the default name (app's name in mix.exs) or
  the index mentioned in the repo's config

      mix riak.index.create Repo

  To create a search index with a custom name

      mix riak.index.create Repo animals
  """

  def run([repo_module_name]) do
    repo = Module.concat [repo_module_name]
    schema_name = repo.conf[:search_schema] || Mix.Project.config[:app]
    repo.create_search_index("#{schema_name}")
  end


  def run([repo_module_name, schema_name]) do
    repo = Module.concat [repo_module_name]
    repo.create_search_index("#{schema_name}")
  end

end
