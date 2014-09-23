defmodule Mix.Tasks.Riak.Index.Delete do
  use Mix.Task

  @shortdoc "Deletes the search index for the repo"

  @moduledoc """
  Deletes the search index for the given repository.

  ## Examples

  To delete a search index with the default name (app's name in mix.exs) or
  the index mentioned in the repo's config

      mix riak.index.delete Repo

  To delete a search index with a custom name

      mix riak.index.delete Repo animals
  """

  def run([repo_module_name]) do
    repo = Module.concat [repo_module_name]
    schema_name = repo.conf[:search_schema] || Mix.Project.config[:app]
    repo.delete_search_index("#{schema_name}")
  end


  def run([repo_module_name, schema_name]) do
    repo = Module.concat [repo_module_name]
    repo.delete_search_index("#{schema_name}")
  end

end
