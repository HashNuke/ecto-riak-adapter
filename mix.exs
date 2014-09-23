defmodule EctoRiakAdapter.Mixfile do
  use Mix.Project

  def project do
    [app: :ecto_riak_adapter,
     version: "0.0.1",
     elixir: "~> 1.0.0",
     deps: deps]
  end

  # Configuration for the OTP application
  #
  # Type `mix help compile.app` for more information
  def application do
    [applications: [:logger]]
  end

  # Dependencies can be Hex packages:
  #
  #   {:mydep, "~> 0.3.0"}
  #
  # Or git/path repositories:
  #
  #   {:mydep, git: "https://github.com/elixir-lang/mydep.git", tag: "0.1.0"}
  #
  # Type `mix help deps` for more examples and options
  defp deps do
    # full_deps_path = Path.expand Mix.Project.config[:deps_path]
    # riak_pb_compile_cmd = "./rebar compile skip_deps=true deps_dir=#{inspect full_deps_path}"
    [
      {:ecto,    "~> 0.2.4"},
      {:decimal, "~> 0.2.5"},
      {:poison,  github: "devinus/poison"},
      {:riakc,   github: "basho/riak-erlang-client"},
      {:riak_pb, github: "basho/riak_pb", compile: "./rebar get-deps compile deps_dir=../", override: true}
    ]
  end
end
