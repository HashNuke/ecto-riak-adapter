defmodule Ecto.Adapters.Riak.Error do
  # We either use our own custom error message
  # OR we use what Riak gives us
  defexception [:message, :riak]


  def message(e) do
    if e.message do
      e.message
    else
      case e.riak do
        error when is_binary(error) ->
          error
        {:tcp, tcp_error} -> "tcp: #{tcp_error}"
        error -> error
      end
    end
  end

end
