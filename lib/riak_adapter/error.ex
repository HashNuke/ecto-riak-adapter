defmodule RiakAdapter.Error do
  defexception [:message, :riak]

  def message(e) do
    if kw = e.postgres do
      msg = "#{kw[:severity]} (#{kw[:code]}): #{kw[:message]}"
    end

    msg || e.message
  end
end
