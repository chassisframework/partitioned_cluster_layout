defmodule PartitionedClusterLayout.Util do
  alias PartitionedClusterLayout.Node

  @doc false
  def maybe_names_to_nodes(list) do
    Enum.map(list, fn
      %Node{} = node ->
        node

      name when is_atom(name) ->
        Node.new(name, name)
    end)
  end
end
