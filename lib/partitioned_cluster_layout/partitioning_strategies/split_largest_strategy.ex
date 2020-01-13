defmodule PartitionedClusterLayout.PartitioningStrategy.SplitLargestStrategy do
  alias PartitionedClusterLayout.PartitioningStrategy
  alias PartitionedClusterLayout.Node

  @behaviour PartitioningStrategy

  @impl true
  @doc false
  def module do
    PartitionMap.SplitLargestStrategy
  end

  @impl true
  @doc false
  def init([nodes, num_partitions]) do
    owners =
      Enum.into(nodes, %{}, fn %Node{name: name} ->
        {name, num_partitions}
      end)

    [owners: owners]
  end

  @impl true
  @doc false
  def add_nodes_args([nodes, num_partitions]) do
    Enum.into(nodes, %{}, fn %Node{name: name} ->
      {name, num_partitions}
    end)
  end

  @impl true
  @doc false
  # def validate_add_node_args(node, _num_partitions) when not is_atom(node) do
  #   {:error, "node name must be an atom"}
  # end

  # def validate_add_node_args(_node, [num_partitions: num_partitions]) when not is_integer(num_partitions) or num_partitions < 1 do
  #   {:error, ":num_partitions must be an integer greater than zero"}
  # end

  def validate_add_node_args(_node, num_partitions: _num_partitions), do: :ok
end
