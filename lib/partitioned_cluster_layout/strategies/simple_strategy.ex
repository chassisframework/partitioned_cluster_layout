defmodule PartitionedClusterLayout.Strategy.SimpleStrategy do
  alias PartitionedClusterLayout.VNode
  alias PartitionedClusterLayout.Util
  alias PartitionedClusterLayout.PartitioningStrategy.SplitLargestStrategy
  alias PartitionedClusterLayout.LayoutStrategy.OneVNodePerLocationStrategy

  # @behaviour PartitionedClusterLayout.Strategy

  def init(args) do
    nodes =
      args
      |> Keyword.fetch!(:nodes)
      |> Util.maybe_names_to_nodes()

    num_partitions = Keyword.get(args, :num_partitions_per_node, 3)
    num_v_nodes = Keyword.get(args, :num_v_nodes, 3)
    private = {num_v_nodes, num_partitions}

    {
      nodes,
      {SplitLargestStrategy, [nodes, num_partitions]},
      {OneVNodePerLocationStrategy, []},
      private
    }
  end

  def num_v_nodes(_partition_id, {num_v_nodes, _num_partitions}), do: num_v_nodes

  def add_nodes_args(args, {_num_v_nodes, num_partitions}) do
    nodes =
      args
      |> Keyword.fetch!(:nodes)
      |> Util.maybe_names_to_nodes()

    {nodes, [nodes, num_partitions]}
  end

  def make_v_node(partition_id, v_node_num, _private) do
    %VNode{partition_id: partition_id, v_node_number: v_node_num}
  end
end
