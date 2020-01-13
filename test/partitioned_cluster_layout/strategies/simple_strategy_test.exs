defmodule PartitionedClusterLayout.Strategy.SimpleStrategyTest do
  use ExUnit.Case

  alias PartitionedClusterLayout.Node
  alias PartitionedClusterLayout.Strategy.SimpleStrategy
  alias PartitionedClusterLayout.PartitioningStrategy.SplitLargestStrategy
  alias PartitionedClusterLayout.LayoutStrategy.OneVNodePerLocationStrategy

  test "init/1" do
    nodes = [
      Node.new(:"a@127.0.0.1", "dc-1", %{cpu: 10}),
      Node.new(:"b@127.0.0.1", "dc-2", %{cpu: 11}),
      Node.new(:"c@127.0.0.1", "dc-3", %{cpu: 12})
    ]

    num_v_nodes = 3
    num_partitions_per_node = 3

    assert {
      ^nodes,
      {SplitLargestStrategy, [^nodes, ^num_partitions_per_node]},
      {OneVNodePerLocationStrategy, []},
      _private
    } = SimpleStrategy.init(
      nodes: nodes,
      num_v_nodes: num_v_nodes,
      num_partitions_per_node: num_partitions_per_node)
  end

  test "num_v_nodes/2" do
    nodes = [
      Node.new(:"a@127.0.0.1", "dc-1", %{cpu: 10}),
      Node.new(:"b@127.0.0.1", "dc-2", %{cpu: 11}),
      Node.new(:"c@127.0.0.1", "dc-3", %{cpu: 12})
    ]

    num_v_nodes = 3

    {
      _nodes,
      _partitioning_strategy,
      _layout_strategy,
      private
    } = SimpleStrategy.init(nodes: nodes, num_v_nodes: num_v_nodes)

    assert num_v_nodes == SimpleStrategy.num_v_nodes(:some_partiton_id, private)
  end
end
