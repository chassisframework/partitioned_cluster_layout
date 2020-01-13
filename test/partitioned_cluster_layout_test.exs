defmodule PartitionedClusterLayoutTest do
  use ExUnit.Case

  alias PartitionedClusterLayout.Node
  alias PartitionedClusterLayout.VNode

  test "new/3 returns a layout" do
    nodes = [
      :"a@127.0.0.1",
      :"b@127.0.0.1",
      :"c@127.0.0.1"
    ]

    assert %PartitionedClusterLayout{} = PartitionedClusterLayout.new(nodes: nodes)
  end

  test "add_nodes/2" do
    nodes = [
      :"a@127.0.0.1",
      :"b@127.0.0.1",
      :"c@127.0.0.1"
    ]

    new_nodes = [:"d@127.0.0.1"]

    num_v_nodes = 3
    num_partitions_per_node = 4

    layout =
      PartitionedClusterLayout.new(
        nodes: nodes,
        num_v_nodes: num_v_nodes,
        num_partitions_per_node: num_partitions_per_node
      )

    new_layout = PartitionedClusterLayout.add_nodes(layout, nodes: new_nodes)

    original_node_names = node_names(layout)
    new_node_names = node_names(new_layout)

    assert MapSet.intersection(original_node_names, new_node_names) == MapSet.new(nodes)
    assert MapSet.difference(new_node_names, original_node_names) == MapSet.new(new_nodes)

    original_layout_v_nodes = v_node_ids(layout)
    new_layout_v_nodes = v_node_ids(new_layout)

    assert MapSet.intersection(original_layout_v_nodes, new_layout_v_nodes) == MapSet.new(original_layout_v_nodes)

    num_new_v_nodes =
      new_layout_v_nodes
      |> MapSet.difference(original_layout_v_nodes)
      |> Enum.count()

    assert num_new_v_nodes == length(new_nodes) * num_partitions_per_node * num_v_nodes
  end

  test "partition_id_for_key/2" do
    nodes = [
      :"a@127.0.0.1",
      :"b@127.0.0.1",
      :"c@127.0.0.1"
    ]

    layout = PartitionedClusterLayout.new(nodes: nodes)

    assert is_number(PartitionedClusterLayout.partition_id_for_key(layout, "abc"))
  end

  test "to_map/1" do
    nodes = [
      :"a@127.0.0.1",
      :"b@127.0.0.1",
      :"c@127.0.0.1"
    ]

    map =
      [nodes: nodes, num_v_nodes: 2]
      |> PartitionedClusterLayout.new()
      |> PartitionedClusterLayout.to_map()

    assert is_map(map)

    Enum.each(map, fn {node, v_nodes} ->
      assert %Node{} = node

      Enum.each(v_nodes, fn v_node ->
        assert %VNode{} = v_node
      end)
    end)
  end

  test "nodes/1" do
    nodes = [
      :"a@127.0.0.1",
      :"b@127.0.0.1",
      :"c@127.0.0.1"
    ]

    expected_nodes =
      Enum.map(nodes, fn name ->
        Node.new(name, name)
      end)

    assert [nodes: nodes]
           |> PartitionedClusterLayout.new()
           |> PartitionedClusterLayout.nodes() == expected_nodes
  end

  test "partition_ids/1" do
    nodes = [
      :"a@127.0.0.1",
      :"b@127.0.0.1",
      :"c@127.0.0.1"
    ]

    assert [nodes: nodes]
           |> PartitionedClusterLayout.new()
           |> PartitionedClusterLayout.partition_ids() == [0, 1, 2, 3, 4, 5, 6, 7, 8]
  end

  defp node_names(layout) do
    layout
    |> PartitionedClusterLayout.nodes()
    |> Enum.map(fn %Node{name: name} -> name end)
    |> Enum.into(MapSet.new())
  end

  defp v_node_ids(layout) do
    layout
    |> PartitionedClusterLayout.v_nodes()
    |> Enum.map(&BinPacker.Ball.id/1)
    |> Enum.into(MapSet.new())
  end
end
