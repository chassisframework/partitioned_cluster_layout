defmodule PartitionedClusterLayout.Diff do
  alias PartitionedClusterLayout, as: Layout
  alias PartitionedClusterLayout.Node
  alias PartitionedClusterLayout.VNode
  alias PartitionMap.Diff.Hunk

  defstruct [
    new_nodes: [],
    deleted_nodes: [],

    new_partitions: [],
    deleted_partitions: [],
    resized_partitions: [], # existing partitions that gave keyspace to other existing partitions

    new_v_nodes: [],
    deleted_v_nodes: [],
    moved_v_nodes: []
  ]

  defmodule NewPartition do
    defstruct [
      :id,
      ranges: %{}, # from partition id -> keyspace range
      v_nodes: []
    ]
  end

  defmodule DeletedPartition do
    defstruct [
      :id,
      ranges: %{}, # to partition id -> keyspace range
      v_nodes: []
    ]
  end

  defmodule ResizedPartition do
    defstruct [
      :id,
      ranges: %{}, # from partition id -> keyspace range
    ]
  end

  def diff(
    %Layout{
      partition_map: partition_map,
      machine_assignment: machine_assignment
    } = layout,
    %Layout{
      partition_map: other_partition_map,
      machine_assignment: other_machine_assignment
    } = other_layout
  ) do
    %PartitionMap.Diff{
      added_ids: new_partition_ids,
      deleted_ids: deleted_partition_ids,
      hunks: partition_hunks
    } = PartitionMap.diff(partition_map, other_partition_map)

    %BinPacker.Diff{
      added_bins: new_nodes,
      removed_bins: deleted_nodes,
      added_balls: new_v_nodes_by_node,
      removed_balls: deleted_v_nodes_by_node,
      ball_moves: moved_v_nodes,
    } = BinPacker.diff(machine_assignment, other_machine_assignment)

    {hunks_for_new_partitions, rest} =
      Enum.split_with(partition_hunks, fn %Hunk{to_id: to_id} ->
        Enum.member?(new_partition_ids, to_id)
      end)

    {hunks_for_deleted_partitions, hunks_for_resized_partitions} =
      Enum.split_with(rest, fn %Hunk{from_id: from_id} ->
        Enum.member?(deleted_partition_ids, from_id)
      end)

    new_partitions =
      hunks_for_new_partitions
      |> Enum.group_by(fn %Hunk{to_id: to_id} -> to_id end)
      |> Enum.map(fn {destination_id, hunks} ->
        v_nodes =
          other_layout
          |> PartitionedClusterLayout.v_nodes_by_partition_id()
          |> Map.get(destination_id)

        ranges = Enum.into(hunks, %{}, fn %Hunk{from_id: from_id, left: left, right: right} -> {from_id, left..right} end)

        %NewPartition{
          id: destination_id,
          ranges: ranges,
          v_nodes: v_nodes
        }
      end)

    deleted_partitions =
      hunks_for_deleted_partitions
      |> Enum.group_by(fn %Hunk{from_id: from_id} -> from_id end)
      |> Enum.map(fn {source_id, hunks} ->
        v_nodes =
          layout
          |> PartitionedClusterLayout.v_nodes_by_partition_id()
          |> Map.get(source_id)

        ranges = Enum.into(hunks, %{}, fn %Hunk{to_id: to_id, left: left, right: right} -> {to_id, left..right} end)

        %DeletedPartition{
          id: source_id,
          ranges: ranges,
          v_nodes: v_nodes
        }
      end)

    resized_partitions =
      hunks_for_resized_partitions
      |> Enum.group_by(fn %Hunk{to_id: to_id} -> to_id end)
      |> Enum.map(fn {destination_id, hunks} ->
        ranges = Enum.into(hunks, %{}, fn %Hunk{from_id: from_id, left: left, right: right} -> {from_id, left..right} end)

        %ResizedPartition{
          id: destination_id,
          ranges: ranges
        }
      end)

    new_v_nodes =
      new_v_nodes_by_node
      |> Enum.flat_map(fn {%Node{name: name}, v_nodes} ->
        Enum.map(v_nodes, fn v_node -> %VNode{v_node | node_name: name} end)
      end)
      |> Enum.reject(fn %VNode{partition_id: partition_id} -> Enum.member?(new_partition_ids, partition_id) end)


    deleted_v_nodes =
      deleted_v_nodes_by_node
      |> Enum.flat_map(fn {%Node{name: name}, v_nodes} ->
        Enum.map(v_nodes, fn v_node -> %VNode{v_node | node_name: name} end)
      end)
      |> Enum.reject(fn %VNode{partition_id: partition_id} -> Enum.member?(deleted_partition_ids, partition_id) end)

    %__MODULE__{
      new_nodes: new_nodes,
      deleted_nodes: deleted_nodes,

      new_partitions: new_partitions,
      deleted_partitions: deleted_partitions,
      resized_partitions: resized_partitions,

      new_v_nodes: new_v_nodes,
      deleted_v_nodes: deleted_v_nodes,
      moved_v_nodes: moved_v_nodes
    }
  end
end
