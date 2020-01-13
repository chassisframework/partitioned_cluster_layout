defmodule PartitionedClusterLayout do
  @moduledoc """
  Documentation for PartitionedClusterLayout.
  """

  alias PartitionMap.Partition
  alias PartitionedClusterLayout.Diff
  alias PartitionedClusterLayout.Node
  alias PartitionedClusterLayout.PartitioningStrategy
  alias PartitionedClusterLayout.VNode
  alias PartitionedClusterLayout.Strategy.SimpleStrategy
  alias PartitionedClusterLayout.Transition

  @type partition_id :: PartitionMap.partition_id()
  @type v_node_number :: pos_integer
  @type attribute :: atom

  # TODO
  @type t :: %__MODULE__{}

  defstruct [
    :partition_map,
    :machine_assignment,
    :strategy,
    :strategy_private,
    :partitioning_strategy,

    # computed from machine assignment
    # node -> [v_nodes]
    :map,
    # partition id -> [node]
    :node_map,
    # partition id -> [v_node]
    :v_node_map,
    # partition id -> key range
    :range_map
  ]

  def new(strategy \\ SimpleStrategy, strategy_args) when is_atom(strategy) do
    {
      nodes,
      {partitioning_strategy, partitioning_strategy_args},
      {layout_strategy, layout_strategy_args},
      strategy_private
    } = strategy.init(strategy_args)

    partition_map = PartitioningStrategy.new(partitioning_strategy, partitioning_strategy_args)

    v_nodes =
      partition_map
      |> PartitionMap.to_list()
      |> Enum.map(fn %Partition{id: id} -> id end)
      |> v_nodes_for_new_partition_ids(strategy, strategy_private)

    machine_assignment =
      nodes
      |> BinPacker.new(
        v_nodes,
        layout_strategy.objectives(layout_strategy_args),
        constraints: layout_strategy.constraints(layout_strategy_args)
      )
      |> BinPacker.search()

    %__MODULE__{
      partition_map: partition_map,
      machine_assignment: machine_assignment,
      strategy: strategy,
      strategy_private: strategy_private,
      partitioning_strategy: partitioning_strategy
    }
    |> update_maps()
  end

  @doc """
  TODO: docs

  Note that adding a node does not perform a bin packing optimizaiton, new v_nodes are placed
  on the lowest cost node.
  """
  def add_nodes(
        %__MODULE__{
          partition_map: partition_map,
          machine_assignment: machine_assignment,
          strategy: strategy,
          strategy_private: strategy_private,
          partitioning_strategy: partitioning_strategy,
        } = layout,
        args
      ) do

    {nodes, partition_map_args} = strategy.add_nodes_args(args, strategy_private)

    new_partition_map = PartitioningStrategy.add_nodes(partitioning_strategy, partition_map, partition_map_args)

    %PartitionMap.Diff{added_ids: new_partition_ids} = PartitionMap.diff(partition_map, new_partition_map)

    v_nodes = v_nodes_for_new_partition_ids(new_partition_ids, strategy, strategy_private)

    new_machine_assignment =
      machine_assignment
      |> BinPacker.add_bins(nodes)
      |> BinPacker.add_balls(v_nodes)

    %__MODULE__{
      layout
      | partition_map: new_partition_map,
        machine_assignment: new_machine_assignment
    }
    |> update_maps()
  end

  def to_map(%__MODULE__{map: map}) do
    map
  end

  def partition_id_for_key(%__MODULE__{partition_map: partition_map}, key) do
    %Partition{id: partition_id} = PartitionMap.get(partition_map, key)

    partition_id
  end

  def partition_id_for_digested_key(%__MODULE__{partition_map: partition_map}, digested_key) do
    %Partition{id: partition_id} = PartitionMap.get_with_digested_key(partition_map, digested_key)

    partition_id
  end

  # later, when we support ordered range (as opposed to hashed) partition maps, we'll need to pass
  # the partition map to PartitionMap.digest_key() so it can determine the correct way to digest the key
  def digest_key(%__MODULE__{}, key) do
    PartitionMap.digest_key(key)
  end

  def v_nodes_by_partition_id(%__MODULE__{v_node_map: v_node_map}) do
    v_node_map
  end

  def ranges_by_partition_id(%__MODULE__{range_map: range_map}) do
    range_map
  end

  def nodes(%__MODULE__{} = layout) do
    layout
    |> to_map()
    |> Map.keys()
  end

  def nodes_for_partition_id(%__MODULE__{node_map: node_map}, partition_id) do
    case Map.fetch(node_map, partition_id) do
      {:ok, nodes} ->
        nodes

      :error ->
        []
    end
  end

  def v_nodes(%__MODULE__{} = layout) do
    layout
    |> to_map()
    |> Map.values()
    |> List.flatten()
  end

  def partition_ids(%__MODULE__{v_node_map: v_node_map}) do
    Map.keys(v_node_map)
  end

  defdelegate diff(layout, other_layout), to: Diff
  defdelegate transition(layout, other_layout), to: Transition, as: :new

  defp update_maps(%__MODULE__{machine_assignment: machine_assignment, partition_map: partition_map} = layout) do
    map =
      machine_assignment
      |> BinPacker.to_map()
      |> Enum.into(%{}, fn {%Node{name: name} = node, v_nodes} ->
        v_nodes = Enum.map(v_nodes, fn v_node -> %VNode{v_node | node_name: name} end)

        {node, v_nodes}
      end)

    v_node_map =
      map
      |> Map.values()
      |> List.flatten()
      |> Enum.group_by(fn %VNode{partition_id: partition_id} -> partition_id end)

    node_map =
      map
      |> Enum.flat_map(fn {node, v_nodes} ->
        Enum.map(v_nodes, fn %VNode{partition_id: partition_id} -> {node, partition_id} end)
      end)
      |> Enum.uniq()
      |> Enum.group_by(fn {_node, partition_id} -> partition_id end,
                       fn {node, _partition_id} -> node end)

      range_map =
        partition_map
        |> PartitionMap.to_list()
        |> Enum.into(%{}, fn %Partition{id: id, left: left, right: right} -> {id, {left, right}} end)

    %__MODULE__{layout | map: map, v_node_map: v_node_map, node_map: node_map, range_map: range_map}
  end

  defp v_nodes_for_new_partition_ids(partition_ids, strategy, strategy_private) do
    Enum.flat_map(partition_ids, fn partition_id ->
      num_v_nodes = strategy.num_v_nodes(partition_id, strategy_private)

      Enum.map(0..(num_v_nodes - 1), fn v_node_num ->
        strategy.make_v_node(partition_id, v_node_num, strategy_private)
      end)
    end)
  end

  # defimpl Inspect do
  #   import Inspect.Algebra

  #   def inspect(_state, _opts) do
  #     concat(["##{__MODULE__}}<", "fixme", ">"])
  #   end
  # end
end
