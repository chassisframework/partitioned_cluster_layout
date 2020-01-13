defmodule PartitionedClusterLayout.PartitioningStrategy do
  @type args :: any
  @type private :: any
  @type nodes_or_node_arg_pairs :: [node | {node, args}]

  @callback module() :: module
  @callback init(args) :: args
  @callback add_nodes_args(args) :: args
  @callback validate_add_node_args(node, args) :: :ok | {:error, any()}

  def new(strategy, args) do
    module = strategy.module
    args = strategy.init(args)

    PartitionMap.new(module, args)
  end

  def add_nodes(strategy, partition_map, args) do
    args = strategy.add_nodes_args(args)

    PartitionMap.add_owners(partition_map, args)
  end
end
