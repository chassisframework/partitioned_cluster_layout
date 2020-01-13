defmodule PartitionedClusterLayout.VNode do
  @type partition_id :: PartitionedClusterLayout.partition_id()
  @type v_node_number :: PartitionedClusterLayout.v_node_number()
  @type attribute :: PartitionedClusterLayout.attribute()

  defstruct [
    :node_name,
    :partition_id,
    :v_node_number,
    attributes: %{}
  ]

  defimpl BinPacker.Ball do
    def id(%@for{partition_id: partition_id, v_node_number: v_node_number}), do: {partition_id, v_node_number}
    def attribute(%@for{partition_id: partition_id}, :partition_id), do: partition_id
    def attribute(%@for{v_node_number: v_node_number}, :v_node_number), do: v_node_number
    def attribute(%@for{attributes: attributes}, attribute), do: Map.get(attributes, attribute)
  end
end
