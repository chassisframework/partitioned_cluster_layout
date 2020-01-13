#TODO: needs way better testing

defmodule PartitionedClusterLayout.DiffTest do
  use ExUnit.Case

  alias PartitionedClusterLayout.Node
  alias PartitionedClusterLayout.Diff
  alias PartitionedClusterLayout.Diff.NewPartition
  alias PartitionedClusterLayout.Diff.DeletedPartition
  alias PartitionedClusterLayout.VNode

  test "diff/2" do
    nodes = [
      :"a@127.0.0.1",
      :"b@127.0.0.1",
      :"c@127.0.0.1"
    ]

    new_nodes = [:"d@127.0.0.1"]

    num_v_nodes = 2
    num_partitions_per_node = 2

    layout =
      PartitionedClusterLayout.new(
        nodes: nodes,
        num_v_nodes: num_v_nodes,
        num_partitions_per_node: num_partitions_per_node
      )

    new_layout = PartitionedClusterLayout.add_nodes(layout, nodes: new_nodes)

    assert %Diff{
      new_nodes: [
        %Node{
          name: :"d@127.0.0.1"
        }
      ],
      new_partitions: [
        %NewPartition{
          id: 6,
          ranges: %{4 => 3221225470..3579139412},
          v_nodes: [
            %VNode{
              node_name: :"a@127.0.0.1",
              partition_id: 6,
              v_node_number: 1
            },
            %VNode{
              node_name: :"d@127.0.0.1",
              partition_id: 6,
              v_node_number: 0
            }
          ]
        },
        %NewPartition{
          id: 7,
          ranges: %{5 => 3937053353..4294967295},
          v_nodes: [
            %VNode{
              node_name: :"b@127.0.0.1",
              partition_id: 7,
              v_node_number: 1
            },
            %VNode{
              node_name: :"d@127.0.0.1",
              partition_id: 7,
              v_node_number: 0
            }
          ]
        }
      ]
    } = PartitionedClusterLayout.diff(layout, new_layout)

    assert %Diff{
      deleted_nodes: [
        %Node{
          name: :"d@127.0.0.1"
        }
      ],
      deleted_partitions: [
        %DeletedPartition{
          id: 6,
          ranges: %{4 => 3221225470..3579139412},
          v_nodes: [
            %VNode{
              node_name: :"a@127.0.0.1",
              partition_id: 6,
              v_node_number: 1
            },
            %VNode{
              node_name: :"d@127.0.0.1",
              partition_id: 6,
              v_node_number: 0
            }
          ]
        },
        %DeletedPartition{
          id: 7,
          ranges: %{5 => 3937053353..4294967295},
          v_nodes: [
            %VNode{
              node_name: :"b@127.0.0.1",
              partition_id: 7,
              v_node_number: 1
            },
            %VNode{
              node_name: :"d@127.0.0.1",
              partition_id: 7,
              v_node_number: 0
            }
          ]
        }
      ]
    } = PartitionedClusterLayout.diff(new_layout, layout)
  end
end
