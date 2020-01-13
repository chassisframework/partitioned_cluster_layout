#TODO: property tests
defmodule PartitionedClusterLayout.TransitionTest do
  use ExUnit.Case

  alias PartitionedClusterLayout.Node
  alias PartitionedClusterLayout.Diff.NewPartition
  alias PartitionedClusterLayout.Transition

  setup_all do
    nodes = [
      :"a@127.0.0.1",
      :"b@127.0.0.1",
      :"c@127.0.0.1"
    ]

    new_nodes = [:"d@127.0.0.1"]

    num_v_nodes = 3
    num_partitions_per_node = 2

    layout =
      PartitionedClusterLayout.new(
        nodes: nodes,
        num_v_nodes: num_v_nodes,
        num_partitions_per_node: num_partitions_per_node
      )

    new_layout = PartitionedClusterLayout.add_nodes(layout, nodes: new_nodes)

    [transition: Transition.new(layout, new_layout)]
  end

  test "new/2", %{transition: transition} do
    assert %Transition{
      completed: [],
      current: current,
      pending: [step_2, step_3]
    } = transition

    assert {{:add_node, %Node{name: :"d@127.0.0.1"}}, ref} = current
    assert is_reference(ref)

    assert {{:add_partition, %NewPartition{}}, ref} = step_2
    assert is_reference(ref)

    assert {{:add_partition, %NewPartition{}}, ref} = step_3
    assert is_reference(ref)
  end

  describe "advance/2" do
    test "with the correct confirmation message, advances the transition" , %{transition: transition} do
      assert %Transition{current: {{:add_node, %Node{name: :"d@127.0.0.1"}}, conf_msg}} = transition

      assert {:ok, transition} = Transition.advance(transition, conf_msg)

      assert %Transition{current: {{:add_partition, %NewPartition{id: 6}}, _conf_msg}} = transition
    end

    test "with an incorrect confirmation message, returns an error" , %{transition: transition} do
      assert {:error, :incorrect_confirmation_message} = Transition.advance(transition, :erlang.make_ref())
    end
  end
end
