defmodule PartitionedClusterLayout.Transition do
  @moduledoc """
    Given two cluster layouts, describes a step-by-step transition from one to the other using
    asynchronous confirmation messages.

    This is essentially a unidirectional, linear state machine. Each step in the transition
    is a new state, and only the a correct confirmation message can advance the transition to
    the next step.

    A step consists of the following:
     - an idempotent action to take, along with some arguments
     - an id that doubles as a confirmation message to advance the transition to the next step

    At present, this approach is tailored to a cluster where a single master node coordinates
    the transition and commits the current transition state via consensus. Additionally,
    confirmation messages should be addressed to the current coordinator to avoid unnecessary
    action repeats in the case of coordination take-over.

    A transition can be executed like so:
      - commit the plan via consensus
      - do the following until the transition is finished:
        - ask the plan for the current step
        - execute the step's action asynchronously
        - await the confirmation message from the async process
        - advance the plan and commit it via consensus

      - upon taking over coordination from another node, simply repeat the current action
        and await the confirmation message. if two confirmation messages are received,
        then discard one with the help of Transition.advance/2.

    Since the coordinating node may fail at any time, the "action" part of a step must be
    idempotent, because we don't know if the node failed before or after initiating the action.

    Given that coordination of the transition could transfer to another node, the new node
    must be able to determine the state of the current action in order to ensure idempotency.
  """

  alias PartitionedClusterLayout, as: Layout
  alias PartitionedClusterLayout.Diff

  # TODO
  @type t :: %__MODULE__{}

  # could be implemented as dual :queues, but since this isn't hot path,
  # i've opted to implement as two lists for simplicity
  defstruct [
    :current, # action | :finished
    completed: [],
    pending: []
  ]

  @doc """
  Creates a new transition plan given the current layout and the target layout.
  """
  def new(%Layout{} = layout, %Layout{} = other_layout) do
    %Diff{
      new_nodes: new_nodes,
      deleted_nodes: deleted_nodes,

      new_partitions: new_partitions,
      deleted_partitions: deleted_partitions,
      resized_partitions: resized_partitions,

      new_v_nodes: new_v_nodes,
      deleted_v_nodes: deleted_v_nodes,
      moved_v_nodes: moved_v_nodes,
    } = Layout.diff(layout, other_layout)

    [current | pending] =
      [
        Enum.map(new_nodes, fn node -> make_step({:add_node, node}) end),

        Enum.map(new_partitions, fn partition -> make_step({:add_partition, partition}) end),
        Enum.map(deleted_partitions, fn partition -> make_step({:remove_partition, partition}) end),
        Enum.map(resized_partitions, fn partition -> make_step({:resize_partition, partition}) end),

        Enum.map(new_v_nodes, fn v_node -> make_step({:add_v_node, v_node}) end),
        Enum.map(moved_v_nodes, fn v_node -> make_step({:move_v_node, v_node}) end),
        Enum.map(deleted_v_nodes, fn v_node -> make_step({:remove_v_node, v_node}) end),

        Enum.map(deleted_nodes, fn node -> make_step({:remove_node, node}) end),
      ]
      |> List.flatten()

    %__MODULE__{
      current: current,
      pending: pending
    }
  end

  @doc """
  Advances the transition if the confirmation message given for the current step is correct.
  """
  def advance(
    %__MODULE__{
      current: {_action, confirmation_message} = step,
      pending: [],
      completed: completed
    } = transition,
    confirmation_message
  ) do
    transition =
      %__MODULE__{
        transition |
        current: :finished,
        completed: [step | completed]
      }

    {:ok, transition}
  end

  def advance(
    %__MODULE__{
      current: {_action, confirmation_message} = step,
      pending: [next | pending],
      completed: completed
    } = transition,
    confirmation_message
  ) do
    transition =
      %__MODULE__{
        transition |
        current: next,
        pending: pending,
        completed: [step | completed]
      }

    {:ok, transition}
  end

  def advance(%__MODULE__{current: {_action, _confirmation_message}}, _wrong_confirmation_message) do
    {:error, :incorrect_confirmation_message}
  end

  def to_list(
    %__MODULE__{
      current: current,
      pending: pending,
      completed: completed
    }) do
    Enum.reverse(completed) ++ [current] ++ pending
  end

  defp make_step(action) do
    {action, :erlang.make_ref()}
  end
end
