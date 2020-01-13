defmodule PartitionedClusterLayout.Node do
  defstruct [
    :name,
    :location,
    attributes: %{}
  ]

  # @type t :: %__MODULE__{}

  def new(name, location, attributes \\ %{}) when is_atom(name) do
    %__MODULE__{name: name, location: location, attributes: attributes}
  end

  defimpl BinPacker.Bin do
    def id(%@for{location: location, name: name}), do: {location, name}
    def attribute(%@for{location: location}, :location), do: location
    def attribute(%@for{attributes: attributes}, attribute), do: Map.get(attributes, attribute)
  end
end
