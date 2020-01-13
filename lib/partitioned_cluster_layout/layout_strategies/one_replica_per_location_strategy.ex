defmodule PartitionedClusterLayout.LayoutStrategy.OneVNodePerLocationStrategy do
  alias BinPacker.OnePerGroupConstraint
  alias BinPacker.EqualNumBallAttributePerBinObjective
  alias BinPacker.EqualNumBallsPerBinObjective

  def objectives([]) do
    %{
      EqualNumBallsPerBinObjective => 1,
      {EqualNumBallAttributePerBinObjective, :v_node_number} => 1
    }
  end

  def constraints([]) do
    [
      {OnePerGroupConstraint, [ball_attribute: :partition_id, bin_attribute: :location]}
    ]
  end
end
