# PartitionedClusterLayout

*WORK IN PROGRESS*

PartitionedClusterLayout is an abstract expression of a generic state-partitioned cluster.

The goal of this library is to provide a data structure suitable for synchronizing between cluster members that encapsulates a few important concepts:

- Cluster membership
- Key-to-partition routing via [PartitionMap](https://github.com/chassisframework/partition_map)
- Optimal partition/replica placement via [BinPacker](https://github.com/chassisframework/bin_packer).

Given two of these layout structures, the library can provide diffs and range transition plans for growing and shrinking the cluster in a controlled manner.

Custom partitioning strategies replica placement strategies are supported, see [SimpleStrategy](https://github.com/chassisframework/partitioned_cluster_layout/blob/main/lib/partitioned_cluster_layout/strategies/simple_strategy.ex) for an example.


## Installation

The package can be installed by adding `partitioned_cluster_layout` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:partitioned_cluster_layout, "~> 0.1.0"}
  ]
end
```

The docs can be found at [https://hexdocs.pm/partitioned_cluster_layout](https://hexdocs.pm/partitioned_cluster_layout).

