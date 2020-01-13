defmodule PartitionedClusterLayout.MixProject do
  use Mix.Project

  @version "0.1.0"

  def project do
    [
      app: :partitioned_cluster_layout,
      version: @version,
      elixir: "~> 1.9",
      start_permanent: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env()),
      deps: deps(),
      description: "Abstract partitioned cluster layout with routing, optimized primary/replica placement, cluster diffs and range transition plans.",
      package: package(),
      deps: deps(),
      docs: docs()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:partition_map, "~> 0.1.0"},
      {:bin_packer, "~> 0.1.0"},
      # {:partition_map, path: "../partition_map"},
      # {:bin_packer, path: "../bin_packer"}
      {:ex_doc, ">= 0.0.0", only: :dev, runtime: false}
    ]
  end

  defp elixirc_paths(:dev), do: ["lib", "test/support"]
  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp package do
    [maintainers: ["Michael Shapiro"],
     licenses: ["MIT"],
     links: %{"GitHub": "https://github.com/chassisframework/partitioned_cluster_layout"}]
  end

  defp docs do
    [extras: ["README.md"],
     source_url: "https://github.com/chassisframework/partitioned_cluster_layout",
     source_ref: @version,
     assets: "assets",
     main: "readme"]
  end
end
