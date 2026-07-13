defmodule Ergon.MixProject do
  use Mix.Project

  def project do
    [
      app: :ergon,
      version: "0.1.0",
      elixir: "~> 1.18",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      aliases: aliases(),
      deps: deps(),
      description: description(),
      package: package(),

      # Docs
      name: "Ergon",
      source_url: "https://github.com/byzantine-systems/ergon",
      # homepage_url: "http://YOUR_PROJECT_HOMEPAGE",
      docs: &docs/0
    ]
  end

  defp docs do
    [
      main: "readme",
      # logo: "path/to/logo.png",
      extras: ["README.md"]
    ]
  end

  # Ergon runs as a supervised OTP application: the Repo and the worker
  # DynamicSupervisor are started by Ergon.Application.
  def application do
    [
      extra_applications: [:logger],
      mod: {Ergon.Application, []}
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      {:ecto_sql, "~> 3.12"},
      {:postgrex, "~> 0.19"},
      {:jason, "~> 1.4"},
      {:broadway, "~> 1.3"},
      {:stream_data, "~> 1.0", only: :test},
      {:ex_doc, "~> 0.34", only: :dev, runtime: false, warn_if_outdated: true}
    ]
  end

  # `mix ecto.setup` creates the database and runs Ergon's temporal + property
  # graph migrations in one shot. `mix test` sets the database up first.
  defp aliases do
    [
      "ecto.setup": ["ecto.create", "ecto.migrate"],
      "ecto.reset": ["ecto.drop", "ecto.setup"],
      test: ["ecto.create --quiet", "ecto.migrate --quiet", "test"]
    ]
  end

  defp description do
    "PostgreSQL-native background job and workflow processing, built on " <>
      "PostgreSQL 18/19 temporal tables, SQL/PGQ property graphs, pgmq " <>
      "durable queues, and pg_cron scheduling."
  end

  defp package do
    [
      licenses: ["LGPL-3.0-only"],
      links: %{"GitHub" => "https://github.com/byzantine-systems/ergon"},
      files: ~w(lib priv/queries priv/repo mix.exs README.md)
    ]
  end
end
