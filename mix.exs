defmodule SchemaParser.MixProject do
  use Mix.Project

  def project do
    [
      app: :schema_parser,
      version: "0.1.0",
      elixir: "~> 1.10",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {SchemaParser.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:mssqlex, "~> 1.1.0"}
    ]
  end
end
