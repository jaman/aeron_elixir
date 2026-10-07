defmodule AeronElixir.MixProject do
  use Mix.Project

  def project do
    [
      app: :aeron_elixir,
      version: "0.1.1",
      elixir: "~> 1.19",
      start_permanent: Mix.env() == :prod,
      compilers: Mix.compilers() ++ [:aeron_native],
      deps: deps(),
      dialyzer: [
        plt_add_apps: [:mix],
        plt_file: {:no_warn, "_build/plts/dialyzer.plt"}
      ],
      aliases: aliases(),
      description:
        "Aeron client for Elixir over IPC and UDP, with the Aeron C media driver built and supervised",
      package: package(),
      source_url: "https://github.com/jaman/aeron_elixir",
      homepage_url: "https://jaman.github.io/aeron_elixir/"
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {AeronElixir.Application, []}
    ]
  end

  defp deps do
    [
      {:ash, "~> 3.0"},
      {:igniter, "~> 0.3"},
      {:benchee, "~> 1.3", only: [:dev, :test], runtime: false},
      {:benchee_json, "~> 1.0", only: [:dev, :test], runtime: false},
      {:benchee_html, "~> 1.0", only: [:dev, :test], runtime: false},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.34", only: :dev, runtime: false}
    ]
  end

  defp package do
    [
      maintainers: ["Jarius Jenkins"],
      files: ~w(lib c_src mix.exs .formatter.exs README.md LICENSE NOTICE),
      licenses: ["Apache-2.0"],
      links: %{
        "GitHub" => "https://github.com/jaman/aeron_elixir",
        "Website" => "https://jaman.github.io/aeron_elixir/"
      }
    ]
  end

  defp aliases do
    [
      quality: ["format", "credo --strict", "dialyzer"],
      "quality.ci": [
        "format --check-formatted",
        "credo --strict",
        "dialyzer"
      ]
    ]
  end
end
