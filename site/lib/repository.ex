defmodule AeronElixirSite.Repository do
  @moduledoc """
  Facts about the aeron_elixir checkout the site is built from.

  `github_url/1` is the repository's GitHub address: `AERON_ELIXIR_REPO`
  (`owner/name`) when set, otherwise the `origin` remote when it points at
  GitHub, otherwise `nil`. `requirement!/1` is the dependency requirement for the
  version in `mix.exs`, such as `"~> 0.1"` for `0.1.0`.
  """

  @spec github_url(Path.t()) :: String.t() | nil
  def github_url(root) do
    case System.get_env("AERON_ELIXIR_REPO") do
      repo when is_binary(repo) and repo != "" -> "https://github.com/" <> repo
      _unset -> root |> origin_remote() |> github_path() |> github_address()
    end
  end

  @spec requirement!(Path.t()) :: String.t()
  def requirement!(root) do
    [major, minor] =
      Regex.run(~r/version: "(\d+)\.(\d+)\.\d+"/, File.read!(Path.join(root, "mix.exs")), capture: :all_but_first)

    "~> #{major}.#{minor}"
  end

  defp origin_remote(root) do
    case System.cmd("git", ["-C", root, "remote", "get-url", "origin"], stderr_to_stdout: true) do
      {url, 0} -> String.trim(url)
      {_message, _status} -> nil
    end
  end

  defp github_path(nil), do: nil
  defp github_path(url), do: Regex.run(~r{github\.com[:/]([^/]+/[^/]+?)(?:\.git)?$}, url, capture: :all_but_first)

  defp github_address([path]), do: "https://github.com/" <> path
  defp github_address(nil), do: nil
end
