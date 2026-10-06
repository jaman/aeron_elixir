defmodule AeronElixirSite.Builder do
  @moduledoc """
  Writes the static site for an aeron_elixir checkout.

  `build!/2` reads the checkout at `root` (benchmark results, README, examples)
  and writes `index.html` and one page per example and helper under
  `examples/` in `out`. Every page is a single file with its styles and
  scripts inlined. Example pages from an earlier build are removed first, so a
  deleted example loses its page. Returns the paths written. Raises when a
  results file, the README tables or an example is missing or malformed.
  """

  alias AeronElixirSite.{Docs, ExamplePage, Explorer, Highlights, IndexPage, Repository, Results}

  @spec build!(Path.t(), Path.t()) :: [Path.t()]
  def build!(root, out) do
    examples = Docs.examples!(root)
    helpers = Docs.helpers!(root)
    examples_out = Path.join(out, "examples")

    File.mkdir_p!(examples_out)
    examples_out |> Path.join("*.html") |> Path.wildcard() |> Enum.each(&File.rm!/1)

    [write!(Path.join(out, "index.html"), index(root, examples))] ++
      example_pages(root, examples, helpers, examples_out) ++
      helper_pages(root, examples, helpers, examples_out)
  end

  defp index(root, examples) do
    results = Results.load!(Path.join(root, "bench/results"))

    render(&IndexPage.render/1, %{
      results: results,
      highlights: Highlights.all(results),
      examples: examples,
      mapping: Docs.pyaeron_mapping!(root),
      requirement: Repository.requirement!(root),
      explorer: Explorer.tabs(results)
    })
  end

  defp example_pages(root, examples, helpers, examples_out) do
    helper_files = Enum.map(helpers, & &1.file)
    neighbours = Enum.zip([[nil | examples], examples, Enum.drop(examples, 1) ++ [nil]])

    for {previous, example, next} <- neighbours do
      source = read_example!(root, example.file)

      page(examples_out, example, %{
        helper?: false,
        previous: previous,
        next: next,
        requires: source |> Docs.required_files() |> Enum.filter(&(&1 in helper_files)),
        used_by: [],
        source: source,
        github_url: Repository.github_url(root)
      })
    end
  end

  defp helper_pages(root, examples, helpers, examples_out) do
    for helper <- helpers do
      page(examples_out, helper, %{
        helper?: true,
        requires: [],
        used_by: Docs.users_of(root, examples, helper.file),
        source: read_example!(root, helper.file),
        github_url: Repository.github_url(root)
      })
    end
  end

  defp page(examples_out, example, assigns),
    do:
      write!(
        Path.join(examples_out, Docs.page_file(example.file)),
        render(&ExamplePage.render/1, Map.put(assigns, :example, example))
      )

  defp render(component, assigns), do: component.(Map.put(assigns, :__changed__, nil))

  defp read_example!(root, file), do: File.read!(Path.join([root, "examples", file]))

  defp write!(path, rendered) do
    File.write!(path, rendered |> Phoenix.HTML.Safe.to_iodata() |> IO.iodata_to_binary())
    path
  end
end
