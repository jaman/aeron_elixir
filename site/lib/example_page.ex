defmodule AeronElixirSite.ExamplePage do
  @moduledoc """
  One example script's page: its description, how to run it, the helpers it
  loads or the examples that load it, and its source with line numbers.

  `render/1` takes the example, its neighbours in the examples table, the
  helper files it loads, the examples that load it when it is a helper, its
  source, and the repository's GitHub address or `nil`.
  """

  use Phoenix.Component

  import AeronElixirSite.Chrome, only: [code: 1, rich: 1]

  alias AeronElixirSite.{Chrome, Docs}

  attr :example, :map, required: true
  attr :helper?, :boolean, required: true
  attr :previous, :map, default: nil
  attr :next, :map, default: nil
  attr :requires, :list, required: true
  attr :used_by, :list, required: true
  attr :source, :string, required: true
  attr :github_url, :string, default: nil

  def render(assigns) do
    assigns =
      assign(assigns,
        highlighted: Docs.highlight(assigns.source),
        line_numbers: line_numbers(assigns.source),
        description: description(assigns.example.shows)
      )

    ~H"""
    <Chrome.document title={"#{@example.title} · aeron_elixir"} description={@description} base="../index.html">
      <main class="wrap example-page">
        <p class="crumbs">
          <a href="../index.html#examples">Examples</a> / {if @helper?, do: @example.file, else: String.slice(@example.file, 0, 2)}
        </p>
        <h1>{@example.title}</h1>
        <p class="sub"><.rich segments={@example.shows} /></p>

        <p :for={helper <- @requires} class="requires">
          Loads <a href={Docs.page_file(helper)}><code>examples/{helper}</code></a> first, the helpers every example shares.
        </p>
        <p :if={@helper?} class="requires">
          Loaded with <code>Code.require_file/2</code> by
          <span :for={{user, index} <- Enum.with_index(@used_by)}>{if index > 0, do: ", "}<a href={Docs.page_file(user.file)}>{user.title}</a></span>.
          It is not run on its own.
        </p>

        <pre :if={!@helper?} class="shell run"><code>mix run examples/{@example.file}</code></pre>

        <div class="listing-head">
          <span class="file">examples/{@example.file}</span>
          <a :if={@github_url} href={"#{@github_url}/blob/main/examples/#{@example.file}"}>View on GitHub</a>
        </div>
        <div class="listing">
          <pre class="gutter" aria-hidden="true">{@line_numbers}</pre>
          <.code html={@highlighted} />
        </div>

        <nav class="pager">
          <a :if={@previous} href={Docs.page_file(@previous.file)}>← {@previous.title}</a>
          <span :if={!@previous}></span>
          <a :if={@next} href={Docs.page_file(@next.file)}>{@next.title} →</a>
        </nav>
      </main>
    </Chrome.document>
    """
  end

  defp line_numbers(source) do
    count = source |> String.trim_trailing("\n") |> String.split("\n") |> length()
    Enum.map_join(1..count, "\n", &Integer.to_string/1)
  end

  defp description(segments), do: Enum.map_join(segments, fn {_kind, text} -> text end)
end
