defmodule AeronElixirSite.Chrome do
  @moduledoc """
  The parts every page shares: the document with its inlined styles and scripts,
  the top navigation, the footer, highlighted code blocks and text with inline
  code.

  `document/1` takes `base`, the page the section anchors belong to: `""` on the
  overview page and `"../index.html"` on an example page, and `script`, extra
  JavaScript inlined at the end of the body.
  """

  use Phoenix.Component

  @assets Path.expand("../assets", __DIR__)
  @external_resource Path.join(@assets, "site.css")
  @external_resource Path.join(@assets, "theme.js")
  @external_resource Path.join(@assets, "nav.js")
  @css File.read!(Path.join(@assets, "site.css"))
  @theme_script File.read!(Path.join(@assets, "theme.js"))
  @nav_script File.read!(Path.join(@assets, "nav.js"))
  @sections [
    {"strengths", "Strengths"},
    {"api", "API"},
    {"log", "How it works"},
    {"benchmarks", "Benchmarks"},
    {"pyaeron", "From pyaeron"},
    {"examples", "Examples"},
    {"start", "Quick start"},
    {"driver", "Driver"}
  ]
  @themes [{"auto", "Auto"}, {"light", "Light"}, {"dark", "Dark"}]
  @icon "data:image/svg+xml," <>
          URI.encode(
            ~s(<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 32 32"><path d="M16 4a12 12 0 1 1-12 12V4z" fill="#4a3aa7"/></svg>),
            &URI.char_unreserved?/1
          )

  attr(:title, :string, required: true)
  attr(:description, :string, required: true)
  attr(:base, :string, required: true)
  attr(:script, :string, default: nil)
  slot(:inner_block, required: true)

  def document(assigns) do
    assigns =
      assign(assigns,
        css: @css,
        theme_script: @theme_script,
        nav_script: @nav_script,
        icon: @icon
      )

    ~H"""
    <!DOCTYPE html>
    <html lang="en">
      <head>
        <meta charset="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <title>{@title}</title>
        <meta name="description" content={@description} />
        <link rel="icon" href={@icon} />
        <link rel="preconnect" href="https://fonts.googleapis.com" />
        <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />
        <link
          rel="stylesheet"
          href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500&family=IBM+Plex+Sans:wght@400;500;600;700&display=swap"
        />
        {Phoenix.HTML.raw("<script>" <> @theme_script <> "</script>")}
        {Phoenix.HTML.raw("<style>" <> @css <> "</style>")}
      </head>
      <body>
        <.nav base={@base} />
        {render_slot(@inner_block)}
        <.footer />
        {Phoenix.HTML.raw("<script>" <> @nav_script <> "</script>")}
        {if @script, do: Phoenix.HTML.raw("<script>" <> @script <> "</script>")}
      </body>
    </html>
    """
  end

  attr(:base, :string, required: true)

  def nav(assigns) do
    assigns = assign(assigns, sections: @sections, themes: @themes)

    ~H"""
    <header class="nav">
      <div class="wrap nav-inner">
        <a href={@base <> "#top"} class="brand"><span class="mark" aria-hidden="true"></span>aeron_elixir</a>
        <nav aria-label="Sections">
          <a :for={{id, label} <- @sections} href={@base <> "#" <> id} data-section={id}>{label}</a>
        </nav>
        <div class="theme-menu" data-theme-menu>
          <button
            type="button"
            class="theme-toggle"
            aria-haspopup="menu"
            aria-expanded="false"
            aria-controls="theme-options"
            aria-label="Colour theme"
            title="Colour theme"
          >
            <.theme_icon :for={{choice, _label} <- @themes} choice={choice} />
          </button>
          <div class="theme-options" id="theme-options" role="menu" aria-label="Colour theme" hidden>
            <button
              :for={{choice, label} <- @themes}
              type="button"
              role="menuitemradio"
              aria-checked="false"
              data-theme-choice={choice}
            >
              <.theme_icon choice={choice} />{label}
              <svg class="theme-check" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><polyline points="20 6 9 17 4 12" /></svg>
            </button>
          </div>
        </div>
      </div>
    </header>
    """
  end

  attr(:choice, :string, required: true)

  defp theme_icon(%{choice: "auto"} = assigns) do
    ~H"""
    <svg class="theme-icon theme-icon-auto" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" aria-hidden="true"><circle cx="12" cy="12" r="8.5" /><path d="M12 3.5a8.5 8.5 0 0 1 0 17z" fill="currentColor" stroke="none" /></svg>
    """
  end

  defp theme_icon(%{choice: "light"} = assigns) do
    ~H"""
    <svg class="theme-icon theme-icon-light" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" aria-hidden="true"><circle cx="12" cy="12" r="4" /><path d="M12 2.5v2M12 19.5v2M2.5 12h2M19.5 12h2M5.3 5.3l1.4 1.4M17.3 17.3l1.4 1.4M5.3 18.7l1.4-1.4M17.3 6.7l1.4-1.4" /></svg>
    """
  end

  defp theme_icon(%{choice: "dark"} = assigns) do
    ~H"""
    <svg class="theme-icon theme-icon-dark" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linejoin="round" aria-hidden="true"><path d="M20 14.5A8.5 8.5 0 0 1 9.5 4a8.5 8.5 0 1 0 10.5 10.5z" /></svg>
    """
  end

  def footer(assigns) do
    ~H"""
    <footer class="wrap footer">
      <span class="brand"><span class="mark" aria-hidden="true"></span>aeron_elixir</span>
      <span>Aeron, without leaving the BEAM.</span>
    </footer>
    """
  end

  attr(:html, :string, required: true)

  def code(assigns) do
    ~H"""
    <div class="code">{Phoenix.HTML.raw(@html)}</div>
    """
  end

  attr(:segments, :list, required: true)

  def rich(assigns) do
    ~H"""
    <%= for {kind, text} <- @segments do %><code :if={kind == :code}>{text}</code><span :if={kind == :text}>{text}</span><% end %>
    """
  end
end
