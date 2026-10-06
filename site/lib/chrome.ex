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
  @css File.read!(Path.join(@assets, "site.css"))
  @theme_script File.read!(Path.join(@assets, "theme.js"))
  @icon "data:image/svg+xml," <>
          URI.encode(
            ~s(<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 32 32"><path d="M16 4a12 12 0 1 1-12 12V4z" fill="#4a3aa7"/></svg>),
            &URI.char_unreserved?/1
          )

  attr :title, :string, required: true
  attr :description, :string, required: true
  attr :base, :string, required: true
  attr :script, :string, default: nil
  slot :inner_block, required: true

  def document(assigns) do
    assigns = assign(assigns, css: @css, theme_script: @theme_script, icon: @icon)

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
        {if @script, do: Phoenix.HTML.raw("<script>" <> @script <> "</script>")}
      </body>
    </html>
    """
  end

  attr :base, :string, required: true

  def nav(assigns) do
    ~H"""
    <header class="nav">
      <div class="wrap nav-inner">
        <a href={@base <> "#top"} class="brand"><span class="mark" aria-hidden="true"></span>aeron_elixir</a>
        <nav>
          <a href={@base <> "#strengths"}>Strengths</a>
          <a href={@base <> "#api"}>API</a>
          <a href={@base <> "#log"}>How it works</a>
          <a href={@base <> "#benchmarks"}>Benchmarks</a>
          <a href={@base <> "#pyaeron"}>From pyaeron</a>
          <a href={@base <> "#examples"}>Examples</a>
          <a href={@base <> "#start"}>Quick start</a>
          <a href={@base <> "#driver"}>Driver</a>
        </nav>
        <div class="theme" role="group" aria-label="Colour theme">
          <button type="button" data-theme-choice="auto" onclick="aeronSetTheme('auto')">Auto</button>
          <button type="button" data-theme-choice="light" onclick="aeronSetTheme('light')">Light</button>
          <button type="button" data-theme-choice="dark" onclick="aeronSetTheme('dark')">Dark</button>
        </div>
      </div>
    </header>
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

  attr :html, :string, required: true

  def code(assigns) do
    ~H"""
    <div class="code">{Phoenix.HTML.raw(@html)}</div>
    """
  end

  attr :segments, :list, required: true

  def rich(assigns) do
    ~H"""
    <%= for {kind, text} <- @segments do %><code :if={kind == :code}>{text}</code><span :if={kind == :text}>{text}</span><% end %>
    """
  end
end
