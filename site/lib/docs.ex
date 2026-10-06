defmodule AeronElixirSite.Docs do
  @moduledoc """
  Content read from the aeron_elixir checkout: the examples table in `README.md`,
  the pyaeron API mapping in `examples/README.md`, and example sources rendered
  with syntax highlighting.

  Table cells are returned as segments, `{:text, string}` or `{:code, string}`,
  split on backticks.
  """

  @type segments :: [{:text | :code, String.t()}]
  @type example :: %{file: String.t(), title: String.t(), shows: segments()}

  @spec examples!(Path.t()) :: [example()]
  def examples!(root) do
    root
    |> Path.join("README.md")
    |> table_rows!(~r/^\| `(\d\d_[a-z_]+\.exs)` \| (.+) \|$/m)
    |> Enum.filter(fn [file, _shows] -> File.exists?(Path.join([root, "examples", file])) end)
    |> Enum.map(fn [file, shows] -> %{file: file, title: title(file), shows: segments(shows)} end)
  end

  @spec pyaeron_mapping!(Path.t()) :: [{segments(), segments()}]
  def pyaeron_mapping!(root) do
    [_before, section] =
      root |> Path.join("examples/README.md") |> File.read!() |> String.split("## pyaeron API → aeron_elixir", parts: 2)

    section
    |> String.split("\n")
    |> Enum.filter(&String.starts_with?(&1, "| `"))
    |> Enum.map(&mapping_row/1)
  end

  @doc """
  Returns the file name of an example's page: `"01_hello.exs"` is `"01_hello.html"`.
  """
  @spec page_file(String.t()) :: String.t()
  def page_file(file), do: Path.rootname(file) <> ".html"

  @spec highlight(String.t()) :: String.t()
  def highlight(source),
    do: Makeup.highlight(source, lexer_options: [group_prefix: "g#{:erlang.phash2(source)}"])

  @spec segments(String.t()) :: segments()
  def segments(text) do
    text
    |> String.split("`")
    |> Enum.with_index()
    |> Enum.reject(fn {part, _index} -> part == "" end)
    |> Enum.map(fn {part, index} -> {segment_kind(rem(index, 2)), part} end)
  end

  @helper_titles %{"support.exs" => "Shared helpers"}

  @doc """
  Returns the helper scripts the examples load with `Code.require_file/2`, each
  described by the first paragraph of its `@moduledoc`.
  """
  @spec helpers!(Path.t()) :: [example()]
  def helpers!(root) do
    for {file, title} <- @helper_titles, File.exists?(Path.join([root, "examples", file])) do
      %{
        file: file,
        title: title,
        shows: [root, "examples", file] |> Path.join() |> File.read!() |> moduledoc() |> segments()
      }
    end
  end

  @doc """
  Returns the files `source` loads with `Code.require_file/2`, in order.
  """
  @spec required_files(String.t()) :: [String.t()]
  def required_files(source) do
    ~r/Code\.require_file\("([^"]+)"/
    |> Regex.scan(source, capture: :all_but_first)
    |> List.flatten()
  end

  @doc """
  Returns the examples whose local source loads `helper`.
  """
  @spec users_of(Path.t(), [example()], String.t()) :: [example()]
  def users_of(root, examples, helper) do
    Enum.filter(examples, fn example ->
      helper in ([root, "examples", example.file] |> Path.join() |> File.read!() |> required_files())
    end)
  end

  defp moduledoc(source) do
    case Regex.run(~r/@moduledoc """\n(.*?)\n\s*(?:\n|""")/s, source, capture: :all_but_first) do
      [paragraph] -> paragraph |> String.split() |> Enum.join(" ")
      nil -> ""
    end
  end

  defp table_rows!(path, pattern), do: Regex.scan(pattern, File.read!(path), capture: :all_but_first)

  defp mapping_row(line) do
    [pyaeron, elixir] =
      line
      |> String.trim()
      |> String.trim("|")
      |> String.split(~r/(?<!\\)\|/)
      |> Enum.map(&(&1 |> String.replace("\\|", "|") |> String.trim()))

    {segments(pyaeron), segments(elixir)}
  end

  @acronyms ~w(ipc rpc udp)

  defp title(file) do
    file
    |> String.replace(~r/^\d\d_|\.exs$/, "")
    |> String.split("_")
    |> Enum.map(&title_word/1)
    |> Enum.join(" ")
    |> capitalize_first()
  end

  defp title_word(word) when word in @acronyms, do: String.upcase(word)
  defp title_word(word), do: word

  defp capitalize_first(<<first::utf8, rest::binary>>), do: String.upcase(<<first::utf8>>) <> rest

  defp segment_kind(0), do: :text
  defp segment_kind(1), do: :code
end
