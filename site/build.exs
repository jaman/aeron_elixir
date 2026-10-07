Mix.install([
  {:phoenix_live_view, "~> 1.1"},
  {:makeup_elixir, "~> 1.0"}
])

for module <-
      ~w(format client results chart compare bench highlights explorer docs repository chrome index_page example_page builder) do
  Code.require_file("lib/#{module}.ex", __DIR__)
end

root = Path.expand("..", __DIR__)
out = Path.join(root, "docs")

for path <- AeronElixirSite.Builder.build!(root, out) do
  IO.puts("wrote #{Path.relative_to(path, root)}")
end
