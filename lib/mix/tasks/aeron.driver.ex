defmodule Mix.Tasks.Aeron.Driver do
  @shortdoc "Shows the media driver binary and directory this project uses"

  @moduledoc """
  Prints the media driver configuration resolved by `AeronElixir.DriverConfig`:
  the mode, the driver directory and the driver binary with its version.

      mix aeron.driver

  Another program can run the printed binary, or connect to the printed directory
  with `AERON_DIR`. When the directory is private to each running application, the
  task prints the directory that holds them, since each running VM has its own.
  """

  use Mix.Task

  alias AeronElixir.DriverConfig

  @impl true
  def run(_args) do
    Mix.Task.run("compile")
    Mix.Task.run("loadpaths")

    config = DriverConfig.resolve()

    Mix.shell().info("mode:      #{describe_mode(config)}")
    Mix.shell().info("directory: #{describe_directory(config)}")
    Mix.shell().info("binary:    #{config.binary}")
    Mix.shell().info("version:   #{binary_version(config.binary, File.regular?(config.binary))}")
  end

  defp describe_mode(%DriverConfig{mode: :embedded}),
    do: "embedded (started and supervised by the application)"

  defp describe_mode(%DriverConfig{mode: :external}),
    do: "external (run by something else; nothing is started)"

  defp describe_directory(%DriverConfig{private?: true}),
    do: "private to each running application, under #{DriverConfig.private_base()}/os-<VM pid>"

  defp describe_directory(%DriverConfig{aeron_dir: aeron_dir}), do: aeron_dir

  defp binary_version(binary, true) do
    {output, _status} = System.cmd(binary, ["-v"], stderr_to_stdout: true)
    String.trim(output)
  end

  defp binary_version(_binary, false), do: "not found"
end
