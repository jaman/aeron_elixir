defmodule Mix.Tasks.Compile.AeronNative.AeronSources do
  @moduledoc """
  Provides the Aeron 1.51.0 C client and media driver sources that
  `Mix.Tasks.Compile.AeronNative` builds the bundled driver from.

  On first use `fetch/0` downloads the 1.51.0 release archive from GitHub,
  checks it against a pinned SHA-512, and unpacks the client and driver C
  sources, `version.txt` and `LICENSE` into `_build/aeron-1.51.0`, which every
  Mix environment of the project shares. Later builds reuse that directory and
  need no network access.

  To build without downloading, set `AERON_SOURCE_DIR` to an unpacked Aeron
  1.51.0 source tree; its `version.txt` must read `1.51.0`.
  """

  @version "1.51.0"
  @git_sha "9773cba37e4b88b2b7eb9460c4e0050267b71d28"
  @archive_url "https://github.com/aeron-io/aeron/archive/refs/tags/#{@version}.tar.gz"
  @archive_sha512 "07416031e022e668dc236971637ecadb62f90ce31ea008da3d30a67553b5dc60" <>
                    "79c2ac2cf631a14d3331fbda09ca1fd70ae9890ca6e1b965e27a802b9be1e9e4"
  @archive_dir "aeron-#{@version}"
  @kept_paths ["aeron-client/src/main/c/", "aeron-driver/src/main/c/", "version.txt", "LICENSE"]

  @doc """
  Returns the Aeron release the sources are pinned to.
  """
  @spec version() :: String.t()
  def version, do: @version

  @doc """
  Returns the Git commit of the pinned Aeron release.
  """
  @spec git_sha() :: String.t()
  def git_sha, do: @git_sha

  @doc """
  Returns the client sources directory under `root`.
  """
  @spec client_dir(Path.t()) :: Path.t()
  def client_dir(root), do: Path.join(root, "aeron-client/src/main/c")

  @doc """
  Returns the driver sources directory under `root`.
  """
  @spec driver_dir(Path.t()) :: Path.t()
  def driver_dir(root), do: Path.join(root, "aeron-driver/src/main/c")

  @doc """
  Returns the root of the Aeron sources, downloading and unpacking them first
  when they are not yet present.

  Returns `{:ok, root}`, or `{:error, message}` when the download fails, the
  archive does not match the pinned checksum, it cannot be unpacked, or
  `AERON_SOURCE_DIR` names a tree of another Aeron version.
  """
  @spec fetch() :: {:ok, Path.t()} | {:error, String.t()}
  def fetch, do: fetch_from(System.get_env("AERON_SOURCE_DIR"))

  defp fetch_from(dir) when dir in [nil, ""], do: cached(cache_dir())
  defp fetch_from(dir), do: dir |> Path.expand() |> check_version()

  defp cache_dir, do: Path.join(Path.dirname(Mix.Project.build_path()), @archive_dir)

  defp cached(dir), do: cached(dir, File.dir?(dir))

  defp cached(dir, true), do: {:ok, dir}
  defp cached(dir, false), do: download(dir)

  defp download(dir) do
    Mix.shell().info("Downloading the Aeron #{@version} sources from #{@archive_url}")

    with {:ok, archive} <- read_archive(),
         {:ok, names} <- source_names(archive),
         :ok <- unpack(archive, names, dir) do
      {:ok, dir}
    end
  end

  defp read_archive do
    @archive_url
    |> Mix.Utils.read_path(sha512: @archive_sha512, timeout: 120_000)
    |> archive_result()
  end

  defp archive_result({:ok, archive}), do: {:ok, archive}
  defp archive_result(:badpath), do: {:error, "#{@archive_url} is not a valid URL"}

  defp archive_result({_kind, message}),
    do: {:error, "downloading #{@archive_url} failed: #{message}"}

  defp source_names(archive) do
    {:binary, archive}
    |> :erl_tar.table([:compressed])
    |> names_result()
  end

  defp names_result({:ok, names}), do: {:ok, Enum.filter(names, &kept?/1)}

  defp names_result({:error, reason}),
    do: {:error, "reading the Aeron archive failed: #{inspect(reason)}"}

  defp kept?(name) do
    relative = name |> List.to_string() |> String.replace_prefix(@archive_dir <> "/", "")
    Enum.any?(@kept_paths, &String.starts_with?(relative, &1))
  end

  defp unpack(archive, names, dir) do
    staging = "#{dir}.partial-#{System.unique_integer([:positive])}"
    File.mkdir_p!(staging)

    {:binary, archive}
    |> :erl_tar.extract([:compressed, {:cwd, String.to_charlist(staging)}, {:files, names}])
    |> unpack_result(staging, dir)
  end

  defp unpack_result(:ok, staging, dir) do
    File.rename!(Path.join(staging, @archive_dir), dir)
    File.rm_rf!(staging)
    :ok
  end

  defp unpack_result({:error, reason}, staging, _dir) do
    File.rm_rf!(staging)
    {:error, "unpacking the Aeron archive failed: #{inspect(reason)}"}
  end

  defp check_version(dir) do
    dir
    |> Path.join("version.txt")
    |> File.read()
    |> version_result(dir)
  end

  defp version_result({:ok, text}, dir), do: matching_version(String.trim(text), dir)

  defp version_result({:error, reason}, dir),
    do:
      {:error,
       "AERON_SOURCE_DIR #{dir} has no readable version.txt: #{:file.format_error(reason)}"}

  defp matching_version(@version, dir), do: {:ok, dir}

  defp matching_version(other, dir),
    do:
      {:error, "AERON_SOURCE_DIR #{dir} holds Aeron #{other}; this build needs Aeron #{@version}"}
end
