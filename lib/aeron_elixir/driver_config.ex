defmodule AeronElixir.DriverConfig do
  @moduledoc """
  Decides which media driver this application uses and where its directory is.

  The directory comes from, in order of precedence:

    1. the `AERON_DIR` environment variable;
    2. `config :aeron_elixir, aeron_dir: path`;
    3. neither: a directory private to this running application, described below.

  `config :aeron_elixir, driver: mode` chooses what happens at that directory:

    * `:embedded` (the default when no directory is set) — the application starts
      the driver binary there and supervises it;
    * `:external` (the default when a directory is set) — a driver that something
      else runs, such as another application or a host service; nothing is
      started and clients connect to it.

  `config :aeron_elixir, binary: path` sets the executable for an embedded driver.
  The default is the driver bundled with this library at `priv/bin/aeronmd`, built
  by `mix compile`.

  The private directory is
  `$XDG_RUNTIME_DIR/aeron_elixir/os-<pid>` when `XDG_RUNTIME_DIR` is set, otherwise
  `/dev/shm/aeron_elixir-<user>/os-<pid>` where `/dev/shm` exists, otherwise
  `<system tmp dir>/aeron_elixir-<user>/os-<pid>`, where `<pid>` is the operating
  system pid of this VM. All three are memory-backed or per-user runtime storage;
  the driver recreates its files on every start.

  Configuration examples:

      config :aeron_elixir, aeron_dir: "/run/aeron"

      config :aeron_elixir, driver: :embedded, aeron_dir: "/run/aeron/shared"

      config :aeron_elixir, binary: "/opt/aeron/bin/aeronmd"
  """

  @enforce_keys [:mode, :aeron_dir, :binary, :private?]
  defstruct @enforce_keys

  @type mode :: :embedded | :external
  @type t :: %__MODULE__{
          mode: mode(),
          aeron_dir: Path.t(),
          binary: Path.t(),
          private?: boolean()
        }

  @private_directory_name "aeron_elixir"

  @doc """
  Resolves the driver configuration from the environment and application config.

  Raises `ArgumentError` for an unknown `:driver` mode, or for `driver: :external`
  without a directory.
  """
  @spec resolve() :: t()
  def resolve do
    configured_dir = configured_directory()
    mode = Application.get_env(:aeron_elixir, :driver, default_mode(configured_dir))
    validate!(mode, configured_dir)

    %__MODULE__{
      mode: mode,
      aeron_dir: configured_dir || private_directory(),
      binary: Application.get_env(:aeron_elixir, :binary, bundled_binary()),
      private?: is_nil(configured_dir)
    }
  end

  @doc """
  Returns the driver directory clients connect to when none is given.
  """
  @spec aeron_dir() :: Path.t()
  def aeron_dir, do: resolve().aeron_dir

  @doc """
  Returns the path of the driver binary bundled with this library.
  """
  @spec bundled_binary() :: Path.t()
  def bundled_binary, do: Application.app_dir(:aeron_elixir, "priv/bin/aeronmd")

  @doc """
  Returns the directory that holds the private directory of every running
  application on this host for the current user.
  """
  @spec private_base() :: Path.t()
  def private_base, do: private_base(System.get_env("XDG_RUNTIME_DIR"), File.dir?("/dev/shm"))

  defp configured_directory do
    case System.get_env("AERON_DIR") do
      dir when is_binary(dir) and dir != "" -> dir
      _unset -> Application.get_env(:aeron_elixir, :aeron_dir)
    end
  end

  defp default_mode(nil), do: :embedded
  defp default_mode(_configured_dir), do: :external

  defp validate!(:embedded, _configured_dir), do: :ok
  defp validate!(:external, dir) when is_binary(dir), do: :ok

  defp validate!(:external, nil),
    do:
      raise(
        ArgumentError,
        "driver: :external needs a directory: set AERON_DIR or config :aeron_elixir, :aeron_dir"
      )

  defp validate!(mode, _configured_dir),
    do:
      raise(
        ArgumentError,
        "unknown :driver mode #{inspect(mode)}; expected :embedded or :external"
      )

  defp private_directory, do: Path.join(private_base(), "os-#{System.pid()}")

  defp private_base(runtime_dir, _shm?) when is_binary(runtime_dir) and runtime_dir != "",
    do: Path.join(runtime_dir, @private_directory_name)

  defp private_base(_runtime_dir, true),
    do: Path.join("/dev/shm", "#{@private_directory_name}-#{user()}")

  defp private_base(_runtime_dir, false),
    do: Path.join(System.tmp_dir!(), "#{@private_directory_name}-#{user()}")

  defp user, do: System.get_env("USER") || System.get_env("LOGNAME") || "default"
end
