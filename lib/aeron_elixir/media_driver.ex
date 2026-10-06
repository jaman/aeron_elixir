defmodule AeronElixir.MediaDriver do
  @moduledoc """
  Runs an Aeron C media driver as an external OS process owned by an Elixir
  process.

  `start_link/1` spawns the driver binary, waits until its `cnc.dat` exists and
  the driver heartbeat is live, and links the owning process so the driver is
  stopped when the owner exits. `stop/1` terminates the driver and waits for the
  OS process to end. If the driver exits on its own, the owning process stops with
  `{:driver_exited, status}`, so a supervisor can start it again.

  The application starts one of these for its embedded driver (see
  `AeronElixir.DriverConfig`). More can be started as children of any
  supervisor, each with its own directory:

      {AeronElixir.MediaDriver, aeron_dir: "/run/aeron/tenant_a", name: MyApp.TenantADriver}

  Options:

  * `:aeron_dir` (required) — directory for the driver's CnC and log files;
    created if missing and emptied on start. Starting fails with
    `{:driver_already_active, aeron_dir}` while another live driver serves it.
  * `:binary` — path to the driver executable; defaults to the driver bundled
    with this library (`AeronElixir.DriverConfig.bundled_binary/0`).
  * `:env` — extra `AERON_*` environment variables as a keyword or map of strings.
  * `:startup_timeout_ms` — how long to wait for liveness (default 5000).
  * `:remove_directory` — remove `aeron_dir` when the driver stops (default
    `false`).
  * `:sweep_stale` — before starting, remove sibling directories of `aeron_dir`
    whose driver is dead: a `cnc.dat` whose heartbeat is older than 30 seconds,
    or no `cnc.dat` and untouched for 30 seconds (default `false`).

  The driver deletes its files when it shuts down, and the bundled driver also
  shuts down when its owner's VM exits without stopping it.
  """

  use GenServer

  alias AeronElixir.CnC.Reader
  alias AeronElixir.DriverConfig

  @default_startup_timeout_ms 5_000
  @poll_interval_ms 20
  @liveness_timeout_ms 2_000
  @stale_after_ms 30_000

  defstruct [:port, :os_pid, :aeron_dir, :remove_directory?]

  @type option ::
          {:aeron_dir, Path.t()}
          | {:binary, Path.t()}
          | {:env, keyword() | map()}
          | {:startup_timeout_ms, pos_integer()}
          | {:remove_directory, boolean()}
          | {:sweep_stale, boolean()}
          | {:name, GenServer.name()}

  @spec start_link([option()]) :: GenServer.on_start()
  def start_link(opts) do
    {name_opts, init_opts} = Keyword.split(opts, [:name])
    GenServer.start_link(__MODULE__, init_opts, name_opts)
  end

  @doc """
  Stops the driver process and waits for it to exit.
  """
  @spec stop(GenServer.server()) :: :ok
  def stop(server) do
    GenServer.stop(server, :normal)
  end

  @doc """
  Returns whether the driver process is running and responding on its CnC file.
  """
  @spec alive?(GenServer.server()) :: boolean()
  def alive?(server) do
    GenServer.call(server, :alive?)
  end

  @doc """
  Returns the operating-system pid of the driver process.
  """
  @spec os_pid(GenServer.server()) :: pos_integer()
  def os_pid(server) do
    GenServer.call(server, :os_pid)
  end

  @doc """
  Returns the directory the driver is serving.
  """
  @spec aeron_dir(GenServer.server()) :: Path.t()
  def aeron_dir(server) do
    GenServer.call(server, :aeron_dir)
  end

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)
    aeron_dir = Keyword.fetch!(opts, :aeron_dir)
    binary = Keyword.get(opts, :binary, DriverConfig.bundled_binary())
    timeout_ms = Keyword.get(opts, :startup_timeout_ms, @default_startup_timeout_ms)

    sweep_stale_siblings(Keyword.get(opts, :sweep_stale, false), aeron_dir)

    with {:ok, executable} <- resolve_binary(binary),
         :ok <- prepare_directory(aeron_dir, driver_alive?(aeron_dir)),
         {:ok, port, os_pid} <- spawn_driver(executable, aeron_dir, Keyword.get(opts, :env, [])),
         :ok <- await_liveness(aeron_dir, port, timeout_ms) do
      {:ok,
       %__MODULE__{
         port: port,
         os_pid: os_pid,
         aeron_dir: aeron_dir,
         remove_directory?: Keyword.get(opts, :remove_directory, false)
       }}
    else
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call(:alive?, _from, state) do
    {:reply, driver_alive?(state.aeron_dir), state}
  end

  def handle_call(:os_pid, _from, state), do: {:reply, state.os_pid, state}
  def handle_call(:aeron_dir, _from, state), do: {:reply, state.aeron_dir, state}

  @impl true
  def handle_info({port, {:exit_status, status}}, %{port: port} = state) do
    {:stop, {:driver_exited, status}, %{state | port: nil}}
  end

  def handle_info({port, {:data, _output}}, %{port: port} = state), do: {:noreply, state}
  def handle_info({:EXIT, _pid, _reason}, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    terminate_driver(state.os_pid, state.port)
    remove_directory(state.remove_directory?, state.aeron_dir)
  end

  defp resolve_binary(binary) do
    resolve_binary(binary, File.regular?(binary), System.find_executable(binary))
  end

  defp resolve_binary(binary, true, _executable), do: {:ok, Path.expand(binary)}

  defp resolve_binary(_binary, false, executable) when is_binary(executable),
    do: {:ok, executable}

  defp resolve_binary(binary, false, nil), do: {:error, {:driver_binary_not_found, binary}}

  defp prepare_directory(aeron_dir, true), do: {:error, {:driver_already_active, aeron_dir}}

  defp prepare_directory(aeron_dir, false) do
    File.rm_rf!(aeron_dir)
    File.mkdir_p!(aeron_dir)
    :ok
  end

  defp sweep_stale_siblings(false, _aeron_dir), do: :ok

  defp sweep_stale_siblings(true, aeron_dir) do
    base = Path.dirname(aeron_dir)

    base
    |> File.ls()
    |> sibling_names()
    |> Enum.map(&Path.join(base, &1))
    |> Enum.reject(&(&1 == aeron_dir))
    |> Enum.filter(&stale_directory?/1)
    |> Enum.each(&File.rm_rf!/1)
  end

  defp sibling_names({:ok, names}), do: names
  defp sibling_names({:error, _reason}), do: []

  defp stale_directory?(dir) do
    stale_cnc?(File.dir?(dir), Path.join(dir, "cnc.dat"), dir)
  end

  defp stale_cnc?(false, _cnc_path, _dir), do: false

  defp stale_cnc?(true, cnc_path, dir) do
    case Reader.connect(Path.dirname(cnc_path)) do
      {:ok, cnc} -> not Reader.driver_alive?(cnc, now_ms(), @stale_after_ms)
      {:error, _reason} -> untouched_for?(dir, @stale_after_ms)
    end
  end

  defp untouched_for?(dir, age_ms) do
    case File.stat(dir, time: :posix) do
      {:ok, %File.Stat{mtime: mtime}} -> System.os_time(:second) - mtime > div(age_ms, 1000)
      {:error, _reason} -> false
    end
  end

  defp remove_directory(true, aeron_dir), do: File.rm_rf!(aeron_dir)
  defp remove_directory(false, _aeron_dir), do: :ok

  defp spawn_driver(executable, aeron_dir, extra_env) do
    env =
      [
        {"AERON_DIR", aeron_dir},
        {"AERON_DIR_DELETE_ON_START", "true"},
        {"AERON_DIR_DELETE_ON_SHUTDOWN", "true"},
        {"AERON_ELIXIR_OWNER_STDIN", "1"}
      ]
      |> Kernel.++(Enum.map(extra_env, fn {key, value} -> {to_string(key), to_string(value)} end))
      |> Enum.map(fn {key, value} -> {String.to_charlist(key), String.to_charlist(value)} end)

    port =
      Port.open({:spawn_executable, executable}, [
        :binary,
        :exit_status,
        :stderr_to_stdout,
        {:env, env}
      ])

    started_port(port, Port.info(port, :os_pid))
  end

  defp started_port(port, {:os_pid, os_pid}), do: {:ok, port, os_pid}
  defp started_port(_port, nil), do: {:error, :driver_failed_to_start}

  defp await_liveness(aeron_dir, port, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    await_loop(aeron_dir, port, deadline)
  end

  defp await_loop(aeron_dir, port, deadline) do
    receive do
      {^port, {:exit_status, status}} -> {:error, {:driver_exited, status}}
    after
      0 ->
        liveness_step(
          driver_alive?(aeron_dir),
          aeron_dir,
          port,
          deadline,
          System.monotonic_time(:millisecond)
        )
    end
  end

  defp liveness_step(true, _aeron_dir, _port, _deadline, _now), do: :ok

  defp liveness_step(false, _aeron_dir, _port, deadline, now) when now >= deadline,
    do: {:error, :driver_start_timeout}

  defp liveness_step(false, aeron_dir, port, deadline, _now) do
    Process.sleep(@poll_interval_ms)
    await_loop(aeron_dir, port, deadline)
  end

  defp driver_alive?(aeron_dir) do
    with true <- File.exists?(Path.join(aeron_dir, "cnc.dat")),
         {:ok, cnc} <- Reader.connect(aeron_dir) do
      Reader.driver_alive?(cnc, now_ms(), @liveness_timeout_ms)
    else
      _ -> false
    end
  end

  defp now_ms, do: System.system_time(:millisecond)

  defp terminate_driver(_os_pid, nil), do: :ok

  defp terminate_driver(os_pid, port) do
    System.cmd("kill", ["-TERM", Integer.to_string(os_pid)])
    await_exit(port, os_pid, System.monotonic_time(:millisecond) + 5_000)
  end

  defp await_exit(port, os_pid, deadline) do
    receive do
      {^port, {:exit_status, _status}} -> :ok
    after
      100 -> exit_step(port, os_pid, deadline, System.monotonic_time(:millisecond))
    end
  end

  defp exit_step(_port, os_pid, deadline, now) when now >= deadline do
    System.cmd("kill", ["-KILL", Integer.to_string(os_pid)])
    :ok
  end

  defp exit_step(port, os_pid, deadline, _now), do: await_exit(port, os_pid, deadline)
end
