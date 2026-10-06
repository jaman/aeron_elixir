defmodule Mix.Tasks.Compile.AeronNative do
  @moduledoc """
  Builds the native parts of aeron_elixir with the system C compiler: the NIF
  into `priv/lib/aeron_elixir_nif.so` and the bundled Aeron C media driver into
  `priv/bin/aeronmd`, both under the application's build directory
  (`Mix.Project.app_path/0`), where `:code.priv_dir/1` finds them.

  The compiler is `$CC`, or `cc` when it is unset. The NIF is compiled from
  `c_src/aeron_elixir_nif.c` against the running VM's `erl_nif.h`. The
  driver is compiled from the Aeron client and driver C sources that
  `Mix.Tasks.Compile.AeronNative.AeronSources` provides, downloading them on the
  first build, together with `c_src/aeron_driver/owner_watch.c`; its optional
  platform features are detected by compiling small probe programs, the same
  checks the upstream CMake build makes.

  Each output is rebuilt only when it is missing or older than any of its
  sources or this task. Runs after the Elixir compiler; add it with
  `compilers: Mix.compilers() ++ [:aeron_native]`.
  """

  use Mix.Task.Compiler

  alias Mix.Tasks.Compile.AeronNative.AeronSources

  @task_file __ENV__.file
  @driver_native_dir "c_src/aeron_driver"
  @nif_source "c_src/aeron_elixir_nif.c"

  @probes [
    {"HAVE_POLL", "#include <poll.h>\nint main(void) { return poll(0, 0, 0); }\n"},
    {"HAVE_EPOLL", "#include <sys/epoll.h>\nint main(void) { return epoll_create(1); }\n"},
    {"HAVE_STRUCT_MMSGHDR",
     "#define _GNU_SOURCE\n#include <sys/socket.h>\nint main(void) { struct mmsghdr m; (void)m; return 0; }\n"},
    {"HAVE_RECVMMSG",
     "#define _GNU_SOURCE\n#include <sys/socket.h>\nint main(void) { return recvmmsg(0, 0, 0, 0, 0); }\n"},
    {"HAVE_SENDMMSG",
     "#define _GNU_SOURCE\n#include <sys/socket.h>\nint main(void) { return sendmmsg(0, 0, 0, 0); }\n"},
    {"HAVE_FALLOCATE",
     "#define _GNU_SOURCE\n#include <fcntl.h>\nint main(void) { return fallocate(0, 0, 0, 0); }\n"},
    {"HAVE_POSIX_FALLOCATE",
     "#include <fcntl.h>\nint main(void) { return posix_fallocate(0, 0, 0); }\n"},
    {"HAVE_F_PREALLOCATE", "#include <fcntl.h>\nint main(void) { return F_PREALLOCATE; }\n"},
    {"HAVE_ARC4RANDOM", "#include <stdlib.h>\nint main(void) { return (int)arc4random(); }\n"},
    {"HAVE_POSIX_MEMALIGN",
     "#include <stdlib.h>\nint main(void) { void *p; return posix_memalign(&p, 64, 64); }\n"},
    {"HAVE_REALLOCF", "#include <stdlib.h>\nint main(void) { return reallocf(0, 1) == 0; }\n"}
  ]

  @uuid_probe "#include <uuid/uuid.h>\nint main(void) { uuid_t id; uuid_generate(id); return 0; }\n"

  @impl true
  def run(_args) do
    [build_nif(), build_driver()]
    |> Enum.reduce({:noop, []}, &combine/2)
  end

  defp combine({:error, diagnostics}, {_status, acc}), do: {:error, acc ++ diagnostics}
  defp combine(_result, {:error, acc}), do: {:error, acc}
  defp combine({:ok, diagnostics}, {_status, acc}), do: {:ok, acc ++ diagnostics}
  defp combine({:noop, diagnostics}, {status, acc}), do: {status, acc ++ diagnostics}

  defp build_nif do
    build_when_stale(
      Mix.Utils.stale?([@task_file, @nif_source], [nif_output()]),
      "NIF",
      nif_output(),
      fn ->
        compile(nif_args())
      end
    )
  end

  defp build_driver, do: build_driver_from(AeronSources.fetch())

  defp build_driver_from({:error, message}), do: failure("media driver", message)

  defp build_driver_from({:ok, root}) do
    sources = driver_sources(root)

    build_when_stale(
      Mix.Utils.stale?([@task_file, sources_module_file() | sources], [driver_output()]),
      "media driver",
      driver_output(),
      fn ->
        probe_dir = Path.join(Mix.Project.build_path(), "aeron_driver_probes")
        File.mkdir_p!(probe_dir)
        {defines, libraries} = detect_features(probe_dir)
        compile(driver_args(root, sources, defines, libraries))
      end
    )
  end

  defp sources_module_file, do: List.to_string(AeronSources.module_info(:compile)[:source])

  defp nif_output, do: Path.join(Mix.Project.app_path(), "priv/lib/aeron_elixir_nif.so")

  defp driver_output, do: Path.join(Mix.Project.app_path(), "priv/bin/aeronmd")

  defp build_when_stale(false, _label, _output, _build), do: {:noop, []}

  defp build_when_stale(true, label, output, build) do
    File.mkdir_p!(Path.dirname(output))
    build_result(build.(), label, output)
  end

  defp compile(args), do: System.cmd(compiler(), args, stderr_to_stdout: true)

  defp compiler, do: System.get_env("CC", "cc")

  defp nif_args do
    [
      "-std=gnu11",
      "-O3",
      "-fPIC",
      "-Wall",
      "-Wextra",
      "-DNDEBUG",
      "-I#{erts_include_dir()}",
      @nif_source
    ] ++ shared_library_flags(:os.type()) ++ ["-o", nif_output()]
  end

  defp erts_include_dir,
    do: Path.join([:code.root_dir(), "erts-#{:erlang.system_info(:version)}", "include"])

  defp shared_library_flags({:unix, :darwin}), do: ["-dynamiclib", "-undefined", "dynamic_lookup"]
  defp shared_library_flags(_os_type), do: ["-shared"]

  defp driver_sources(root) do
    [AeronSources.client_dir(root), AeronSources.driver_dir(root), @driver_native_dir]
    |> Enum.flat_map(&Path.wildcard(Path.join(&1, "**/*.c")))
    |> Enum.sort()
  end

  defp driver_args(root, sources, defines, libraries) do
    [
      "-std=gnu11",
      "-O3",
      "-DNDEBUG",
      "-DDISABLE_BOUNDS_CHECKS",
      "-D_FILE_OFFSET_BITS=64"
    ] ++
      platform_defines(:os.type()) ++
      version_defines() ++
      Enum.map(defines, &"-D#{&1}") ++
      ["-I#{AeronSources.driver_dir(root)}", "-I#{AeronSources.client_dir(root)}"] ++
      sources ++
      ["-lpthread", "-lm"] ++
      platform_libraries(:os.type()) ++
      libraries ++
      ["-o", driver_output()]
  end

  defp platform_defines({:unix, :darwin}), do: ["-DHAVE_SENDMSG_X", "-DHAVE_RECVMSG_X"]
  defp platform_defines({:unix, :linux}), do: ["-D_DEFAULT_SOURCE"]
  defp platform_defines(_os_type), do: []

  defp platform_libraries({:unix, :linux}), do: ["-ldl"]
  defp platform_libraries(_os_type), do: []

  defp version_defines do
    version = AeronSources.version()
    git_sha = String.slice(AeronSources.git_sha(), 0, 10)
    [major, minor, patch] = version |> String.split(".") |> Enum.take(3)

    [
      "-DAERON_VERSION_TXT=\"#{version}\"",
      "-DAERON_VERSION_MAJOR=#{major}",
      "-DAERON_VERSION_MINOR=#{minor}",
      "-DAERON_VERSION_PATCH=#{patch}",
      "-DAERON_VERSION_GITSHA=\"#{git_sha}\""
    ]
  end

  defp detect_features(probe_dir) do
    defines = for {define, code} <- @probes, probe?(probe_dir, define, code, []), do: define
    {uuid_defines, uuid_libraries} = detect_uuid(probe_dir, :os.type())
    {defines ++ uuid_defines ++ urandom_define(File.exists?("/dev/urandom")), uuid_libraries}
  end

  defp detect_uuid(probe_dir, {:unix, :linux}),
    do: uuid_result(probe?(probe_dir, "HAVE_UUID_GENERATE", @uuid_probe, ["-luuid"]), ["-luuid"])

  defp detect_uuid(probe_dir, _os_type),
    do: uuid_result(probe?(probe_dir, "HAVE_UUID_GENERATE", @uuid_probe, []), [])

  defp uuid_result(true, libraries), do: {["HAVE_UUID_H", "HAVE_UUID_GENERATE"], libraries}
  defp uuid_result(false, _libraries), do: {[], []}

  defp urandom_define(true), do: ["HAVE_DEV_URANDOM"]
  defp urandom_define(false), do: []

  defp probe?(probe_dir, name, code, libraries) do
    source = Path.join(probe_dir, "#{name}.c")
    File.write!(source, code)
    {_output, status} = compile([source | libraries] ++ ["-o", Path.join(probe_dir, name)])
    status == 0
  end

  defp build_result({_output, 0}, label, output) do
    Mix.shell().info("Built the #{label} at #{Path.relative_to_cwd(output)}")
    {:ok, []}
  end

  defp build_result({output, status}, label, _output),
    do: failure(label, "#{compiler()} exit #{status}:\n#{output}")

  defp failure(label, reason) do
    message = "building the #{label} failed: #{reason}"
    Mix.shell().error(message)

    {:error,
     [
       %Mix.Task.Compiler.Diagnostic{
         compiler_name: "aeron_native",
         file: @task_file,
         message: message,
         position: 0,
         severity: :error
       }
     ]}
  end
end
