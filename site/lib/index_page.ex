defmodule AeronElixirSite.IndexPage do
  @moduledoc """
  The aeron_elixir overview page: what the client is good at, its API, how the
  log moves bytes, a benchmark explorer, the pyaeron API mapping, the runnable
  examples, a quick start and the media driver.

  The explorer holds every chart each tab can show, rendered ahead of time; the
  page's script shows the one matching the pressed buttons.
  """

  use Phoenix.Component

  import AeronElixirSite.Chrome, only: [code: 1, rich: 1]

  alias AeronElixirSite.{Chart, Chrome, Docs, Explorer}

  @explorer_script_path Path.expand("../assets/explorer.js", __DIR__)
  @external_resource @explorer_script_path
  @explorer_script File.read!(@explorer_script_path)

  @publish_snippet Docs.highlight(~S"""
                   alias AeronElixir, as: AE

                   {:ok, client} = AE.start_link()

                   channel = AE.ChannelUri.ipc(alias: "orders", term_length: 16_777_216)
                   {:ok, publication} = AE.add_publication(client, channel, 1001)
                   :ok = AE.await_connected(publication)

                   {:ok, _position} = AE.publish(publication, ["order:", "42"])
                   """)

  @subscribe_snippet Docs.highlight(~S"""
                     {:ok, subscription} =
                       AE.add_subscription(client, channel, 1001,
                         available_image: &IO.inspect(&1, label: "joined")
                       )

                     AE.poll(subscription, 10, fn payload, header ->
                       IO.inspect({AE.Header.session_id(header), payload})
                     end)
                     """)

  @batch_snippet Docs.highlight(~S"""
                 {:ok, handle} = AE.publication_handle(publication)
                 {:ok, 3} = AE.publish_list(handle, ["a", "b", "c"])
                 {:ok, 3, ["a", "b", "c"]} = AE.poll_batch(subscription, 64)

                 {:ok, batcher} = AE.BatchPublisher.start_link(publication: publication)
                 :ok = AE.BatchPublisher.publish(batcher, "fire and forget")
                 """)

  @driver_snippet Docs.highlight(~S"""
                  config :aeron_elixir, aeron_dir: "/run/aeron"
                  """)

  @supervise_snippet Docs.highlight(~S"""
                     children = [
                       {AeronElixir, name: MyApp.Aeron},
                       MyApp.OrderPublisher
                     ]

                     Supervisor.start_link(children, strategy: :rest_for_one)
                     """)

  @strengths [
    {"Publish from any process",
     "Any number of processes can publish to the same stream at once. Each message claims its own space in the shared buffer, so nobody waits on a lock or queues behind anyone else."},
    {"Nothing in the middle",
     "Publishing and polling happen right inside the process that calls them. No server process relays your messages, so one busy process never holds up the rest."},
    {"Batches when you need speed",
     "Send a whole list in one call with publish_list/2, or read many messages at once with poll_batch/2. BatchPublisher gathers messages from many processes and sends them together, without making any of them wait."},
    {"Send what you already have",
     "Pass a binary or iodata as it is; nothing has to be joined up first. Large messages are split on send and put back together on receive for you."},
    {"The driver comes with it",
     "The Aeron media driver is built with the library and runs under your application's supervisor, with nothing to install or configure. Point AERON_DIR at a driver that is already running to share it instead."},
    {"Talks to every Aeron client",
     "It speaks the same protocol as Aeron's Java, C and C++ clients and pyaeron, so Elixir services share streams with them on one machine or across the network."}
  ]

  @log_steps [
    {"Publishers",
     "A process calls publish. The message is written into a buffer in shared memory and marked complete only once every byte is in place, so a reader never sees half a message."},
    {"Media driver",
     "A separate program that owns the buffers. On one machine it keeps publishers from getting too far ahead of the slowest reader; across machines it sends and receives the messages over UDP."},
    {"Subscribers",
     "A process calls poll. It reads every complete message since its last poll, joins split messages back together and returns them as ordinary binaries."}
  ]

  @family_labels [
    elixir: "aeron_elixir",
    python: "pyaeron",
    native: "Aeron C, C++, Java, .NET, Go, Rust",
    other: "Other"
  ]

  attr :results, :map, required: true
  attr :highlights, :list, required: true
  attr :examples, :list, required: true
  attr :mapping, :list, required: true
  attr :requirement, :string, required: true
  attr :explorer, :list, required: true

  def render(assigns) do
    assigns =
      assign(assigns,
        strengths: Enum.with_index(@strengths, 1),
        log_steps: Enum.with_index(@log_steps, 1),
        snippets: %{
          publish: @publish_snippet,
          subscribe: @subscribe_snippet,
          batch: @batch_snippet,
          deps: deps_snippet(assigns.requirement),
          driver: @driver_snippet,
          supervise: @supervise_snippet
        },
        explorer_script: @explorer_script,
        description:
          "Aeron client for Elixir: publish and subscribe on Aeron streams from any process, with the Aeron media driver bundled and supervised."
      )

    ~H"""
    <Chrome.document title="aeron_elixir" description={@description} base="" script={@explorer_script}>
    <main id="top">
      <section class="hero wrap">
        <p class="eyebrow">Aeron client for Elixir</p>
        <h1>Aeron, native to the BEAM.</h1>
        <p class="lead">
          Publish and subscribe on Aeron streams from Elixir. Publish and poll are plain function calls
          from any process, and the schedulers spread the work across every core.
        </p>
        <ul class="pills">
          <li>IPC and UDP</li>
          <li>Interoperates with Java, C, C++, Python</li>
          <li>Media driver under supervision</li>
          <li>Ash resources for lifecycle</li>
        </ul>
        <div class="tiles">
          <div :for={tile <- @highlights} class="tile">
            <div class="tile-value">{tile.value}</div>
            <div class="tile-label">{tile.label}</div>
            <div class="tile-detail">{tile.detail}</div>
          </div>
        </div>
        <p class="fine">Measured on {@results.host} · {@results.runtime}. Every figure on this page is read from the benchmark results in the repository.</p>
      </section>

      <section id="strengths" class="band">
        <div class="wrap">
          <p class="eyebrow">Strengths</p>
          <h2>What the BEAM brings to Aeron</h2>
          <div class="grid three">
            <article :for={{{heading, body}, index} <- @strengths} class="card">
              <p class="step">{pad(index)} / {String.upcase(heading)}</p>
              <p>{body}</p>
            </article>
          </div>
        </div>
      </section>

      <section id="api" class="wrap section">
        <p class="eyebrow">Elixir API</p>
        <h2>Aeron's model, in ordinary Elixir</h2>
        <p class="sub">Tagged tuples for every outcome, iodata for every payload, a handle any process can hold.</p>
        <div class="api">
          <div class="api-step">
            <p class="step">01 / PUBLISH</p>
            <h3>Offer with back-pressure you can see</h3>
            <ul class="ticks">
              <li><code>publish/2</code> retries until the client's driver timeout; <code>try_publish/2</code> makes one attempt.</li>
              <li>Returns <code>{"{:ok, position}"}</code> or <code>:back_pressured</code>, <code>:not_connected</code>, <code>:admin_action</code>, <code>:closed</code>.</li>
            </ul>
            <.code html={@snippets.publish} />
          </div>
          <div class="api-step">
            <p class="step">02 / SUBSCRIBE</p>
            <h3>Poll whole messages, never fragments</h3>
            <ul class="ticks">
              <li><code>poll/3</code> and <code>controlled_poll/3</code> take a handler; <code>:continue</code>, <code>:commit</code>, <code>:break</code>, <code>:abort</code> steer consumption.</li>
              <li>Get a callback whenever a publisher joins or leaves the stream.</li>
            </ul>
            <.code html={@snippets.subscribe} />
          </div>
          <div class="api-step">
            <p class="step">03 / BATCH</p>
            <h3>Cross into native code once per batch</h3>
            <ul class="ticks">
              <li>Keep a handle so each call skips looking up the publication.</li>
              <li><code>BatchPublisher</code> never blocks the caller and drops nothing.</li>
            </ul>
            <.code html={@snippets.batch} />
          </div>
        </div>
      </section>

      <section id="log" class="band">
        <div class="wrap">
          <p class="eyebrow">How it works</p>
          <h2>From one process to another, through shared memory</h2>
          <div class="flow">
            <article :for={{{heading, body}, index} <- @log_steps} class="flow-step">
              <p class="step">{pad(index)} / {String.upcase(heading)}</p>
              <p>{body}</p>
            </article>
          </div>
          <div class="term" aria-hidden="true">
            <div class="partition">
              <span class="frame read"></span><span class="frame read"></span><span class="frame read wide"></span><span class="frame read"></span><span class="frame pad"></span>
            </div>
            <div class="partition active">
              <span class="frame read"></span><span class="frame read wide"></span><span class="marker position"><span>read up to here</span></span><span class="frame unread"></span><span class="frame unread wide"></span><span class="frame unread"></span><span class="marker tail"><span>written up to here</span></span>
            </div>
            <div class="partition"></div>
          </div>
          <div class="grid two notes">
            <div>
              <h3>Messages arrive as ordinary binaries</h3>
              <p>
                Each message is copied out of shared memory, so it is safe to keep and pass around.
                The messages from one poll share a single copy; if you hold on to one long after the poll,
                call <code>:binary.copy/1</code> on it so the rest can be freed.
              </p>
            </div>
            <div>
              <h3>Who may call what</h3>
              <p>
                Any number of processes can publish to the same publication at once.
                Only one process should poll a given subscription at a time.
              </p>
            </div>
          </div>
        </div>
      </section>

      <section id="benchmarks" class="wrap section">
        <p class="eyebrow">Benchmarks</p>
        <h2>Measured against the clients it talks to</h2>
        <p class="sub">
          The same encode-offer-poll-decode work in every client, against one C media driver, with every message distinct. Hover a bar for its percentiles.
        </p>
        <div class="explorer" data-explorer>
          <div class="tabs" role="tablist">
            <button
              :for={tab <- @explorer}
              type="button"
              role="tab"
              aria-selected={to_string(tab.id == Explorer.first_tab())}
              data-tab={tab.id}
            >
              {tab.label}
            </button>
          </div>
          <div :for={tab <- @explorer} class="panel" data-tab-panel={tab.id} hidden={tab.id != Explorer.first_tab()}>
            <div class="controls">
              <div :for={control <- tab.controls} class="control" data-field={control.field}>
                <span class="control-label">{control.label}</span>
                <div class="segmented">
                  <button
                    :for={{id, value, label} <- control.options}
                    type="button"
                    aria-pressed={to_string(option_pressed?(tab.default, control, value))}
                    data-option={id}
                  >
                    {label}
                  </button>
                </div>
              </div>
              <button type="button" class="link" data-toggle-table>
                <span class="when-chart">Show table</span><span class="when-table">Show chart</span>
              </button>
            </div>
            <.chart_panel :for={view <- tab.charts} view={view} />
            <p class="method">{tab.method}</p>
            <p class="fine">{@results.host} · {@results.runtime} · C media driver aeronmd_s.</p>
          </div>
        </div>
      </section>

      <section id="pyaeron" class="band">
        <div class="wrap">
          <p class="eyebrow">Coming from pyaeron</p>
          <h2>The same calls, one to one</h2>
          <div class="mapping">
            <div class="mapping-row head"><span>pyaeron</span><span>aeron_elixir</span></div>
            <div :for={{pyaeron, elixir} <- @mapping} class="mapping-row">
              <span><.rich segments={pyaeron} /></span>
              <span><.rich segments={elixir} /></span>
            </div>
          </div>
        </div>
      </section>

      <section id="examples" class="wrap section">
        <p class="eyebrow">Examples</p>
        <h2>{length(@examples)} focused examples, ready to run</h2>
        <p class="sub">
          Each runs with no setup, against the media driver the application starts. Open one to read it.
          They all load <a href={"examples/" <> Docs.page_file("support.exs")}><code>examples/support.exs</code></a>, the shared helpers.
        </p>
        <div class="grid three">
          <a :for={example <- @examples} href={"examples/" <> Docs.page_file(example.file)} class="example">
            <span class="step">{String.slice(example.file, 0, 2)} / {String.upcase(example.title)}</span>
            <span class="example-shows"><.rich segments={example.shows} /></span>
            <span class="example-file">examples/{example.file} →</span>
          </a>
        </div>
      </section>

      <section id="start" class="band">
        <div class="wrap">
          <p class="eyebrow">Quick start</p>
          <h2>Add it. Send a message.</h2>
          <div class="start">
            <div>
              <p class="step">01 / DEPEND</p>
              <.code html={@snippets.deps} />
              <pre class="shell"><code>mix deps.get && mix compile</code></pre>
              <p class="sub small">Compiling builds the NIF and the Aeron media driver with your system C compiler; the driver starts and stops with your application.</p>
            </div>
            <div>
              <p class="step">02 / PUBLISH</p>
              <.code html={@snippets.publish} />
            </div>
            <div>
              <p class="step">03 / SHARE A DRIVER (OPTIONAL)</p>
              <p class="sub small">To use a driver that is already running, name its directory in the environment or in config. <code>mix aeron.driver</code> shows which driver is in use.</p>
              <pre class="shell"><code>AERON_DIR=/run/aeron mix run</code></pre>
              <.code html={@snippets.driver} />
            </div>
          </div>
        </div>
      </section>

      <section id="driver" class="wrap section">
        <p class="eyebrow">The media driver</p>
        <h2>Built in, supervised, and honest when it goes away</h2>
        <p class="sub">
          The Aeron C media driver is compiled with the library and started under your application's supervisor.
          Nothing needs installing; one setting points it at a driver you already run instead.
        </p>
        <div class="mapping">
          <div class="mapping-row head"><span>Setting</span><span>What happens</span></div>
          <div class="mapping-row">
            <span>Nothing set</span>
            <span>The bundled driver starts with your application, in a directory private to it, and is restarted if it exits.</span>
          </div>
          <div class="mapping-row">
            <span><code>AERON_DIR</code> or <code>config :aeron_elixir, aeron_dir:</code></span>
            <span>Connect to the driver something else runs there, such as a host service or another application. Nothing is started.</span>
          </div>
          <div class="mapping-row">
            <span><code>driver: :embedded</code> with a directory</span>
            <span>This application runs the driver at a shared location for other programs on the host to use.</span>
          </div>
          <div class="mapping-row">
            <span><code>binary:</code> path</span>
            <span>Launch another <code>aeronmd</code> instead of the bundled one, still supervised.</span>
          </div>
        </div>
        <div class="grid two notes">
          <div>
            <h3>When the driver goes away</h3>
            <p>
              The client follows the same contract as Aeron's Java and C clients. If the driver shuts down, stops
              heartbeating for the driver timeout, or reports that the client timed out, every publication and
              subscription is closed: publishing returns <code>{"{:error, :closed}"}</code>, polling reads nothing,
              and your image-unavailable callbacks run. The check runs in the client's own process, never on the
              publish or poll path.
            </p>
          </div>
          <div>
            <h3>Supervision brings it back</h3>
            <p>
              The client process exits with the reason, such as <code>{"{:shutdown, :driver_timeout}"}</code>.
              Put the processes that own publications after it in a <code>:rest_for_one</code> supervisor: they
              restart in order, wait for the driver, and add their publications again.
            </p>
          </div>
        </div>
        <.code html={@snippets.supervise} />
      </section>
    </main>
    </Chrome.document>
    """
  end

  attr :view, :map, required: true

  defp chart_panel(assigns) do
    ~H"""
    <div data-chart={@view.key} hidden={not @view.shown?}>
      <div class="chart-head">
        <h3>{@view.chart.title}</h3>
        <ul class="legend">
          <li :for={{family, label} <- families(@view.chart)}><span class={"swatch fam-#{family}"}></span>{label}</li>
        </ul>
      </div>
      <.bars chart={@view.chart} />
      <.table chart={@view.chart} />
      <p :if={Chart.versus_python(@view.chart)} class="versus">
        aeron_elixir: <strong>{Chart.best_elixir(@view.chart).display}</strong>, {Chart.versus_python(@view.chart)}.
      </p>
      <p :if={@view.missing != []} class="fine">
        Not run at this rate, having fallen short at the rate below: {Enum.join(@view.missing, ", ")}.
      </p>
    </div>
    """
  end

  attr :chart, Chart, required: true

  defp bars(assigns) do
    ~H"""
    <ol class="bars">
      <li :for={row <- @chart.rows} class={"bar-row fam-#{row.client.family}"} tabindex="0">
        <div class="bar-label">
          <span class="bar-name">{row.client.name}</span>
          <span class="bar-variant">{row.client.variant}</span>
        </div>
        <div class="bar-track">
          <span class="bar" style={"width: calc((100% - 6.5rem) * #{row.width})"}></span>
          <span class="bar-value">{row.display}</span>
        </div>
        <div class="tip" role="tooltip">
          <strong>{row.client.name}</strong>
          <span class="tip-variant">{row.client.variant}</span>
          <dl>
            <div :for={{label, value} <- row.details}><dt>{label}</dt><dd>{value}</dd></div>
          </dl>
        </div>
      </li>
    </ol>
    """
  end

  attr :chart, Chart, required: true

  defp table(assigns) do
    assigns =
      assign(assigns,
        columns: assigns.chart.rows |> List.first(%{details: []}) |> Map.fetch!(:details) |> Enum.map(&elem(&1, 0))
      )

    ~H"""
    <div class="table-scroll chart-table">
      <table class="data">
        <thead>
          <tr>
            <th>Client</th>
            <th>Variant</th>
            <th :for={column <- @columns}>{column}</th>
          </tr>
        </thead>
        <tbody>
          <tr :for={row <- @chart.rows} class={"fam-#{row.client.family}"}>
            <td><span class="swatch"></span>{row.client.name}</td>
            <td>{row.client.variant}</td>
            <td :for={{_label, value} <- row.details}>{value}</td>
          </tr>
        </tbody>
      </table>
    </div>
    """
  end

  defp deps_snippet(requirement) do
    Docs.highlight("""
    defp deps do
      [{:aeron_elixir, "#{requirement}"}]
    end
    """)
  end

  defp families(chart) do
    present = MapSet.new(chart.rows, & &1.client.family)
    Enum.filter(@family_labels, fn {family, _label} -> MapSet.member?(present, family) end)
  end

  defp option_pressed?(default, control, value), do: Map.fetch!(default, control.field) == value

  defp pad(index), do: index |> Integer.to_string() |> String.pad_leading(2, "0")
end
