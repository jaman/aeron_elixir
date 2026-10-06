defmodule AeronElixir.Idle do
  @moduledoc """
  Idle strategies for poll and offer duty cycles.

  A strategy is one of `:busy_spin` (return immediately), `:yield` (hand the
  scheduler a chance to run other processes), `{:sleep, ms}` (park for `ms`
  milliseconds), or a backoff struct from `backoff/1` that spins, then yields,
  then parks with doubling intervals while there is no work.

  `idle/2` takes the strategy and the amount of work the caller just did; it
  returns the strategy to use on the next iteration (backoff carries state).
  """

  defmodule Backoff do
    @moduledoc """
    State of a backoff idle strategy: how many spins and yields remain before
    parking, and the park interval bounds.
    """

    @type t :: %__MODULE__{
            max_spins: non_neg_integer(),
            max_yields: non_neg_integer(),
            min_park_ms: pos_integer(),
            max_park_ms: pos_integer(),
            state: :spinning | :yielding | :parking,
            spins: non_neg_integer(),
            yields: non_neg_integer(),
            park_ms: pos_integer()
          }

    @enforce_keys [:max_spins, :max_yields, :min_park_ms, :max_park_ms, :park_ms]
    defstruct [
      :max_spins,
      :max_yields,
      :min_park_ms,
      :max_park_ms,
      :park_ms,
      state: :spinning,
      spins: 0,
      yields: 0
    ]
  end

  @type strategy :: :busy_spin | :yield | {:sleep, pos_integer()} | Backoff.t()

  @doc """
  Builds a backoff strategy. Options: `max_spins` (default 10), `max_yields`
  (default 5), `min_park_ms` (default 1), `max_park_ms` (default 16).
  """
  @spec backoff(keyword()) :: Backoff.t()
  def backoff(opts \\ []) do
    min_park_ms = Keyword.get(opts, :min_park_ms, 1)

    %Backoff{
      max_spins: Keyword.get(opts, :max_spins, 10),
      max_yields: Keyword.get(opts, :max_yields, 5),
      min_park_ms: min_park_ms,
      max_park_ms: Keyword.get(opts, :max_park_ms, 16),
      park_ms: min_park_ms
    }
  end

  @doc """
  Idles according to `strategy` when `work_count` is zero; with work done it
  only resets backoff state. Returns the strategy for the next iteration.
  """
  @spec idle(strategy(), non_neg_integer()) :: strategy()
  def idle(strategy, work_count) when work_count > 0, do: reset(strategy)
  def idle(:busy_spin, _work_count), do: :busy_spin

  def idle(:yield, _work_count) do
    :erlang.yield()
    :yield
  end

  def idle({:sleep, ms} = strategy, _work_count) do
    Process.sleep(ms)
    strategy
  end

  def idle(%Backoff{state: :spinning, spins: spins, max_spins: max_spins} = backoff, _work_count)
      when spins < max_spins,
      do: %{backoff | spins: spins + 1}

  def idle(%Backoff{state: :spinning} = backoff, _work_count),
    do: idle(%{backoff | state: :yielding}, 0)

  def idle(
        %Backoff{state: :yielding, yields: yields, max_yields: max_yields} = backoff,
        _work_count
      )
      when yields < max_yields do
    :erlang.yield()
    %{backoff | yields: yields + 1}
  end

  def idle(%Backoff{state: :yielding} = backoff, _work_count),
    do: idle(%{backoff | state: :parking}, 0)

  def idle(%Backoff{state: :parking} = backoff, _work_count) do
    Process.sleep(backoff.park_ms)
    %{backoff | park_ms: min(backoff.park_ms * 2, backoff.max_park_ms)}
  end

  @doc """
  Returns the phase a backoff strategy is in.
  """
  @spec state(Backoff.t()) :: :spinning | :yielding | :parking
  def state(%Backoff{state: state}), do: state

  defp reset(%Backoff{} = backoff) do
    %{backoff | state: :spinning, spins: 0, yields: 0, park_ms: backoff.min_park_ms}
  end

  defp reset(strategy), do: strategy
end
