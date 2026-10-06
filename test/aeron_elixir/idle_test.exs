defmodule AeronElixir.IdleTest do
  use ExUnit.Case, async: true

  alias AeronElixir.Idle

  test "busy spin and yield return the strategy unchanged" do
    assert Idle.idle(:busy_spin, 0) == :busy_spin
    assert Idle.idle(:yield, 0) == :yield
  end

  test "sleep parks for the configured interval when there was no work" do
    strategy = {:sleep, 2}
    started = System.monotonic_time(:millisecond)
    assert Idle.idle(strategy, 0) == strategy
    assert System.monotonic_time(:millisecond) - started >= 2
  end

  test "backoff escalates while idle and resets on work" do
    strategy = Idle.backoff(max_spins: 2, max_yields: 2, min_park_ms: 1, max_park_ms: 4)

    escalated = Enum.reduce(1..6, strategy, fn _, acc -> Idle.idle(acc, 0) end)
    assert Idle.state(escalated) == :parking

    assert Idle.state(Idle.idle(escalated, 5)) == :spinning
  end
end
