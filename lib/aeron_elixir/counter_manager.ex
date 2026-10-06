defmodule AeronElixir.CounterManager do
  @moduledoc """
  Atomic accessor for driver-allocated counter values.

  Each counter occupies a `COUNTER_LENGTH` (128-byte) slot in the CnC
  counters-values buffer; its value is the int64 at the slot's base. The value
  address is resolved once during the add-counter handshake via
  `AeronElixir.CnC.Reader.counter_address/2` and then read and mutated in place
  with the same NIF atomics the data path uses for the publication-limit and
  subscriber-position counters. All functions operate on the absolute value
  address, never on a counter id, so there is no per-call buffer arithmetic.
  """

  alias AeronElixir.NIF

  @spec get(integer()) :: {:ok, integer()} | {:error, term()}
  def get(value_address) when is_integer(value_address) do
    NIF.atomic_get_int64(value_address)
  end

  @spec increment(integer(), integer()) :: {:ok, integer()} | {:error, term()}
  def increment(value_address, delta) when is_integer(value_address) and is_integer(delta) do
    with {:ok, previous} <- NIF.atomic_fetch_add_int64(value_address, delta) do
      {:ok, previous + delta}
    end
  end

  @spec set(integer(), integer()) :: {:ok, integer()} | {:error, term()}
  def set(value_address, value) when is_integer(value_address) and is_integer(value) do
    with :ok <- NIF.write_int64_ordered(value_address, value) do
      {:ok, value}
    end
  end
end
