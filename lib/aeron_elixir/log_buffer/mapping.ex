defmodule AeronElixir.LogBuffer.Mapping do
  @moduledoc """
  A mapped log file: the region owning the mapping, its base address and its length.

  `region` is the reference returned by `AeronElixir.NIF.map_log/1`. The mapping
  stays valid while any term referencing that reference is reachable and is
  released when the last one is collected, so `base_address` is meaningful only
  for as long as a handle built from this mapping is held.
  """

  @type t :: %__MODULE__{
          region: reference(),
          base_address: integer(),
          length: pos_integer()
        }

  defstruct [:region, :base_address, :length]

  @doc """
  Builds a mapping from the values `AeronElixir.NIF.map_log/1` returns.
  """
  @spec new(reference(), integer(), pos_integer()) :: t()
  def new(region, base_address, length)
      when is_reference(region) and is_integer(base_address) and is_integer(length) and length > 0 do
    %__MODULE__{region: region, base_address: base_address, length: length}
  end
end
