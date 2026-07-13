defmodule Ergon.Postgrex.Extensions.TstzRange do
  @moduledoc """
  Postgrex binary extension decoding `tstzrange`, PostgreSQL's concrete
  representation of a temporal validity/system period, into a
  `Ergon.TemporalPeriod` struct (and back).

  It matches `tstzrange` by `typname`, so it must be listed *before* the
  generic `Postgrex.Extensions.Range` super-extension (which matches every
  range via `send: "range_send"`) in `Ergon.PostgresTypes`, so the first
  matching clause wins. Other range types are left to the generic handler.

  The wire format is PostgreSQL's range binary layout: a flags byte followed
  by each present bound as `int32` length + value, where a `timestamptz`
  value is an `int64` count of microseconds since the PostgreSQL epoch
  (2000-01-01 UTC). Endpoints flagged infinite are omitted from the payload.
  """

  @behaviour Postgrex.Extension

  import Bitwise

  alias Ergon.TemporalPeriod

  # Range flag bits, see PostgreSQL src/include/utils/rangetypes.h.
  @range_empty 0x01
  @range_lb_inc 0x02
  @range_ub_inc 0x04
  @range_lb_inf 0x08
  @range_ub_inf 0x10

  # Microseconds between the Unix epoch (1970-01-01) and the PostgreSQL
  # timestamptz epoch (2000-01-01), both UTC.
  @us_epoch 946_684_800_000_000

  # PostgreSQL's sentinel timestamptz values for -infinity / +infinity. These
  # are distinct from the range flags `@range_lb_inf` / `@range_ub_inf`: those
  # mark an infinite *endpoint* (omitted from the payload entirely), while
  # `DT_NOBEGIN` / `DT_NOEND` show up as an actual 8-byte value inside the
  # payload when the range is constructed with a literal `'infinity'::timestamptz`
  # bound (e.g. `tstzrange(now(), 'infinity')`, which is ergon.jobs's
  # `valid_period` default). Without this handling the decoder tries to build
  # a `DateTime` from INT64_MIN/MAX and crashes.
  @pg_dt_nobegin -0x8000000000000000
  @pg_dt_noend 0x7FFFFFFFFFFFFFFF

  @impl true
  def init(opts), do: opts

  @impl true
  def matching(_state), do: [type: "tstzrange"]

  @impl true
  def format(_state), do: :binary

  @impl true
  def decode(_state) do
    quote location: :keep do
      <<len::signed-size(32), data::binary-size(len)>> ->
        unquote(__MODULE__).decode_range(data)
    end
  end

  @impl true
  def encode(_state) do
    quote location: :keep do
      %Ergon.TemporalPeriod{} = period ->
        unquote(__MODULE__).encode_range(period)

      other ->
        raise DBConnection.EncodeError,
              Postgrex.Utils.encode_msg(other, Ergon.TemporalPeriod)
    end
  end

  ## Helpers (run in this module's context, called from the quoted clauses)

  @doc false
  def decode_range(<<flags, _rest::binary>>) when (flags &&& @range_empty) != 0 do
    TemporalPeriod.empty()
  end

  def decode_range(<<flags, rest::binary>>) do
    {lower, rest} = decode_bound(rest, (flags &&& @range_lb_inf) != 0)
    {upper, _rest} = decode_bound(rest, (flags &&& @range_ub_inf) != 0)

    %TemporalPeriod{
      lower: lower,
      upper: upper,
      lower_inclusive: (flags &&& @range_lb_inc) != 0,
      upper_inclusive: (flags &&& @range_ub_inc) != 0,
      empty: false
    }
  end

  defp decode_bound(rest, true), do: {:unbounded, rest}

  defp decode_bound(rest, false) do
    <<len::signed-size(32), value::binary-size(len), tail::binary>> = rest
    {decode_timestamptz(value), tail}
  end

  defp decode_timestamptz(<<@pg_dt_nobegin::signed-size(64)>>), do: :unbounded
  defp decode_timestamptz(<<@pg_dt_noend::signed-size(64)>>), do: :unbounded

  defp decode_timestamptz(<<microseconds::signed-size(64)>>) do
    DateTime.from_unix!(microseconds + @us_epoch, :microsecond)
  end

  @doc false
  def encode_range(%TemporalPeriod{empty: true}) do
    [<<1::signed-size(32)>>, @range_empty]
  end

  def encode_range(%TemporalPeriod{} = period) do
    {flags, data} = encode_bound(0, [], period.lower, @range_lb_inf)
    {flags, data} = encode_bound(flags, data, period.upper, @range_ub_inf)
    flags = if period.lower_inclusive, do: flags ||| @range_lb_inc, else: flags
    flags = if period.upper_inclusive, do: flags ||| @range_ub_inc, else: flags

    [<<IO.iodata_length(data) + 1::signed-size(32)>>, flags | data]
  end

  defp encode_bound(flags, data, endpoint, inf_flag) when endpoint in [:unbounded, :empty, nil] do
    {flags ||| inf_flag, data}
  end

  defp encode_bound(flags, data, %DateTime{} = dt, _inf_flag) do
    microseconds = DateTime.to_unix(dt, :microsecond) - @us_epoch
    {flags, [data | <<8::signed-size(32), microseconds::signed-size(64)>>]}
  end
end
