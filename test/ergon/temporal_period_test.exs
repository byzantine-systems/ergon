defmodule Ergon.TemporalPeriodTest do
  # Ergon.TemporalPeriod had no dedicated test file before this, new/2,
  # empty/0, and contains?/2 are pure and boundary-inclusivity-sensitive,
  # which makes them a good fit for properties over hand-picked instants: the
  # interesting bugs here live exactly at the lower/upper boundary, which a
  # handful of examples would only ever spot-check.
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Ergon.TemporalPeriod

  # Synthetic instants: small integers mapped through DateTime.from_unix!/2
  # so bounds and probe datetimes can coincide exactly (the case that matters
  # for inclusive/exclusive boundary math) without needing real timestamps.
  defp instant(i), do: DateTime.from_unix!(i, :second)
  defp instant_gen, do: map(integer(-1000..1000), &instant/1)

  describe "new/2" do
    property "always builds a closed-open (lower-inclusive, upper-exclusive), non-empty period" do
      bound_gen = one_of([constant(:unbounded), instant_gen()])

      check all(
              lower <- bound_gen,
              upper <- bound_gen
            ) do
        period = TemporalPeriod.new(lower, upper)

        assert period.lower == lower
        assert period.upper == upper
        assert period.lower_inclusive
        refute period.upper_inclusive
        refute period.empty
      end
    end
  end

  describe "contains?/2, bounded period" do
    property "contains? agrees with [lower, upper) against the same integer instants" do
      check all(
              lower_i <- integer(-1000..999),
              upper_i <- integer((lower_i + 1)..1000),
              dt_i <- integer(-1100..1100)
            ) do
        period = TemporalPeriod.new(instant(lower_i), instant(upper_i))
        expected = dt_i >= lower_i and dt_i < upper_i

        assert TemporalPeriod.contains?(period, instant(dt_i)) == expected
      end
    end
  end

  describe "contains?/2, unbounded endpoints" do
    property "an unbounded lower bound contains every instant before upper" do
      check all(
              upper_i <- integer(-1000..1000),
              dt_i <- integer(-1100..1100)
            ) do
        period = TemporalPeriod.new(:unbounded, instant(upper_i))

        assert TemporalPeriod.contains?(period, instant(dt_i)) == dt_i < upper_i
      end
    end

    property "an unbounded upper bound contains every instant from lower onward" do
      check all(
              lower_i <- integer(-1000..1000),
              dt_i <- integer(-1100..1100)
            ) do
        period = TemporalPeriod.new(instant(lower_i), :unbounded)

        assert TemporalPeriod.contains?(period, instant(dt_i)) == dt_i >= lower_i
      end
    end

    property "a fully unbounded period contains every instant" do
      check all(dt_i <- integer(-1000..1000)) do
        assert TemporalPeriod.contains?(TemporalPeriod.new(:unbounded, :unbounded), instant(dt_i))
      end
    end
  end

  describe "empty/0" do
    property "the empty period contains no instant" do
      check all(dt_i <- integer(-1000..1000)) do
        refute TemporalPeriod.contains?(TemporalPeriod.empty(), instant(dt_i))
      end
    end

    test "is marked empty with :empty, non-inclusive bounds" do
      period = TemporalPeriod.empty()

      assert period.empty
      assert period.lower == :empty
      assert period.upper == :empty
      refute period.lower_inclusive
      refute period.upper_inclusive
    end
  end
end
