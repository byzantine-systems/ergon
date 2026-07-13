defmodule Ergon.TemporalPeriod do
  @moduledoc """
  Elixir representation of a PostgreSQL temporal validity period.

  PostgreSQL 19 has no scalar `PERIOD` type: application- and system-time
  periods are stored as `tstzrange` values. This struct is the decoded,
  type-safe form of such a range, the target of
  `Ergon.Postgrex.Extensions.TstzRange` (see `Ergon.PostgresTypes`), so
  temporal columns come back as structs instead of `%Postgrex.Range{}`
  wrappers or unparsed strings.

  Unbounded (infinite) endpoints are represented as `:unbounded`, and an empty
  range as `empty: true` with `:empty` endpoints.
  """

  @type endpoint :: DateTime.t() | :unbounded | :empty

  @type t :: %__MODULE__{
          lower: endpoint(),
          upper: endpoint(),
          lower_inclusive: boolean(),
          upper_inclusive: boolean(),
          empty: boolean()
        }

  @enforce_keys [:lower, :upper]
  defstruct lower: :unbounded,
            upper: :unbounded,
            lower_inclusive: true,
            upper_inclusive: false,
            empty: false

  @doc """
  Builds a closed-open period `[lower, upper)`, the default bound style
  PostgreSQL uses for `tstzrange`. Either endpoint may be `:unbounded`.
  """
  @spec new(endpoint(), endpoint()) :: t()
  def new(lower, upper) do
    %__MODULE__{lower: lower, upper: upper, lower_inclusive: true, upper_inclusive: false}
  end

  @doc "The empty period."
  @spec empty() :: t()
  def empty do
    %__MODULE__{
      lower: :empty,
      upper: :empty,
      lower_inclusive: false,
      upper_inclusive: false,
      empty: true
    }
  end

  @doc """
  Whether `datetime` falls within the period, honouring bound inclusivity and
  unbounded endpoints. An empty period contains nothing.
  """
  @spec contains?(t(), DateTime.t()) :: boolean()
  def contains?(%__MODULE__{empty: true}, %DateTime{}), do: false

  def contains?(%__MODULE__{} = period, %DateTime{} = dt) do
    after_lower?(period, dt) and before_upper?(period, dt)
  end

  defp after_lower?(%__MODULE__{lower: :unbounded}, _dt), do: true

  defp after_lower?(%__MODULE__{lower: lower, lower_inclusive: inc}, dt) do
    case DateTime.compare(dt, lower) do
      :gt -> true
      :eq -> inc
      :lt -> false
    end
  end

  defp before_upper?(%__MODULE__{upper: :unbounded}, _dt), do: true

  defp before_upper?(%__MODULE__{upper: upper, upper_inclusive: inc}, dt) do
    case DateTime.compare(dt, upper) do
      :lt -> true
      :eq -> inc
      :gt -> false
    end
  end
end
