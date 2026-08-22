-module(ergon_ident).
-moduledoc """
The guard on names that reach SQL as identifiers rather than as bind parameters.

Almost everything Ergon sends to PostgreSQL is parameterised. A few things
cannot be, and they are the reason this module exists:

- pgmq derives `pgmq.q_<queue>` from a queue name, and the notify tick builds its
  visibility probe with `format('%I')`.
- `ergon_migration` bakes a table name into `auto_manage_partitions_<table>`,
  and into `CREATE TABLE` statements generally.
- `ergon_partition_boot_check` calls that function by name, and a function
  cannot be addressed by bind parameter.

In each case the name is host-authored rather than user input, so this is not the
last line of defence. It is the cheap one: rejecting anything that could need
quoting is simpler to reason about than quoting correctly everywhere, and it
fails at the call site rather than inside a `format/2` three layers down.

Deliberately stricter than PostgreSQL's own rules, which permit quoted
identifiers containing almost anything. Ergon allows only what never needs
quoting in the first place.
""".

-export([validate/2, valid/1, pattern/0]).
-export([format_error/2]).

-define(RE, "^[a-z_][a-z0-9_]*$").

-doc "The regular expression an identifier must match.".
-spec pattern() -> string().
pattern() -> ?RE.

-doc "Whether `Name` is a bare lowercase SQL identifier.".
-spec valid(binary()) -> boolean().
valid(Name) when is_binary(Name) ->
    re:run(Name, ?RE, [{capture, none}]) =:= match.

-doc """
Return `Name` if it is a bare lowercase SQL identifier, or raise.

`Kind` names what is being validated (`queue`, `table`, `channel`) and appears in
the error, since "invalid identifier" on its own tells a caller very little about
which of several names it got wrong.
""".
-spec validate(binary(), atom()) -> binary().
validate(Name, Kind) when is_binary(Name) ->
    case valid(Name) of
        true ->
            Name;
        false ->
            erlang:error(
                invalid_identifier,
                [Name, Kind],
                [{error_info, #{module => ?MODULE, cause => #{name => Name, kind => Kind}}}]
            )
    end.

-doc false.
-spec format_error(term(), erlang:stacktrace()) -> #{pos_integer() | general => unicode:chardata()}.
format_error(invalid_identifier, [{_M, _F, _Args, Info} | _]) ->
    #{name := Name, kind := Kind} = cause(Info),
    #{
        1 => iolist_to_binary(
            io_lib:format(
                "invalid ~s name ~p. Must match ~s: it reaches SQL as an identifier, "
                "not only as a bind parameter",
                [Kind, Name, ?RE]
            )
        )
    };
format_error(_Reason, _Stacktrace) ->
    #{}.

cause(Info) ->
    maps:get(cause, proplists:get_value(error_info, Info, #{}), #{}).
