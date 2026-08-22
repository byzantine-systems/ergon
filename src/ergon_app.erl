-module(ergon_app).
-moduledoc """
Ergon's application callback.

Starts the connection pool, then the supervision tree. The pool comes first and
lives outside the tree on purpose: `pgo:start_pool/2` hands it to `pgo_sup`,
which already supervises it, and returns a pid nothing here is linked to. See
`ergon_repo` for why that is not turned into a child spec.
""".

-behaviour(application).

-export([start/2, stop/1]).

-spec start(application:start_type(), term()) -> {ok, pid()} | {error, term()}.
start(_StartType, _StartArgs) ->
    {ok, _Pool} = ergon_repo:start_pool(),
    ergon_sup:start_link().

-spec stop(term()) -> ok.
stop(_State) -> ok.
