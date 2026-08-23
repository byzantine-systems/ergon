-module(ergon_graph_SUITE).
-moduledoc """
The SQL/PGQ property graph and the dependency enforcement built on it.

`ready_children/0` is the case that matters historically. The vertex table
originally keyed on `ergon.jobs (id)`, which is deliberately **not** unique under
a temporal primary key, so every valid-time version of a job became its own
vertex and `bool_and(parent_state = 'completed')` was false for any parent that
had ever run. Since `apply_outcome` transitions state with `FOR PORTION OF`, a
completed job always has at least two versions, which made the query return zero
rows for every parent capable of unblocking anything. It is pointed at
`ergon.jobs_current` now, and `completed_parent_appears_once/1` is the assertion
that says so directly rather than by consequence.

That one case **cannot use the fixture transaction**, and the reason is worth
stating because it is also the likeliest reason the original suite never caught
the bug. `valid_period` is application time and is split by `FOR PORTION OF` at
`now()`, which is frozen for the whole transaction. So inside one transaction the
split point equals the row's own lower bound, the leading portion is empty, and
PostgreSQL keeps a single row: a job driven from available to completed inside a
fixture transaction has exactly **one** valid-time version, and a duplicate
vertex is unreproducible. Committed, it has two. The Elixir suite ran everything
inside the Ecto sandbox.

This is the opposite conclusion from `system_time`, and both are right. Belief
time must advance within a transaction, because two beliefs really did happen in
sequence, so the versioning trigger uses `statement_timestamp()`. Application
time must not, because a transaction is a single instant as far as the world is
concerned, so `FOR PORTION OF` uses `now()`.
""".

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([init_per_testcase/2, end_per_testcase/2]).
-export([
    child_is_not_ready_while_parent_is_live/1,
    completing_a_parent_readies_its_child/1,
    completed_parent_appears_once/1,
    descendants_returns_the_transitive_closure/1,
    direct_children_returns_one_row_per_child/1,
    link_rejects_a_cycle/1,
    link_rejects_a_self_loop/1,
    blocked_child_is_absent_from_checkout/1,
    pending_parents_tracks_the_edges/1
]).

-define(SPEC(Queue), ergon_new_job:on_queue(ergon_new_job:new(~"w"), Queue)).

all() ->
    [
        child_is_not_ready_while_parent_is_live,
        completing_a_parent_readies_its_child,
        completed_parent_appears_once,
        descendants_returns_the_transitive_closure,
        direct_children_returns_one_row_per_child,
        link_rejects_a_cycle,
        link_rejects_a_self_loop,
        blocked_child_is_absent_from_checkout,
        pending_parents_tracks_the_edges
    ].

init_per_suite(Config) ->
    ok = ergon_test_db:setup(),
    Config.

end_per_suite(_Config) ->
    ok.

%% Cases that need more than one valid-time version, and so cannot run inside the
%% fixture transaction. See the module docs.
committed_cases() -> [completed_parent_appears_once].

init_per_testcase(Case, Config) ->
    case lists:member(Case, committed_cases()) of
        true ->
            [{prefix, ergon_test_db:unique(~"gph")} | Config];
        false ->
            ok = ergon_test_db:sandbox(),
            Config
    end.

end_per_testcase(Case, Config) ->
    case lists:member(Case, committed_cases()) of
        true -> ergon_test_db:cleanup_jobs(?config(prefix, Config));
        false -> ergon_test_db:rollback()
    end.

%% ---------------
%% Readiness
%% ---------------

child_is_not_ready_while_parent_is_live(_Config) ->
    {_Parent, Child} = pair(),
    {ok, Ready} = ergon:ready_children(),
    ?assertNot(lists:member(Child, Ready)).

completing_a_parent_readies_its_child(_Config) ->
    {Parent, Child} = pair(),
    ok = complete(Parent),

    {ok, Ready} = ergon:ready_children(),
    ?assert(lists:member(Child, Ready)),
    ?assertEqual({ok, [Child]}, ergon:unblocked_by(Parent)).

%% The property-graph bug, asserted at its source rather than through a query
%% that happens to depend on it.
%%
%% Committed, one statement per transition, because that is the only way a second
%% valid-time version comes into existence.
completed_parent_appears_once(Config) ->
    Prefix = ?config(prefix, Config),
    ParentQueue = <<Prefix/binary, "_parent">>,
    ChildQueue = <<Prefix/binary, "_child">>,

    {ok, #{id := Parent}} = ergon:enqueue(?SPEC(ParentQueue)),
    {ok, #{id := Child}} = ergon:enqueue(?SPEC(ChildQueue)),
    ok = ergon:depends_on(Parent, Child),

    {ok, [Job]} = ergon_db:checkout(ParentQueue, 1),
    {ok, Outcome} = ergon_fsm:transition(Job, succeeded),
    {ok, #{state := completed}} = ergon_db:apply_outcome(Parent, Outcome),

    %% The job really does own more than one valid-time version...
    ?assert(
        ergon_test_db:scalar(
            "SELECT count(*)::int FROM ergon.jobs WHERE id = $1", [Parent]
        ) >= 2
    ),
    %% ...and the graph still sees exactly one vertex for it.
    ?assertEqual(
        1,
        ergon_test_db:scalar(
            "SELECT count(*)::int FROM ergon.jobs_current WHERE id = $1", [Parent]
        )
    ),
    %% Which is what makes the readiness query answer at all: with a duplicate
    %% vertex the superseded version made bool_and(parent_state = 'completed')
    %% false and this returned nothing.
    {ok, Ready} = ergon:ready_children(),
    ?assert(lists:member(Child, Ready)).

%% ---------------
%% Reachability
%% ---------------

descendants_returns_the_transitive_closure(Config) ->
    {A, B, C} = chain(Config),
    {ok, Ids} = ergon_graph:descendants(A),
    ?assertEqual([B, C], lists:sort(Ids)).

%% The milder form of the same vertex bug returned each child once per parent
%% version rather than once.
direct_children_returns_one_row_per_child(_Config) ->
    {Parent, Child} = pair(),
    ok = complete(Parent),
    ?assertEqual({ok, [Child]}, ergon_graph:direct_children(Parent)).

link_rejects_a_cycle(Config) ->
    {A, _B, C} = chain(Config),
    %% C -> A would close the A -> B -> C -> A loop.
    ?assertEqual({error, would_create_cycle}, ergon_db:link(C, A)).

link_rejects_a_self_loop(Config) ->
    {A, _B, _C} = chain(Config),
    ?assertEqual({error, would_create_cycle}, ergon_db:link(A, A)).

%% ---------------
%% Enforcement
%% ---------------

%% Phase 4 made this true. Before it, checkout never joined job_edges at all and
%% a child was claimable the moment it was enqueued, while the docs promised
%% otherwise.
blocked_child_is_absent_from_checkout(_Config) ->
    Queue = ergon_test_db:unique(~"q"),
    {ok, #{id := Parent}} = ergon:enqueue(?SPEC(Queue)),
    {ok, #{id := Child}} = ergon:enqueue(?SPEC(Queue)),
    ok = ergon:depends_on(Parent, Child),

    %% Only the parent, though both are available and on the same queue.
    {ok, Claimed} = ergon_db:checkout(Queue, 10),
    ?assertEqual([Parent], [Id || #{id := Id} <- Claimed]),

    {ok, Outcome} = ergon_fsm:transition(hd(Claimed), succeeded),
    {ok, _} = ergon_db:apply_outcome(Parent, Outcome),

    %% Completing the parent puts the child back in the index.
    ?assertMatch({ok, [#{id := Child}]}, ergon_db:checkout(Queue, 10)).

%% The counter is denormalised onto the row so `jobs_fetch_idx` can carry
%% `pending_parents = 0` and blocked jobs are never scanned. It is maintained
%% incrementally by triggers, so it has to be checked against the edges it
%% summarises. ergon_reconciler_SUITE covers the drift case.
pending_parents_tracks_the_edges(_Config) ->
    P1 = enqueue_alone(),
    P2 = enqueue_alone(),
    Child = enqueue_alone(),

    ?assertEqual(0, pending_parents(Child)),
    ok = ergon:depends_on(P1, Child),
    ?assertEqual(1, pending_parents(Child)),
    ok = ergon:depends_on(P2, Child),
    ?assertEqual(2, pending_parents(Child)),

    ok = complete(P1),
    ?assertEqual(1, pending_parents(Child)),
    ok = complete(P2),
    ?assertEqual(0, pending_parents(Child)).

%% ---------------
%% Helpers
%% ---------------

%% One job on a queue of its own.
%%
%% Every helper here does this, because `complete/1` works by checking the job
%% out and a shared queue makes that ambiguous: a checkout aimed at one job
%% claims every other runnable job on the queue too, leaving them `executing` and
%% unclaimable by the next call. Sharing a queue is a deliberate choice in
%% `blocked_child_is_absent_from_checkout/1`, which is about exactly that, and a
%% bug everywhere else.
enqueue_alone() ->
    {ok, #{id := Id}} = ergon:enqueue(?SPEC(ergon_test_db:unique(~"q"))),
    Id.

pair() ->
    Parent = enqueue_alone(),
    Child = enqueue_alone(),
    ok = ergon:depends_on(Parent, Child),
    {Parent, Child}.

chain(_Config) ->
    A = enqueue_alone(),
    B = enqueue_alone(),
    C = enqueue_alone(),
    ok = ergon:depends_on(A, B),
    ok = ergon:depends_on(B, C),
    {A, B, C}.

%% Drive a job to completed through the real lifecycle rather than by writing the
%% state, so the transition guard and the versioning trigger both see it.
complete(Id) ->
    Queue = ergon_test_db:scalar("SELECT queue FROM ergon.jobs_current WHERE id = $1", [Id]),
    {ok, [Job]} = ergon_db:checkout(Queue, 1),
    #{id := Id} = Job,
    {ok, Outcome} = ergon_fsm:transition(Job, succeeded),
    {ok, #{state := completed}} = ergon_db:apply_outcome(Id, Outcome),
    ok.

pending_parents(Id) ->
    ergon_test_db:scalar(
        "SELECT pending_parents FROM ergon.jobs_current WHERE id = $1", [Id]
    ).
