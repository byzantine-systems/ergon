-module(ergon_path).
-moduledoc """
Resolving the directory specs Ergon accepts from host applications.

Two things take directories from a host: `ergon_sql`, for extra `priv/queries`
roots, and `ergon_migrate`, for extra migration sources. Both accept the same
three forms, which is deliberate: a host that has learned one has learned both.

```erlang
{priv, my_app, "queries"}       %% <my_app>/priv/queries
{app,  my_app, "priv/queries"}  %% <my_app>/priv/queries, spelled the long way
"/absolute/path/to/queries"     %% used verbatim
```

`{priv, App, Sub}` is the form to reach for. `{app, App, Sub}` exists because an
application's private files are not always under `priv/`, and a bare path exists
because a test fixture is not an application at all.

Both resolutions raise rather than answer a path that does not exist, because a
misspelled application name would otherwise surface much later as an empty
directory, which reads as "no queries here" rather than as a mistake.
""".

-export([resolve_root/1]).

-type root() ::
    file:filename_all()
    | {priv, atom(), file:filename_all()}
    | {app, atom(), file:filename_all()}.
-export_type([root/0]).

-doc """
Resolve a root spec to an absolute directory, as a string.

**A string, never a binary**, even when given one. `code:priv_dir/1` answers a
string and so does `filename:join/2` on strings, so that is what everything
downstream has been built against: migraterl converts the filenames it scans with
`list_to_binary/1`, which raises `badarg` on a binary that is already one. A host
passing a binary path would otherwise get a failure from deep inside the scanner
with nothing pointing back at the path it configured.
""".
-spec resolve_root(root()) -> string().
resolve_root({priv, App, Sub}) -> flatten(filename:join(priv_dir(App), Sub));
resolve_root({app, App, Sub}) -> flatten(filename:join(lib_dir(App), Sub));
resolve_root(Path) when is_list(Path); is_binary(Path) -> flatten(Path).

flatten(Path) -> unicode:characters_to_list(Path).

priv_dir(App) ->
    case code:priv_dir(App) of
        {error, bad_name} -> erlang:error({priv_dir_not_found, App});
        Dir -> Dir
    end.

lib_dir(App) ->
    case code:lib_dir(App) of
        {error, bad_name} -> erlang:error({lib_dir_not_found, App});
        Dir -> Dir
    end.
