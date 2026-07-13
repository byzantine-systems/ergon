import Config

# Connection settings are resolved at runtime from the standard libpq
# environment variables (PGHOST, PGUSER, ...), matching the devenv Postgres
# service and the project's `.env`. This keeps credentials out of the repo and
# works identically for `mix`, `iex`, and packaged releases.
host = System.get_env("PGHOST", "127.0.0.1")
port = String.to_integer(System.get_env("PGPORT", "5432"))
user = System.get_env("PGUSER", "ergon")
password = System.get_env("PGPASSWORD", "ergon")
database = System.get_env("PGDATABASE", "ergon")

# The test suite gets its own database so integration tests never touch a
# developer's working data.
database = if config_env() == :test, do: "#{database}_test", else: database

config :ergon, Ergon.Repo,
  hostname: host,
  port: port,
  username: user,
  password: password,
  database: database
