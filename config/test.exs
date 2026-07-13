import Config

# Integration tests run against a real PostgreSQL 18/19 instance (the temporal
# constraints and the SQL/PGQ property graph cannot be faked). The Sandbox
# pool gives each test an isolated transaction.
config :ergon, Ergon.Repo,
  pool: Ecto.Adapters.SQL.Sandbox,
  pool_size: 10

# PartitionBootCheck is disabled in tests, the app boots before the SQL
# sandbox is configured, and tests exercise it explicitly via
# `start_supervised!({Ergon.PartitionBootCheck, enabled: true, ...})`.
config :ergon, Ergon.PartitionBootCheck, enabled: false

# JobNotifier is disabled in tests: its dedicated LISTEN connection lives
# outside the SQL sandbox, and a sandboxed enqueue's NOTIFY is rolled back, so
# an always-on listener would never fire anyway. The notifier tests start it
# explicitly against a committed (non-sandbox) connection.
config :ergon, Ergon.JobNotifier, enabled: false

config :logger, level: :warning
