use Mix.Config

config :schema_parser, Sp.Repo,
  adapter: MssqlEcto,
  database: "grendene_gerador_programas_12_02_20",
  # username: "test",
  # password: "12345",
  hostname: "localhost",
  instance_name: "SQLEXPRESS2016",
  odbc_driver: "ODBC Driver 17 for SQL Server",
  trusted_connection: "yes"
config :schema_parser, ecto_repos: [Sp.Repo]
