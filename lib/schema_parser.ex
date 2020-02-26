defmodule SchemaParser do
  @moduledoc false

  @doc false
  def run do
    {:ok, connPID} = Mssqlex.start_link(
      [ database: "grendene_gerador_programas_12_02_20",
      # username: "test",
      # password: "12345",
      hostname: "localhost",
      instance_name: "SQLEXPRESS2016",
      odbc_driver: "ODBC Driver 17 for SQL Server",
      trusted_connection: "yes"
      ]
    )
    # {token, _, responseData} = Mssqlex.query(connPID, "select top 0 * from UserData.Orders", [])
    # IO.inspect token
    # IO.inspect "responseData"
    # IO.inspect responseData

    # {token, _, responseData} = Mssqlex.query(connPID, "select top 0 * from UserData.Cu", [])
    # IO.inspect token
    # IO.inspect "responseData"
    # IO.inspect responseData

    pCols = ["Name"]

    {token, response} = query(connPID, "select top 0 * from UserData.ResourceGroups")
    case token do
      :ok -> IO.puts "Everything worked!\n"
              getCols(response, pCols)
      :error -> IO.puts "We got an error! \n"
                IO.inspect response
    end

    :ok
  end

  defp query(connPID, sql) do
    response = Mssqlex.query(connPID, sql, [])
    case response do
      {:ok, _, data} -> {:ok, data}
      {:error, reason} -> %{:odbc_code => code} = reason
                            {:error, code}
    end
  end

  def getCols(response, pCols) do
  %{:columns => cols} = response
    IO.inspect cols

    deletedCols = Enum.filter(cols, fn col -> !Enum.member?(pCols, col) end)

    IO.inspect deletedCols

  end

end
