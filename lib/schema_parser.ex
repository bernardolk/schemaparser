defmodule SchemaParser do
  @moduledoc false

  @doc false
  def run do
    {:ok, connPID} =
      Mssqlex.start_link(
        database: "testDb",
        hostname: "localhost",
        instance_name: "SQLEXPRESS2016",
        odbc_driver: "ODBC Driver 17 for SQL Server",
        trusted_connection: "yes"
      )

    # Table schema parsed data. A tuple with :table atom, table index (integer, local), schema definition (list)
    pData = [
      {:table, 0,
       [
         schema: "dbo",
         tablename: "testTable",
         columns: ["Id", "Name", "Bananas", "Group", "Resources", "Beatrice", "Mercury"],
         coltypes: [
           "VARCHAR",
           "INT",
           "VARCHAR",
           "VARCHAR",
           "VARCHAR",
           "VARCHAR",
           "VARCHAR"
         ],
         colmodifiers: [
           is_nullable: ["YES", "YES", "YES", "YES", "YES", "YES", "YES"],
           char_maxlength: ["30", "30", "30", "30", "30", "30", "30"],
           numeric_precision: [nil, nil, nil, nil, nil, nil, nil]
         ]
       ]}
    ]

    qData =
      Enum.map(pData, fn {:table, index, tabledata} ->
        [
          schema: tableschema,
          tablename: tablename,
          columns: _columns,
          coltypes: _coltypes,
          colmodifiers: _colmodifiers
        ] = tabledata

        getTabledataSQL(tableschema, tablename, connPID)
        # with {:ok, data} <- query(connPID, tabledata_cmd) do
        #   {:ok, data}
        #   # {:table, index, [schema: tablename: tablename, qCols]}
        # else
        #   {:error, details} -> {:error, tablename, details}
        # end
      end)

    IO.inspect(qData)
    IO.puts("\n")

    # for each queryed table in database..
    for table <- pData do
      with {:table, index, newschema} = table do
        [
          schema: new_tableschema,
          tablename: new_tablename,
          columns: new_columns,
          coltypes: new_coltypes,
          colmodifiers: new_modifiers
        ] = newschema

        {signal, dbschema} = Enum.at(qData, index)

        IO.puts("\ndbschema:")
        IO.inspect(dbschema)

        with [
               schema: db_tableschema,
               tablename: db_tablename,
               columns: db_columns,
               coltypes: db_coltypes,
               colmodifiers: db_modifiers
             ] = dbschema do
          deletion = checkDeletion(Enum.into(dbschema, %{}), Enum.into(newschema, %{}))
          IO.inspect(deletion)

          ketp_columns = deletion |> Keyword.take([:keep]) |> Keyword.values()

          dbschema = filterSchema(Enum.into(dbschema, %{}), ketp_columns)
          IO.inspect(dbschema)
          # deletionCommands =
          #   deletion
          #   |> Keyword.take([:drop])
          #   |> Keyword.values()

          # dbschema =
          #   deletion
          #   |> Keyword.take([:keep])
          #   |> Keyword.values()

          # creation = checkCreation(tablename, dbschema, newschema)

          # creationCommands =
          #   creation
          #   |> Keyword.take([:create])
          #   |> Keyword.values()

          # aditions =
          #   creation
          #   |> Keyword.take([:add])
          #   |> Keyword.values()

          # dbschema = List.flatten(dbschema, aditions)
          # commands = List.flatten(deletionCommands, creationCommands)

          # # IO.inspect(dbschema)
          # # IO.inspect(commands)

          # dbschema_dc =
          #   dbschema
          #   |> Enum.map(&String.downcase(&1))

          # newschema_dc =
          #   newschema
          #   |> Enum.map(&String.downcase(&1))

          # cond do
          #   dbschema_dc == newschema_dc ->
          #     IO.puts("\nNew Schema:")
          #     IO.inspect(dbschema)
          #     IO.puts("\nExecuted Commands:")
          #     IO.inspect(commands)
          #     # IO.puts("\nSQL Server response:")
          #     # IO.inspect(executeCmds(commands, connPID))

          #   dbschema_dc != newschema_dc ->
          #     commands = dropAndCreate(tablename, newschema)
          #     IO.puts("\nNew Schema:")
          #     IO.inspect(dbschema)
          #     IO.puts("\nExecuted Commands (drop and create):")
          #     IO.inspect(commands)
          #     # IO.puts("\nSQL Server response:")
          #     # IO.inspect(executeCmds(commands, connPID))
          # end
        else
          {:error, tablename, reason} ->
            [odbc_code: odbc_code, message: message] = reason
            IO.inspect(odbc_code)

            case odbc_code do
              :base_table_or_view_not_found ->
                cmds = createTable(tablename, newschema)
                IO.inspect(cmds)
                IO.inspect(executeCmds(cmds, connPID))

              _ ->
                IO.inspect({:error, tablename, message})
            end

          {:table_not_found, message} ->
            IO.inspect(message)
        end
      else
        _ -> {:error, "Parsed file is corrupted (unformatted)."}
      end
    end

    :ok
  end

  defp createTable(tablename, newschema) do
    "create table #{tablename} (" <>
      Enum.reduce(newschema, "", fn col, sql -> sql <> "[#{col}] VARCHAR(30)," end) <> ")"
  end

  # for each column, checks if needs to be deleted in database
  defp checkDeletion(ds, ns) do
    tablename = ds.tablename
    tableschema = ds.schema
    dcolmods = Enum.into(ds.colmodifiers, %{})
    ncolmods = Enum.into(ns.colmodifiers, %{})
    col_n = ds.columns |> Kernel.length()
    IO.inspect(col_n)

    f = &Enum.at(&1, &2)

    Enum.reduce(0..(col_n - 1), [], fn i, acc ->
      colname = f.(ds.columns, i)

      cond do
        f.(ns.columns, i) |> String.downcase() == colname |> String.downcase() &&
          f.(ns.coltypes, i) |> String.downcase() == f.(ds.coltypes, i) |> String.downcase() &&
          f.(ncolmods.is_nullable, i) |> String.downcase() ==
            f.(dcolmods.is_nullable, i) |> String.downcase() &&
          f.(ncolmods.char_maxlength, i) == f.(dcolmods.char_maxlength, i) &&
            f.(ncolmods.numeric_precision, i) == f.(dcolmods.numeric_precision, i) ->
          acc ++ [{:keep, colname}]

        true ->
          acc ++ [{:drop, "alter table #{tableschema}.#{tablename} drop column #{colname}"}]
      end
    end)
  end

  defp filterSchema(schema, kept_columns) do
    tablename = schema.tablename
    tableschema = schema.schema
    columns = schema.columns
    coltypes = schema.coltypes
    colmods = Enum.into(schema.colmodifiers, %{})

    indexes =
      Enum.map(kept_columns, fn column -> Enum.find_index(columns, &(&1 == column)) end)

    # filtered_schema = [schema: tableschema, tablename: tablename, columns: [], coltypes: [], colmodifiers: [is_nullable: [], char_maxlength: [], numeric_precision: []]]

    f_cols = filterListByIndex(columns, indexes)
    f_coltypes = filterListByIndex(coltypes, indexes)
    f_nullable = filterListByIndex(colmods.is_nullable, indexes)
    f_charlength = filterListByIndex(colmods.char_maxlength, indexes)
    f_numprecision = filterListByIndex(colmods.numeric_precision, indexes)

    [
      schema: tableschema,
      tablename: tablename,
      columns: f_cols,
      coltypes: f_coltypes,
      colmodifiers: [
        is_nullable: f_nullable,
        char_maxlength: f_charlength,
        numeric_precision: f_numprecision
      ]
    ]
  end

  defp filterListByIndex(list, filter) when is_list(list) do
    with [] <- filter do
      Enum.map(filter, fn index ->
        Enum.at(list, index)
      end)
    else
      _ ->
        []
    end
  end

  # for each column, checks if needs to be deleted in database
  defp checkCreation(tablename, oldschema, newschema) do
    Enum.map(newschema, fn column ->
      case _toCreate? = Enum.member?(oldschema, column) do
        false ->
          [{:create, "alter table #{tablename} add [#{column}] VARCHAR(30)"}, {:add, column}]

        true ->
          {:ignore, ""}
      end
    end)
    |> List.flatten()
  end

  defp dropAndCreate(tablename, newschema) do
    ["drop table #{tablename}", createTable(tablename, newschema)]
  end

  defp query(connPID, sql) do
    response = Mssqlex.query(connPID, sql, [])

    case response do
      {:ok, _, data} -> {:ok, data}
      {:error, details} -> response
    end
  end

  defp getTabledataSQL(tableschema, tablename, connPID) do
    IO.puts(tablename)
    IO.puts(tableschema)

    fields =
      "COLUMN_NAME, ORDINAL_POSITION, DATA_TYPE, COLUMN_DEFAULT, IS_NULLABLE, " <>
        "CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, DATETIME_PRECISION"

    sql =
      "select #{fields} from INFORMATION_SCHEMA.COLUMNS" <>
        " WHERE TABLE_SCHEMA = '#{tableschema}' AND TABLE_NAME = '#{tablename}'"

    response = query(connPID, sql)

    with {:ok, data} <- response do
      %{:num_rows => num_rows, :columns => columns, :rows => rows} = data

      if num_rows > 0 do
        column_definitions =
          Enum.map(rows, fn row ->
            Enum.zip(columns, row)
          end)
          |> List.flatten()
          |> Enum.map(fn {k, v} -> {:"#{k}", v} end)

        column_names = Keyword.take(column_definitions, [:COLUMN_NAME]) |> Keyword.values()
        column_types = Keyword.take(column_definitions, [:DATA_TYPE]) |> Keyword.values()

        mod_char_maxlength =
          Keyword.take(column_definitions, [:CHARACTER_MAXIMUM_LENGTH]) |> Keyword.values()

        mod_is_nullable = Keyword.take(column_definitions, [:IS_NULLABLE]) |> Keyword.values()

        mod_numeric_precision =
          Keyword.take(column_definitions, [:NUMERIC_PRECISION]) |> Keyword.values()

        {:table,
         [
           schema: tableschema,
           tablename: tablename,
           columns: column_names,
           coltypes: column_types,
           colmodifiers: [
             is_nullable: mod_is_nullable,
             char_maxlength: mod_char_maxlength,
             numeric_precision: mod_numeric_precision
           ]
         ]}
      else
        {:table_not_found, "The table #{tableschema}.#{tablename} wasn't found in the database."}
      end
    else
      {:error, details} -> {:error, details}
    end
  end

  defp executeCmds(sqlCmds, connPID) when is_list(sqlCmds) do
    Enum.map(sqlCmds, fn cmd ->
      query(connPID, cmd)
    end)
  end

  defp executeCmds(sqlCmd, connPID) when is_bitstring(sqlCmd) do
    query(connPID, sqlCmd)
  end
end
