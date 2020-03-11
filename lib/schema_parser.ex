defmodule SchemaParser do
  @moduledoc false
  import NimbleParsec
  import SchemaParser.Helpers

  defparsec(
    :tables,
    eventually(string("{"))
    |> eventually(string("}"))
  )

  defparsec(
    :table,
    ignore(optional(whiteSpace()))
    |> ignore(string("{"))
    |> ignore(optional(whiteSpace()))
    |> identifier()
    |> ignore(string("."))
    |> identifier()
    |> ignore(string(":"))
    |> ignore(newline())
    |> ignore(optional(whiteSpace()))
    |> identifier()
    |> ignore(string(" "))
    |> ignore(optional(spaces()))
    |> coltype()
    |> ignore(optional(spaces()))
    |> colmod()
    |> ignore(optional(spaces()))
    |> optional(
      repeat(
        string(",")
        |> ignore(optional(spaces()))
        |> ignore(whiteSpace())
        |> identifier()
        |> ignore(string(" "))
        |> ignore(optional(spaces()))
        |> coltype()
        |> ignore(optional(spaces()))
        |> colmod()
        |> ignore(optional(spaces()))
      )
    )
    |> ignore(optional(whiteSpace()))
    |> ignore(string("}"))
  )

  def parse() do
    raw_text = readfile("schema.txt")
    text = String.trim(raw_text)
    tableList = parseTables(text)

    IO.inspect(tableList)

  parsedTablesList = Enum.map(tableList, fn table ->
      tableindex = Enum.find_index(tableList, &(&1 == table))
      tableschema = Enum.at(table, 0)
      tablename = Enum.at(table, 1)

      {_, tableFields} = Enum.split(table, 2)

      columns = parseFields(tableFields, 0)
      coltypes = parseFields(tableFields, 1)
      colprecisions = parseFields(tableFields, 2)
      colmods = parseFields(tableFields, 3)

      {_i, char_ml} =
        Enum.reduce(coltypes, {0, []}, fn type, {i, acc} ->
          precision = Enum.at(colprecisions, i)
          IO.inspect precision
          case type do
            "VARCHAR" ->
              {value, _r} = Integer.parse(precision)
              {i + 1, acc ++ [value]}

            _ ->
              {i + 1, acc ++ [nil]}
          end
        end)

      {_i, num_pr} =
        Enum.reduce(coltypes, {0, []}, fn type, {i, acc} ->
          precision = Enum.at(colprecisions, i)
          case type do
            "FLOAT" ->
              {value, _r} = Integer.parse(precision)
              {i + 1, acc ++ [value]}

            _ ->
              {i + 1, acc ++ [nil]}
          end
        end)

      {:table, tableindex,
       %{
         schema: tableschema,
         tablename: tablename,
         columns: columns,
         coltypes: coltypes,
         colmodifiers: [
           is_nullable: colmods,
           char_maxlength: char_ml,
           numeric_precision: num_pr
         ]
       }}
    end)

    resultCmds = run(parsedTablesList)
    File.write("sqlcmds.txt", inspect(resultCmds))
    File.write("sqlcmds_bin.txt", :erlang.term_to_binary(resultCmds))

  end

  def parseFields(fields, index) do
    {field, rest} = Enum.split(fields, 5)

    case rest do
      [] -> [Enum.at(field, index)]
      _ -> [Enum.at(field, index)] ++ parseFields(rest, index)
    end
  end

  def parseTables(text) do
    result = table(text)

    case result do
      {:ok, table, rest, _map, _tup, _int} ->
        case rest do
          "" -> [table]
          _ -> [table] ++ parseTables(rest)
        end

      {:error, message, rest, _map, _tup, _int} ->
        {:error, message, rest}
    end
  end

  def readfile(path) do
    case File.read(path) do
      {:ok, text} -> text
      {:error, message} -> message
      _ -> :wtf
    end
  end

  @doc false
  def run(parsed_schema_data) do
    {:ok, connPID} =
      Mssqlex.start_link(
        database: "testDb",
        hostname: "localhost",
        instance_name: "SQLEXPRESS2016",
        odbc_driver: "ODBC Driver 17 for SQL Server",
        trusted_connection: "yes"
      )

    # Table schema parsed data. A tuple with :table atom, table index (integer, local), schema definition (list)
    # TO KEEP IN MIND !!
    # DEFAULT VALUES FOR CHAR_MAXLENGHT: 1, NUMERIC_PRECISION: 10
    # numeric precision, cannot be set when creating the column. It can only be read from (but in this case, just checking for INT do the trick)
    # right now, for INT's it is dropping and creating them because when it checks the db, numeric precision is 10 and when i set it i gotta set it to nil
    # parsed_schema_data = [
    #   {:table, 0,
    #    %{
    #      schema: "dbo",
    #      tablename: "testTable",
    #      columns: ["Hakuna Matata", "ExampleField"],
    #      coltypes: [
    #        "VARCHAR",
    #        "VARCHAR"
    #      ],
    #      colmodifiers: [
    #        is_nullable: ["NOT NULL", "NOT NULL"],
    #        char_maxlength: [25, 1],
    #        numeric_precision: [nil, nil]
    #      ]
    #    }}
    # ]

    IO.inspect(parsed_schema_data)
    IO.puts("\n")

    db_schema_data =
      Enum.map(parsed_schema_data, fn {:table, index, tabledata} ->
        getTabledataSQL(tabledata.schema, tabledata.tablename, connPID)
      end)

    IO.inspect(db_schema_data)
    IO.puts("\n")

    # for each table definition found in text file...
    commands = Enum.map(parsed_schema_data,fn data_entry ->
      # gets the table definition (new_table_schema)
      with {:table, index, new_table_schema} <- data_entry do
        %{
          schema: new_tableschema,
          tablename: new_tablename,
          columns: new_columns,
          coltypes: new_coltypes,
          colmodifiers: new_modifiers
        } = new_table_schema

        {signal, db_table_schema} = Enum.at(db_schema_data, index)

        IO.puts("\ndb_table_schema:")
        IO.inspect(db_table_schema)
        # gets the correspondent database table schema
        # if it fails to match, either create a new table or
        # process error
        with {:table,
              %{
                schema: db_tableschema,
                tablename: db_tablename,
                columns: db_columns,
                coltypes: db_coltypes,
                colmodifiers: db_modifiers
              }} <- {signal, db_table_schema} do
          deletion = checkDeletion(db_table_schema, new_table_schema)
          IO.puts("\n result from checkDeletion: ")
          IO.inspect(deletion)

          kept_columns = Keyword.get_values(deletion, :keep)
          del_cmds = Keyword.get_values(deletion, :drop)

          IO.puts("kept columns")
          IO.inspect(kept_columns)

          db_table_schema = filterSchema(db_table_schema, kept_columns)
          IO.inspect(db_table_schema)

          IO.puts("\ndel Cmds:")
          IO.inspect(del_cmds)

          case kept_columns do
            [] ->
              getDropAndCreateCmds(new_table_schema)
              # IO.inspect(dcsql)
              # IO.inspect(executeCmds(dcsql, connPID))

            _ ->
              creation = checkColumnCreation(db_table_schema, new_table_schema)
              IO.inspect(creation)

              add_cmds = Keyword.get_values(creation, :create)

              del_cmds ++ add_cmds
              # IO.inspect(cmd)

              # ans = executeCmds(cmd, connPID)
              # IO.inspect(ans)
          end
        else
          {:error, tablename, reason} ->
            handleError(tablename, reason)

          {:table_not_found, message} ->
            IO.inspect(message)
            getCreateTableCmds(new_table_schema)
            # IO.inspect(ccmd)
            # IO.inspect(ccmd |> executeCmds(connPID))

          _ ->
            IO.inspect({signal, db_table_schema})
        end
      else
        _ -> {:error, "Parsed file is corrupted (unformatted)."}
      end
    end)

    List.flatten(commands)

    # :ok
  end

  defp query(connPID, sql) do
    response = Mssqlex.query(connPID, sql, [])

    case response do
      {:ok, _, data} -> {:ok, data}
      {:error, _details} -> response
    end
  end

  defp handleError(tablename, reason) do
    [odbc_code: odbc_code, message: message] = reason
    IO.inspect(odbc_code)

    case odbc_code do
      :base_table_or_view_not_found ->
        # cmds = getCreateTableCmds(new_table_schema)
        # IO.inspect(cmds)
        # IO.inspect(executeCmds(cmds, connPID))
        IO.puts("error 717: base table or view not found")

      _ ->
        IO.inspect({:error, tablename, message})
    end
  end


  defp getDropAndCreateCmds(new_table_schema) do
    tablename = new_table_schema.tablename
    schema = new_table_schema.schema

    ["drop table [#{schema}].[#{tablename}]", getCreateTableCmds(new_table_schema)]
  end


  defp getCreateTableCmds(new_table_schema) do
    ns = Enum.into(new_table_schema, %{})
    tablename = ns.tablename
    schema = ns.schema
    ns_mods = Enum.into(ns.colmodifiers, %{})

    IO.inspect(ns_mods)

    # special modifier (related to column types)
    ml = ns_mods[:char_maxlength]
    np = ns_mods[:numeric_precision]
    # removes from modifiers list so won't be concatenated as a regular modifier
    ns_mods = Map.delete(ns_mods, :char_maxlength)
    ns_mods = Map.delete(ns_mods, :numeric_precision)

    IO.inspect(ns_mods)

    columns =
      Enum.reduce(ns.columns, "", fn column, acc ->
        ns_col_index = Enum.find_index(ns.columns, &(&1 == column))
        coltype = Enum.at(ns.coltypes, ns_col_index)
        col_ml = Enum.at(ml, ns_col_index)
        col_np = Enum.at(np, ns_col_index)

        typemod =
          cond do
            col_ml != nil ->
              "(#{col_ml})"

            col_np != nil ->
              "(#{col_np})"

            true ->
              ""
          end

        colmods =
          Enum.reduce(ns_mods, "", fn {_mod, modlist}, acc2 ->
            col_mod = Enum.at(modlist, ns_col_index)

            case col_mod do
              nil -> acc2
              _ -> acc2 <> "#{col_mod} "
            end
          end)

        acc <> " [#{column}] #{coltype}#{typemod} #{colmods},"
      end)

    # returns the create table SQL query
    "create table [#{schema}].[#{tablename}] (" <> columns <> ")"
  end

  # for each column, checks if needs to be deleted in database
  defp checkDeletion(ds, ns) do
    tablename = ds.tablename
    tableschema = ds.schema
    dcolmods = Enum.into(ds.colmodifiers, %{})
    ncolmods = Enum.into(ns.colmodifiers, %{})
    col_n = length(ds.columns)

    # alias
    f = &Enum.at(&1, &2)

    # for each column in database, checks if it should be deleted and returns list of columns to keep
    Enum.reduce(0..(col_n - 1), [], fn ds_col_i, acc ->
      colname = f.(ds.columns, ds_col_i)
      newschema_column_index = Enum.find_index(ns.columns, &(&1 == colname))

      IO.puts("\nfound #{colname} from db in newschema??")
      IO.puts("#{newschema_column_index != nil}")

      case newschema_column_index do
        nil ->
          acc ++ [{:drop, "alter table [#{tableschema}].[#{tablename}] drop column [#{colname}]"}]

        ns_col_i ->
          mod1_ns = Enum.at(ncolmods.is_nullable, ns_col_i)
          mod2_ns = Enum.at(ncolmods.numeric_precision, ns_col_i)
          mod3_ns = Enum.at(ncolmods.char_maxlength, ns_col_i)

          mod1_ds = Enum.at(dcolmods.is_nullable, ds_col_i)
          mod2_ds = Enum.at(dcolmods.numeric_precision, ds_col_i)
          mod3_ds = Enum.at(dcolmods.char_maxlength, ds_col_i)

          col_type_ds = Enum.at(ds.coltypes, ds_col_i) |> String.downcase()
          col_type_ns = Enum.at(ns.coltypes, ns_col_i) |> String.downcase()

          # overloads numeric precision if INT
          mod2_ns =
            cond do
              col_type_ns == "int" -> nil
              true -> mod2_ns
            end

          mod2_ds =
            cond do
              col_type_ds == "int" -> nil
              true -> mod2_ds
            end

          IO.inspect([
            mod1_ds,
            mod1_ns,
            mod2_ds,
            mod2_ns,
            mod3_ds,
            mod3_ns,
            col_type_ds,
            col_type_ns
          ])

          # conditions for deletion of column
          cond do
            mod1_ds == mod1_ns && mod2_ds == mod2_ns && mod3_ds == mod3_ns &&
                col_type_ds == col_type_ns ->
              acc ++ [{:keep, colname}]

            true ->
              acc ++
                [{:drop, "alter table [#{tableschema}].[#{tablename}] drop column [#{colname}]"}]
          end
      end
    end)
  end

  defp filterSchema(schema, kept_columns) do
    tablename = schema.tablename
    tableschema = schema.schema
    columns = schema.columns
    coltypes = schema.coltypes
    colmods = Enum.into(schema.colmodifiers, %{})

    # indexes of filtered columns
    indexes = Enum.map(kept_columns, fn column -> Enum.find_index(columns, &(&1 == column)) end)

    f_coltypes = filterListByIndex(coltypes, indexes)
    f_nullable = filterListByIndex(colmods.is_nullable, indexes)
    f_charlength = filterListByIndex(colmods.char_maxlength, indexes)
    f_numprecision = filterListByIndex(colmods.numeric_precision, indexes)

    # return value
    %{
      schema: tableschema,
      tablename: tablename,
      columns: kept_columns,
      coltypes: f_coltypes,
      colmodifiers: [
        is_nullable: f_nullable,
        char_maxlength: f_charlength,
        numeric_precision: f_numprecision
      ]
    }
  end

  defp filterListByIndex(list, filter) when is_list(list) do
    with [] <- filter do
      list
    else
      _ ->
        Enum.map(filter, fn index ->
          Enum.at(list, index)
        end)
    end
  end

  defp checkColumnCreation(ds, ns) do
    # this function expects to be called after checkColumnDeletion
    # so that any column type/mod checking is made prior on the checkColumnDeletion function
    # and now we just need to create what's missing
    tablename = ds.tablename
    schema = ds.schema
    ns_mods = Enum.into(ns.colmodifiers, %{})
    # special modifier (related to column types)
    ml = ns_mods[:char_maxlength]
    np = ns_mods[:numeric_precision]
    # removes from modifiers list so won't be concatenated as a regular modifier
    ns_mods = Map.delete(ns_mods, :char_maxlength)
    ns_mods = Map.delete(ns_mods, :numeric_precision)

    IO.puts("\nds columns: ")
    IO.inspect(ds.columns)
    IO.puts("\nns columns: ")
    IO.inspect(ns.columns)

    # return value
    Enum.reduce(ns.columns, [], fn column, acc ->
      ns_col_index = Enum.find_index(ns.columns, &(&1 == column))
      coltype = Enum.at(ns.coltypes, ns_col_index)

      case _toCreate? = Enum.member?(ds.columns, column) do
        false ->
          col_ml = Enum.at(ml, ns_col_index)
          col_np = Enum.at(np, ns_col_index)

          typemod =
            cond do
              col_ml != nil -> s_ml = Integer.to_string(col_ml)
                "(#{s_ml})"

              col_np != nil -> s_np = Integer.to_string(col_np)
                "(#{s_np})"

              true ->
                ""
            end

          # load column modifiers
          colmods =
            Enum.reduce(ns_mods, [], fn {_mod, modlist}, acc ->
              col_mod = Enum.at(modlist, ns_col_index)

              case col_mod do
                nil -> acc
                _ -> acc ++ [" #{col_mod}"]
              end
            end)

          # reduce return value
          acc ++
            [
              {:create,
               "alter table [#{schema}].[#{tablename}] add [#{column}] #{coltype}#{typemod} #{
                 colmods
               }"},
              {:add, column}
            ]

        # reduce return value
        true ->
          acc ++ [{:ignore, "#{column}"}]
      end
    end)
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
      # %{:num_rows => num_rows, :columns => columns, :rows => rows} = data
      num_rows = data.num_rows
      columns = data.columns
      rows = data.rows

      if num_rows > 0 do
        # create map of column definitions
        column_definitions =
          Enum.map(rows, fn row ->
            Enum.zip(columns, row)
          end)
          |> List.flatten()
          |> Enum.map(fn {k, v} -> {:"#{k}", v} end)

        column_names = Keyword.get_values(column_definitions, :COLUMN_NAME)
        column_types = Keyword.get_values(column_definitions, :DATA_TYPE)
        mod_char_maxlength = Keyword.get_values(column_definitions, :CHARACTER_MAXIMUM_LENGTH)
        mod_is_nullable = Keyword.get_values(column_definitions, :IS_NULLABLE)
        mod_numeric_precision = Keyword.get_values(column_definitions, :NUMERIC_PRECISION)

        # converts YES to NULL and NO to NOT NULL
        mod_is_nullable =
          Enum.map(mod_is_nullable, fn mod_item ->
            case mod_item do
              "YES" -> "NULL"
              "NO" -> "NOT NULL"
            end
          end)

        # return value
        {:table,
         %{
           schema: tableschema,
           tablename: tablename,
           columns: column_names,
           coltypes: column_types,
           colmodifiers: [
             is_nullable: mod_is_nullable,
             char_maxlength: mod_char_maxlength,
             numeric_precision: mod_numeric_precision
           ]
         }}
      else
        {:table_not_found, "The table #{tableschema}.#{tablename} wasn't found in the database."}
      end
    else
      {:error, details} -> {:error, details}
    end
  end

  def executeCmds() do
    {:ok, connPID} =
      Mssqlex.start_link(
        database: "testDb",
        hostname: "localhost",
        instance_name: "SQLEXPRESS2016",
        odbc_driver: "ODBC Driver 17 for SQL Server",
        trusted_connection: "yes"
      )
      {:ok, read} = File.read("sqlcmds_bin.txt")
      sqlCmds = :erlang.binary_to_term(read)

    Enum.map(sqlCmds, fn cmd ->
      query(connPID, cmd)
    end)
  end

  # def executeCmds(sqlCmd) when is_bitstring(sqlCmd) do
  #   {:ok, connPID} =
  #     Mssqlex.start_link(
  #       database: "testDb",
  #       hostname: "localhost",
  #       instance_name: "SQLEXPRESS2016",
  #       odbc_driver: "ODBC Driver 17 for SQL Server",
  #       trusted_connection: "yes"
  #     )
  #   query(connPID, sqlCmd)
  # end
end
