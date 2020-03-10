defmodule Parser do
  import NimbleParsec
  import Parser.Helpers

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

  def parse(text) do
    # test = "{
    # dbo.testTwo:
    # FieldOne INT,
    # FieldTwo VARCHAR(20),
    # FieldThree VARCHAR
    # }

    # {
    #   dbo.testOne:
    #   FieldOne INT,
    #   FieldTwo VARCHAR(20),
    #   FieldThree DATETIME

    # }
    # "

    text = String.trim(text)
    tableList = parseTables(text)

    IO.inspect(tableList)

    Enum.map(tableList, fn table ->
      tableindex = Enum.find_index(tableList, &(&1 == table))
      tableschema = Enum.at(table, 0)
      tablename = Enum.at(table, 1)

      {_, tableFields} = Enum.split(table, 2)

      columns = parseFields(tableFields, 0)
      coltypes = parseFields(tableFields, 1)
      colprecisions = parseFields(tableFields, 2)
      colmods = parseFields(tableFields, 3)

      IO.puts "\n HEY"
      IO.inspect colprecisions
      IO.inspect coltypes


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
          IO.inspect precision
          case type do
            "FLOAT" ->
              {value, _r} = Integer.parse(precision)
              {i + 1, acc ++ [value]}

            _ ->
              {i + 1, acc ++ [nil]}
          end
        end)

      IO.inspect(char_ml)

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
  end

  def parseFields(fields, index) do
    {field, rest} = Enum.split(fields, 5)

    case rest do
      [] -> [Enum.at(field, index)]
      _ -> [Enum.at(field, index)] ++ parseFields(rest, index)
    end
  end

  def parseTables(text) do
    result = Parser.table(text)

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
end
