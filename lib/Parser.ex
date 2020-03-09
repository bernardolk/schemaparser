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

    IO.inspect tableList

    Enum.map(tableList, fn table ->
      tableindex = Enum.find_index(tableList, &(&1 == table))
      tableschema = Enum.at(table,0)
      tablename = Enum.at(table,1)

      {_,tableFields} = Enum.split(table,2)

      columns = parseFields(tableFields,0)
      coltypes = parseFields(tableFields,1)
      colprecisions = parseFields(tableFields,2)

      char_ml = []
      num_pr = []

     for type <- coltypes do
        precision = Enum.at(colprecisions,Enum.find_index(coltypes, &(&1==type)))
        case type do
          "VARCHAR" -> char_ml = char_ml ++ [precision]
            num_pr = num_pr ++ [nil]
           "FLOAT" -> num_pr = num_pr ++ [precision]
              char_ml = char_ml ++ [nil]
            _ ->  num_pr = num_pr ++ [nil]
            char_ml = char_ml ++ [nil]
        end
      end


      {:table, tableindex,
      %{
        schema: tableschema,
        tablename: tablename,
        columns: columns,
        coltypes: coltypes,
        colmodifiers: [
          is_nullable: ["NOT NULL", "NOT NULL", "NOT NULL"],
          char_maxlength: char_ml,
          numeric_precision: num_pr
        ]
      }}

    end
    )
  end

  def parseFields(fields, index) do
    {field, rest} = Enum.split(fields, 4)
    case rest do
      [] -> [Enum.at(field, index)]
      _ -> [Enum.at(field, index)] ++ parseFields(rest,index)
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
