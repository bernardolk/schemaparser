defmodule Parser do
  import NimbleParsec
  import Parser.Helpers

  defparsec(
    :table,
    empty()
    |> string("{")
    |> (&ignore(&1, whiteSpace())).()
    |> identifier()
    |> string(":")
    |> newline()
    |> (&ignore(&1, whiteSpace())).()
    |> identifier()
    |> string(" ")
    |> optional(spaces())
    |> coltype()
    |> optional(spaces())
    |> string(",")
    |> optional(spaces())
    |> newline()
    |> #optional(
      repeat(
        ignore(whiteSpace())
        |> identifier()
        |> string(" ")
        |> optional(spaces())
        |> coltype()
        |> optional(spaces())
        |> string(",")
        |> optional(spaces())
        |> newline()
      #)
    )
  |> string("}")
  )

  # |> (&(replace(whiteSpace(&1),"\n"))).()
  # |> identifier()
  # |> coltype()
  # |> string("EO")

  # |> lookahead_not()

  def main() do
    test = "{test2:
    Field1 INT,
    Field2 VARCHAR(20),
    Field3 DATETIME}"

    Parser.table(test)
  end
end
