defmodule Parser.Helpers do
  import NimbleParsec

  def whiteSpace(combinator \\ empty()) do
    combinator
    |> repeat(
      choice([ascii_char([10]), ascii_char([32]), concat(ascii_char([13]), ascii_char([10]))])
    )
  end

  def newline(combinator) do
    combinator
    |> choice([ascii_char([10]), concat(ascii_char([13]), ascii_char([10]))])
  end

  def identifier(combinator) do
    combinator
    |> ascii_char([?a..?z, ?A..?Z])
    |> repeat(ascii_char([?a..?z, ?A..?Z, ?0..?9]))
  end

  def spaces(combinator \\ empty()) do
    combinator
    |> repeat(ascii_char([32]))
  end

  def coltype(combinator) do
    combinator
    |> choice([
      string("INT"),
      concat(
        string("VARCHAR"),
        optional(
          string("(")
          |> repeat(ascii_char([?0..?9]))
          |> string(")")
        )
      ),
      concat(
        string("DATETIME"),
        optional(
          string("(")
          |> repeat(ascii_char([?0..?9]))
          |> string(")")
        )
      ),
      string("FLOAT")
    ])
  end
end
