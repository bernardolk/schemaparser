defmodule Parser.Helpers do
  import NimbleParsec

  def whiteSpace(combinator \\ empty()) do
    combinator
    |> repeat(
      choice([ascii_char([10]), ascii_char([32]), concat(ascii_char([13]), ascii_char([10]))])
    )
  end

  def newline(combinator \\ empty()) do
    combinator
    |> choice([ascii_char([10]), concat(ascii_char([13]), ascii_char([10]))])
  end

  def identifier(combinator) do
    combinator
    # |> ascii_char([?a..?z, ?A..?Z])
    |> ascii_string([?a..?z, ?A..?Z], min: 1)
  end

  def spaces(combinator \\ empty()) do
    combinator
    |> repeat(ascii_char([32]))
  end

  def coltype(combinator) do
    combinator
    |> choice([
      concat(string("INT"), empty() |> replace(nil)),
      concat(
        string("VARCHAR"),
        choice([
          ignore(string("("))
          |> ascii_string([?0..?9], min: 1)
          |> ignore(string(")")),
          empty() |> replace(nil)
        ])
      ),
      concat(
        string("DATETIME"),
        choice([
          ignore(string("("))
          |> ascii_string([?0..?9], min: 1)
          |> ignore(string(")")),
          empty() |> replace(nil)
        ])
      ),
      string("FLOAT")
    ])
  end
end
