defmodule Parser do
  import NimbleParsec
  import Parser.Helpers

  defparsec :tablename,
    empty()
    |> ignore(string("{"))
    # |> optional(repeat(string(" ")))
    |> white_space()
    |> ascii_char([?a..?z, ?A..?Z])
    |> repeat(ascii_char([?a..?z, ?A..?Z, ?0..?9]))
    |> ignore(string(":"))
    |> white_space()
    |> ignore(string("ZUU"))


  # |> lookahead_not()

  def main() do
    test = "{
        test2:

          ZUU}"

    Parser.tablename(test)
  end
end
