
defmodule Parser.Helpers do
  import NimbleParsec


  def white_space(combinator) do
    combinator |>
    ignore(
      repeat(
        choice([ascii_char([10]), ascii_char([32]), concat(ascii_char([13]), ascii_char([10]))])
      )
    )
  end
end
