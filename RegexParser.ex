defmodule Parser do
    # def run(), do: IO.gets("Type the filename (.txt): ") |> String.trim |> readFile |> parseFile
    def run(), do: readFile("schema.txt") |> parseFile

    defp parseFile(filestring) do
        case filestring do
            :error ->  IO.puts "try again \n\n"
                        run()
            _ ->
                openbrackets = Regex.scan(~r{\{}, filestring) |> length
                closedbrackets = Regex.scan(~r{\}}, filestring) |> length

                IO.puts "#{filestring}\n"

                if openbrackets == closedbrackets do
                    IO.puts "Brackets are good! \n"
                    # Regex.scan(~r{\{\s*[a-zA-Z][a-zA-Z0-9]+:\s*([a-zA-Z][a-zA-Z0-9]+[ \t]*,[ \t]*(([a-zA-Z]+ ?[a-zA-Z]+)|([a-zA-Z]+\([0-9]+\))|([a-zA-Z]+))\s*)+\s*\}}, filestring)
                    tables = Regex.scan(~r{\{\s*[a-zA-Z][a-zA-Z0-9]+:\s*([a-zA-Z][a-zA-Z0-9]+[ \t]*,[ \t]*(VARCHAR\([0-9]+\)|VARCHAR\(MAX\)|INT|DATETIME|BIT|REAL|FLOAT|CHAR)\s*)+\s*\}}, filestring)
                            |> getFirstNestedElement()

                    if tables |> length != openbrackets do
                        cond do
                            openbrackets == 0 && closedbrackets == 0 -> IO.puts "There are no table definitions in file"
                            openbrackets == 0 -> IO.puts "No opening brackets found"
                            closedbrackets == 0 -> IO.puts "No closing brackets found"
                            true -> IO.puts "Invalid table definition \n"
                        end
                    else
                        IO.puts "Table definitions looking fine! \n"
                        IO.inspect tables
                        #Extract table names
                        tableNames = tables
                            |> Enum.map(&Regex.run(~r/\{\s*[a-zA-Z][a-zA-Z0-9]+:/, &1))
                            |> getFirstNestedElement()
                            |> Enum.map(&Regex.run(~r{[a-zA-Z][a-zA-Z0-9]+}, &1))
                            |> getFirstNestedElement()
                        #Extract field names and types
                        fields = tables
                            |> Enum.map(&Regex.scan(~r{([a-zA-Z][a-zA-Z0-9]+[ \t]*,[ \t]*(([a-zA-Z]+ +[a-zA-Z]+)|([a-zA-Z]+\([0-9]+\))|([a-zA-Z]+[^(0-9)]))\s*)}, &1))
                            |> Enum.map(&getFirstNestedElement(&1))
                            |> Enum.map(&trimStringList(&1))
                            |> Enum.map(&mapFieldStringList(&1))
                    end
                else
                    cond do
                        openbrackets > closedbrackets -> IO.puts "There are more opening brackets than closing ones in the schema definition file"
                        closedbrackets > openbrackets -> IO.puts "There are more closing brackets than opening ones in the schema definition file"
                    end
                end
        end
    end

    defp readFile(filename) do
        case File.read(filename) do
        {:ok, body} -> body
        {:error, reason} -> IO.puts ~s(Could not open "#{filename}"\n)
                            :error
        end
    end

    defp getFirstNestedElement([h | t]) do
        [nhead | _] = h
        case t do
            [] -> [nhead]
            _ -> [nhead | getFirstNestedElement(t)]
        end
    end

    defp trimStringList([h | t]) do
        case t do
            [] -> [String.trim(h)]
            _ -> [String.trim(h) | trimStringList(t)]
        end
    end

    defp mapFieldStringList([h|t]) do
        case t do
            [] -> [fieldName, fieldvalue] = String.split(h, ~r{[ \t]*,[ \t]*})
            %{:"#{fieldName}" => fieldvalue}
            _ -> [fieldName, fieldvalue] = String.split(h, ~r{[ \t]*,[ \t]*})
                mapFieldStringList(t) |> Map.put(:"#{fieldName}", fieldvalue)
        end
    end
end
