using GISBlox.IO.GeoParquet.Common;
using System.CommandLine;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace GISBlox.IO.GeoParquet.CLI
{
   internal class Program
   {
      private static readonly JsonSerializerOptions jsonSerializerOptions = new()
      {
         WriteIndented = true,
         DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
      };

      static async Task<int> Main(string[] args)
      {
         RootCommand rootCommand = new("GeoParquet CLI tool");

         Option<string> fileOption = new("--file", "-f")
         {
            Description = "The file path to the GeoParquet file.",
            Required = true
         };

         rootCommand.Options.Add(fileOption);
         rootCommand.SetAction(parseResult =>
         {
            string? file = parseResult.GetValue(fileOption);            
            Inspect(file);                    
         });

         try
         {
            return rootCommand.Parse(args).Invoke();
         }
         catch (Exception ex)
         {
            Console.Error.WriteLine($"Error: {ex.Message}");
            return 1;
         }         
      }

      private static void Inspect(string? fileName)
      {
         Console.WriteLine("\r\nGeoParquet CLI");
         Console.WriteLine(typeof(Program).Assembly.GetCustomAttribute<AssemblyCopyrightAttribute>()?.Copyright);
         Console.WriteLine($"Version: {typeof(Program).Assembly.GetName().Version}");

         if (string.IsNullOrEmpty(fileName))
         {
            Console.Error.WriteLine("Error: No file specified. Use --file option to specify the GeoParquet file.");
            return;
         }
         
         ParquetFileMetadata metadata = GeoParquetReader.ReadFileMetadata(fileName);
         if (metadata != null)
         {
            using (TableWriter writer = new(padding: 2))
            {
               writer.WriteSection("File metadata", ConsoleColor.Yellow);

               writer.StartNameValueTable("Property", "Value", ConsoleColor.Blue, ConsoleColor.Blue);

               writer.WriteNameValueRow("File name", Path.GetFileName(fileName));
               writer.WriteNameValueRow("Number of row groups", metadata.NumRowGroups);
               writer.WriteNameValueRow("Number of rows", metadata.NumRows);
               writer.WriteNameValueRow("Size", metadata.Size);
               writer.WriteNameValueRow("Version", metadata.Version);

               writer.WriteTable();
            }

            using (TableWriter writer = new(padding: 2))
            {
               writer.WriteSection("Parquet schema", ConsoleColor.Yellow);

               writer.StartNameValueTable("Name", "Type", ConsoleColor.Blue, ConsoleColor.Blue);
               foreach (var column in metadata.Columns)
               {
                  writer.WriteNameValueRow(column.Key, column.Value);
               }
               writer.WriteTable();
            }
         }

         GeoFileMetadata? geoMetadata = GeoParquetReader.ReadGeoMetadata(fileName);
         if (geoMetadata != null)
         {
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine("\r\nGeo metadata\r\n");
            Console.ResetColor();
            Console.WriteLine(JsonSerializer.Serialize(geoMetadata, jsonSerializerOptions));
         }
      }
   }
}
