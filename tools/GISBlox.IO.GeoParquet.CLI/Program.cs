using System.CommandLine;

namespace GISBlox.IO.GeoParquet.CLI
{
   internal class Program
   {
      static async Task<int> Main(string[] args)
      {
         // Define the Inspect command
         var fileOption = new Option<string>("--file", "Input file path")
         {
            IsRequired = true
         };

         var cmdInspect = new Command("inspect", "Displays the metadata")
         {
            fileOption
         };

         cmdInspect.SetHandler((string file) =>
         {
            Inspect(file);
         }, fileOption);

         // Define the Dump command
         var rowCountOption = new Option<int>("--rows", "Number of rows to display")
         {
            IsRequired = false
         };

         var cmdDump = new Command("dump", "Displays the first few rows of the file")
         {
            fileOption,
            rowCountOption
         };

         cmdDump.SetHandler((string file, int rowCount) =>
         {
            DumpContents(file, rowCount);
         }, fileOption, rowCountOption);

         var rootCommand = new RootCommand
            {
               cmdInspect,
               cmdDump
            };
         rootCommand.Description = "GeoParquet CLI tool";

         // Invoke the command line parser and handle errors
         try
         {
            return await rootCommand.InvokeAsync(args);
         }
         catch (Exception ex)
         {
            Console.Error.WriteLine($"Error: {ex.Message}");
            return 1;
         }
      }

      private static void Inspect(string file)
      {       
         Console.WriteLine($"Listing contents of file: {file}");
       
      }

      private static void DumpContents(string file, int rowCount)
      {
         if (rowCount <= 0)
         {
            rowCount = 10;
         }
         Console.WriteLine($"Dumping {rowCount} rows from file: {file}");
       
      }
   }
}
