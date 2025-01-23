namespace GISBlox.IO.GeoParquet.Common
{
   /// <summary>
   /// GeoParquet file metadata.
   /// </summary>
   public class ParquetFileMetadata
   {
      /// <summary>
      /// The columns in the file.
      /// </summary>
      public IDictionary<string, Type> Columns { get; set; } = new Dictionary<string, Type>();

      /// <summary>
      /// The number of row groups in the file.
      /// </summary>
      public int NumRowGroups { get; set; }

      /// <summary>
      /// The number of rows in the file.
      /// </summary>
      public long NumRows { get; set; }

      /// <summary>
      /// The size of the file.
      /// </summary>
      public int Size { get; set; }

      /// <summary>
      /// The parquet version used to generate the file.
      /// </summary>
      public required string Version { get; set; }
   }
}
