using System.ComponentModel.DataAnnotations;
using System.Text.Json.Serialization;

namespace GISBlox.IO.GeoParquet.Common
{
   /// <summary>
   /// GeoParquet file metadata.
   /// See https://geoparquet.org/releases/v1.1.0/schema.json for more information.
   /// </summary>
   public class GeoFileMetadata
   {
      /// <summary>
      /// The version identifier for the GeoParquet specification.
      /// </summary>
      [JsonPropertyName("version")]
      public required string Version { get; set; }

      /// <summary>
      /// The name of the "primary" geometry column.
      /// </summary>
      [JsonPropertyName("primary_column")]
      [Required]
      public string? Primary_column { get; set; }

      /// <summary>
      /// The metadata for each geometry column. Each key is the name of a geometry column in the table.
      /// </summary>
      [JsonPropertyName("columns")]
      [Required]
      public IDictionary<string, GeoColumnMetadata> Columns { get; set; } = new Dictionary<string, GeoColumnMetadata>();
   }
}