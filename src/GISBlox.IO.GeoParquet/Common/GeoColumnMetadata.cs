using System.ComponentModel.DataAnnotations;
using System.Text.Json.Serialization;

namespace GISBlox.IO.GeoParquet.Common
{
   /// <summary> 
   /// GeoParquet column metadata with additional metadata for each geometry column. 
   /// See https://geoparquet.org/releases/v1.1.0/schema.json for more information.
   /// </summary>   
   /// <param name="columnName">Name of the geometry column.</param>
   /// <param name="encoding">Name of the geometry encoding format.</param>
   public class GeoColumnMetadata(string columnName, string encoding)
   {
      /// <summary>
      /// Name of the geometry column. Internal use only.
      /// </summary>
      [JsonIgnore]
      public string ColumnName { get; set; } = columnName;

      /// <summary>
      /// Specifies whether additional properties are allowed in the geometry column.
      /// </summary>
      [JsonPropertyName("additionalProperties")]
      public bool AdditionalProperties { get; set; }

      /// <summary>
      /// Combined bounding box of all the geometries in the column.
      /// </summary>
      [JsonPropertyName("bbox")]
      public double[]? Bbox { get; set; }

      /// <summary>
      /// Optional simplified representations of each geometry. Currently the only supported encoding is "bbox" which specifies the names of bounding box columns.
      /// </summary>
      [JsonPropertyName("covering")]
      public Covering? Covering { get; set; }

      /// <summary>
      /// Name of the coordinate system for the edges.
      /// </summary>
      [JsonPropertyName("edges")]
      [JsonConverter(typeof(JsonStringEnumConverter))]
      public Edges Edges { get; set; }

      /// <summary>
      /// Name of the geometry encoding format.
      /// </summary>
      [JsonPropertyName("encoding")]
      [RegularExpression(@"^(WKB|point|linestring|polygon|multipoint|multilinestring|multipolygon)$")]
      public string Encoding { get; set; } = encoding;

      /// <summary>
      /// Coordinate epoch in case of a dynamic CRS, expressed as a decimal year.
      /// </summary>
      [JsonPropertyName("epoch")]
      public double Epoch { get; set; }

      /// <summary>
      /// List of unique geometry types in the column.
      /// </summary>
      [JsonPropertyName("geometry_types")]
      [Required]
      [RegularExpression(@"^(GeometryCollection|(Multi)?(Point|LineString|Polygon))( Z)?$")]
      public ICollection<string>? GeometryTypes { get; set; }

      /// <summary>
      /// Winding order of exterior ring of polygons. If present must be "counterclockwise"; interior rings are wound in opposite order. 
      /// If absent, no assertions are made regarding the winding order.
      /// </summary>
      [JsonPropertyName("orientation")]
      public string? Orientation { get; set; }
   }
}