using System.ComponentModel.DataAnnotations;
using System.Text.Json.Serialization;

namespace GISBlox.IO.GeoParquet.Common
{
   /// <summary>
   /// Simplified representation of each geometry. Currently the only supported encoding is "bbox" which specifies the names of bounding box columns.
   /// </summary>
   public partial class Covering
   {
      /// <summary>
      /// The spatial extent of the geometries.
      /// </summary>
      [JsonPropertyName("bbox")]
      [Required]
      public BBox Bbox { get; set; } = new BBox();
   }
}
