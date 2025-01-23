namespace GISBlox.IO.GeoParquet.Common
{
   /// <summary>
   /// Defines the spatial extent of a geometry.
   /// </summary>
   public class BBox
   {
      /// <summary>
      /// The maximum value of the X dimension.
      /// </summary>
      public string[]? XMax { get; set; }

      /// <summary>
      /// The minimum value of the X dimension.
      /// </summary>
      public string[]? XMin { get; set; }

      /// <summary>
      /// The maximum value of the Y dimension.
      /// </summary>
      public string[]? YMax { get; set; }

      /// <summary>
      /// The minimum value of the Y dimension.
      /// </summary>
      public string[]? YMin { get; set; }

      /// <summary>
      /// The minimum value of the Z dimension.
      /// </summary>
      public string[]? ZMin { get; set; }

      /// <summary>
      /// The maximum value of the Z dimension.
      /// </summary>
      public string[]? ZMax { get; set; }
   }
}
