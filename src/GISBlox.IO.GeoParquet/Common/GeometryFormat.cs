namespace GISBlox.IO.GeoParquet.Common
{
   /// <summary>
   /// Indicates the format of geometry data.
   /// </summary>
   public enum GeometryFormat
   {
      /// <summary>
      /// Unknown format.
      /// </summary>
      Unknown = 0,

      /// <summary>
      /// Well-known binary format.
      /// </summary>
      WKB = 1,

      /// <summary>
      /// Well-known text format.
      /// </summary>
      WKT = 2
   }
}
