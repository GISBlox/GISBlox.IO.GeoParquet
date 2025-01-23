using NetTopologySuite.Geometries;
using NetTopologySuite.IO;
using System.Data;

namespace GISBlox.IO.GeoParquet.Utils
{
   /// <summary>
   /// Helper class to transform and enumerate geometries in a <see cref="DataTable"/>.
   /// </summary>
   /// <param name="dataTable">A <see cref="DataTable"/>.</param>
   internal class WKBColumnWriter(DataTable dataTable)
   {
      private readonly DataTable _dataTable = dataTable ?? throw new ArgumentNullException(nameof(dataTable));
      private readonly HashSet<string> _uniqueGeometryTypes = [];
      private readonly Envelope _combinedBBox = new();

      /// <summary>
      /// Returns the combined bounding box of all geometries in the geometry column.
      /// </summary>
      public double[]? Bbox
      {
         get
         {
            return [_combinedBBox.MinX, _combinedBBox.MinY, _combinedBBox.MaxX, _combinedBBox.MaxY];
         }
      }

      /// <summary>
      /// Returns the unique geometry types found in the geometry column.
      /// </summary>
      public ICollection<string> GeometryTypes
      {
         get
         {
            return (ICollection<string>)_uniqueGeometryTypes.AsEnumerable();
         }
      }

      /// <summary>
      /// Transforms WKT geometries in a <see cref="DataTable"/> into WKB geometries.
      /// </summary>      
      /// <param name="wktColumn">The <see cref="DataColumn"/> that contains the WKT geometries.</param>
      /// <param name="wkbColumn">The target <see cref="DataColumn"/> for the WKB geometries.</param>
      /// <param name="batchSize">The amount of rows to process per batch. Used to reduce memory stress for large tables.</param>
      public void TransformWktGeometries(DataColumn wktColumn, DataColumn wkbColumn, int batchSize)
      {
         var wktReader = new WKTReader();
         var wkbWriter = new WKBWriter();

         _uniqueGeometryTypes.Clear();
         _combinedBBox.Init();

         int totalRows = _dataTable.Rows.Count;
         for (int batchStart = 0; batchStart < totalRows; batchStart += batchSize)
         {
            var rows = _dataTable.AsEnumerable().Skip(batchStart).Take(batchSize);
            foreach (var row in rows)
            {
               // Parse WKT geometry
               string? wkt = row[wktColumn].ToString();
               if (!string.IsNullOrEmpty(wkt))
               {
                  Geometry geometry = wktReader.Read(wkt);

                  // Get geometry type
                  _uniqueGeometryTypes.Add(geometry.GeometryType);

                  // Expand combined bounding box with the geometry's envelope
                  _combinedBBox.ExpandToInclude(geometry.EnvelopeInternal);

                  // Write WKB geometry
                  row[wkbColumn] = wkbWriter.Write(geometry);
               }
            }
         }
      }

      /// <summary>
      /// Enumerates WKB geometries in a <see cref="DataTable"/> to get unique geometry types and a combined bounding box.
      /// </summary>
      /// <param name="wkbColumn">The <see cref="DataColumn"/> that contains the WKB geometries.</param>
      /// <param name="batchSize">The amount of rows to process per batch. Used to reduce memory stress for large tables.</param>
      public void EnumerateWkbGeometries(DataColumn wkbColumn, int batchSize)
      {
         var wkbReader = new WKBReader();

         _uniqueGeometryTypes.Clear();
         _combinedBBox.Init();

         int totalRows = _dataTable.Rows.Count;
         for (int batchStart = 0; batchStart < totalRows; batchStart += batchSize)
         {
            var rows = _dataTable.AsEnumerable().Skip(batchStart).Take(batchSize);
            foreach (var row in rows)
            {
               // Parse WKB geometry
               if (row[wkbColumn] is byte[] wkb)
               {
                  Geometry geometry = wkbReader.Read(wkb);

                  // Get geometry type
                  _uniqueGeometryTypes.Add(geometry.GeometryType);

                  // Expand combined bounding box with the geometry's envelope
                  _combinedBBox.ExpandToInclude(geometry.EnvelopeInternal);
               }
            }
         }
      }
   }
}