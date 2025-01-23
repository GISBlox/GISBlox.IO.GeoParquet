using GISBlox.IO.GeoParquet.Common;
using GISBlox.IO.GeoParquet.Extensions;
using NetTopologySuite.Geometries;
using NetTopologySuite.IO;
using System.Data;

namespace GISBlox.IO.GeoParquet.Utils
{
   /// <summary>
   /// Helper class to transform WKB geometries to WKT.
   /// </summary>
   /// <param name="dataTable">A <see cref="DataTable"/>.</param>
   internal class WKTColumnWriter(DataTable dataTable)
   {
      private readonly DataTable _dataTable = dataTable ?? throw new ArgumentNullException(nameof(dataTable));

      /// <summary>
      /// Transforms all WKB geometries in a <see cref="DataTable"/> into WKT geometries.
      /// </summary>
      /// <param name="batchSize">The amount of rows to process per batch. Used to reduce memory stress for large tables.</param>
      public void TransformGeometries(int batchSize)
      {
         var wkbReader = new WKBReader();
         var wktWriter = new WKTWriter();

         var wkbColumns = _dataTable.GetGeoColumns(GeometryFormat.WKB);

         foreach (DataColumn wkbColumn in wkbColumns)
         {
            // Add a column to the data table that will contain the transformed WKB geometries
            DataColumn wktColumn = AddWktColumn(wkbColumn, wkbColumn.Ordinal);

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

                     // Write WKT geometry
                     row[wktColumn] = wktWriter.Write(geometry);
                  }
               }
            }

            // Remove the original WKB column from the data table
            _dataTable.Columns.Remove(wkbColumn);
            wktColumn.RenameColumn(wkbColumn.ColumnName);
         }
      }

      /// <summary>
      /// Adds a WKT column to the current data table.
      /// </summary>
      /// <param name="wkbColumn">The WKB <see cref="DataColumn"/> that it will replace.</param>
      /// <param name="ordinalPosition">The ordinal position of the new column.</param>
      /// <returns></returns>
      private DataColumn AddWktColumn(DataColumn wkbColumn, int ordinalPosition)
      {
         string wktColumnName = $"{wkbColumn.ColumnName}_WKT";
         DataColumn wktColumn = _dataTable.AddGeoColumn(wktColumnName, ordinalPosition, GeometryFormat.WKT);

         // If the WKB column is the primary geometry column, mark the WKT column as the primary one as well 
         if (wkbColumn.IsPrimaryGeoColumn())
         {
            wktColumn.SetAsPrimaryGeoColumn();
         }

         return wktColumn;
      }
   }
}
