using GISBlox.IO.GeoParquet.Common;
using GISBlox.IO.GeoParquet.Extensions;
using System.Data;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace GISBlox.IO.GeoParquet.Utils
{
   /// <summary>
   /// Creates GeoParquet metadata based on a <see cref="DataTable"/>.
   /// </summary>
   /// <param name="dataTable">A <see cref="DataTable"/>.</param>
   internal class MetadataBuilder(DataTable dataTable)
   {
      private int _batchSize;
      private readonly DataTable _dataTable = dataTable ?? throw new ArgumentNullException(nameof(dataTable));

      private static readonly JsonSerializerOptions serializerOptions = new()
      {
         DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
      };

      /// <summary>
      /// Generates GeoParquet metadata for the current <see cref="DataTable"/>.
      /// </summary>
      /// <param name="batchSize">The amount of <see cref="DataTable"/> rows to process per operation.</param>
      /// <returns>A Dictionary with GeoParquet metadata.</returns>
      /// <exception cref="ArgumentException"></exception>
      public Dictionary<string, string> BuildGeoParquetMetadata(int batchSize)
      {
         _batchSize = batchSize;

         // Create GeoParquet column metadata for each geometry column in the data table
         var columnMetadata = GenerateColumnMetadata();

         // Create the GeoParquet file metadata and add the column metadata
         GeoFileMetadata fileMetadata = new()
         {
            Version = "1.1.0",
            Primary_column = _dataTable.GetPrimaryGeoColumnName(),
            Columns = columnMetadata.ToDictionary(column => column.ColumnName, column => column)
         };

         // Serialize the resulting metadata to JSON and return it as key-value metadata 
         string json = JsonSerializer.Serialize(fileMetadata, serializerOptions);
         return new Dictionary<string, string>
         {
            ["geo"] = json
         };         
      }

      /// <summary>
      /// Creates GeoParquet column metadata for each geometry column in a <see cref="DataTable"/>.
      /// Encodes WKT geometries into their WKB representation, if applicable.
      /// Note: a <see cref="DataTable"/> should either contain WKT or WKB geometries, not both.
      /// </summary>      
      /// <returns>A List with <see cref="GeoColumnMetadata"/> types that contain the GeoParquet column metadata.</returns>
      private List<GeoColumnMetadata> GenerateColumnMetadata()
      {
         List<GeoColumnMetadata> metadataColumns = [];

         List<DataColumn> wktColumns = _dataTable.GetGeoColumns(GeometryFormat.WKT);
         List<DataColumn> wkbColumns = _dataTable.GetGeoColumns(GeometryFormat.WKB);

         if (wktColumns.Count == 0 && wkbColumns.Count == 0)
         {
            throw new GeometryException("No geometry columns found in the data table.");
         }

         if (wktColumns.Count > 0 && wkbColumns.Count > 0)
         {
            throw new GeometryException("A DataTable should either contain WKT or WKB geometries, not both.");
         }

         if (wktColumns.Count > 0)
         {
            metadataColumns = GenerateWktColumnMetadata(wktColumns);
         }

         if (wkbColumns.Count > 0)
         {
            metadataColumns = GenerateWkbColumnMetadata(wkbColumns);
         }

         return metadataColumns;
      }

      /// <summary>
      /// Creates GeoParquet column metadata for WKT geometry columns in a <see cref="DataTable"/>.
      /// Additionally encodes the WKT geometries into their WKB representation.
      /// </summary>
      /// <param name="wktColumns">A List with WKT geometries in a <see cref="DataColumn"/>.</param>
      /// <returns>A List with <see cref="GeoColumnMetadata"/> types that contain the GeoParquet column metadata.</returns>
      private List<GeoColumnMetadata> GenerateWktColumnMetadata(List<DataColumn> wktColumns)
      {
         List<GeoColumnMetadata> metadataColumns = [];
         WKBColumnWriter wkbColumnWriter = new(_dataTable);

         foreach (DataColumn wktColumn in wktColumns)
         {
            // Add a column to the data table that will contain the transformed WKT geometries
            DataColumn wkbColumn = AddWkbColumn(wktColumn, wktColumn.Ordinal);

            // Create the GeoParquet column metadata for the new column
            GeoColumnMetadata columnMetadata = new(columnName: wktColumn.ColumnName, encoding: "WKB");

            // Transform the geometries in the WKT column into their WKB representation              
            wkbColumnWriter.TransformWktGeometries(wktColumn, wkbColumn, _batchSize);
            columnMetadata.Bbox = wkbColumnWriter.Bbox;
            columnMetadata.GeometryTypes = wkbColumnWriter.GeometryTypes;

            // Add the metadata column to the list of GeoParquet metadata columns
            metadataColumns.Add(columnMetadata);

            // Remove the original WKT column from the data table
            _dataTable.Columns.Remove(wktColumn);
            wkbColumn.RenameColumn(wktColumn.ColumnName);
         }

         return metadataColumns;
      }

      /// <summary>
      /// Creates GeoParquet column metadata for WKB geometry columns in a <see cref="DataTable"/>.
      /// </summary>
      /// <param name="wkbColumns">A List with WKB geometries in a <see cref="DataColumn"/>.</param>
      /// <returns>A List with <see cref="GeoColumnMetadata"/> types that contain the GeoParquet column metadata.</returns>
      private List<GeoColumnMetadata> GenerateWkbColumnMetadata(List<DataColumn> wkbColumns)
      {
         List<GeoColumnMetadata> metadataColumns = [];
         WKBColumnWriter wkbColumnWriter = new(_dataTable);

         foreach (DataColumn wkbColumn in wkbColumns)
         {
            // Create the GeoParquet column metadata
            GeoColumnMetadata columnMetadata = new(columnName: wkbColumn.ColumnName, encoding: "WKB");

            wkbColumnWriter.EnumerateWkbGeometries(wkbColumn, _batchSize);
            columnMetadata.Bbox = wkbColumnWriter.Bbox;
            columnMetadata.GeometryTypes = wkbColumnWriter.GeometryTypes;

            // Add the metadata column to the list of GeoParquet metadata columns
            metadataColumns.Add(columnMetadata);
         }

         return metadataColumns;
      }

      /// <summary>
      /// Adds a WKB column to the current data table.
      /// </summary>
      /// <param name="wktColumn">The WKT <see cref="DataColumn"/> that it will replace.</param>
      /// <param name="ordinalPosition">The ordinal position of the new column.</param>
      /// <returns></returns>
      private DataColumn AddWkbColumn(DataColumn wktColumn, int ordinalPosition)
      {
         string wkbColumnName = $"{wktColumn.ColumnName}_WKB";
         DataColumn wkbColumn = _dataTable.AddGeoColumn(wkbColumnName, ordinalPosition, GeometryFormat.WKB);

         // If the WKT column is the primary geometry column, mark the WKB column as the primary one as well 
         if (wktColumn.IsPrimaryGeoColumn())
         {
            wkbColumn.SetAsPrimaryGeoColumn();
         }

         return wkbColumn;
      }
   }
}