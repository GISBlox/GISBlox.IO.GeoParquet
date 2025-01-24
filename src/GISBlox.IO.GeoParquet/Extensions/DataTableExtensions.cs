using GISBlox.IO.GeoParquet.Common;
using System.Data;

namespace GISBlox.IO.GeoParquet.Extensions
{
   internal static class DataTableExtensions
   {
      /// <summary>
      /// Adds processing metadata to the geometry columns in a <see cref="DataTable"/>
      /// </summary>
      /// <param name="table">A <see cref="DataTable"/>.</param>
      /// <param name="geoColumns">A Dictionary with the names and types of the geo columns in the <see cref="DataTable"/>.</param>
      /// <param name="primaryGeoColumn">The name of the primary geo column.</param>
      /// <exception cref="Exception"></exception>
      public static void AddGeoProcessingMetadata(this DataTable table, Dictionary<string, GeometryFormat> geoColumns, string primaryGeoColumn)
      {
         foreach (var geoColumn in geoColumns)
         {
            DataColumn? col = table.Columns[geoColumn.Key] ?? throw new Exception($"Column '{geoColumn.Key}' does not exist in the data table.");
            col.ExtendedProperties.Add("is_geo_column", true);
            col.ExtendedProperties.Add("geo_format", geoColumn.Value);

            if (col.ColumnName.Equals(primaryGeoColumn))
            {
               col.ExtendedProperties.Add("is_primary_geo_column", true);
            }
         }
      }

      /// <summary>
      /// Adds processing metadata to the geometry column(s) in a <see cref="DataTable"/> as identified by the file's <see cref="GeoFileMetadata"/>.
      /// </summary>
      /// <param name="table">A <see cref="DataTable"/>.</param>
      /// <param name="geoFileMetadata">A <see cref="GeoFileMetadata"/> type.</param>
      public static void AddGeoProcessingMetadata(this DataTable table, GeoFileMetadata? geoFileMetadata)
      {
         string? primaryGeoColumn;
         Dictionary<string, GeometryFormat> geoColumns = [];         
         if (geoFileMetadata != null)
         {
            primaryGeoColumn = geoFileMetadata.Primary_column;
            foreach (var geoColumn in geoFileMetadata.Columns)
            {
               if (table.Columns.Contains(geoColumn.Key))
               {
                  geoColumns.Add(geoColumn.Key, Enum.Parse<GeometryFormat>(geoColumn.Value.Encoding));
               }
            }
            table.AddGeoProcessingMetadata(geoColumns, !string.IsNullOrEmpty(primaryGeoColumn) ? primaryGeoColumn : string.Empty);
         }
      }

      /// <summary>
      /// Adds a geometry column to a <see cref="DataTable"/>.
      /// </summary>
      /// <param name="table">A <see cref="DataTable"/>.</param>
      /// <param name="columnName">The name of the new column.</param> 
      /// <param name="ordinalPosition">The ordinal position of the new column.</param>
      /// <param name="format">The <see cref="GeometryFormat"/> of the new column.</param>
      /// <returns>The new <see cref="DataColumn"/>.</returns>
      public static DataColumn AddGeoColumn(this DataTable table, string columnName, int ordinalPosition, GeometryFormat format)
      {
         Type columnType = format switch
         {
            GeometryFormat.WKB => typeof(byte[]),
            GeometryFormat.WKT => typeof(string),
            _ => throw new ArgumentException($"Unsupported GeometryFormat: {format}", nameof(format)),
         };

         int columnCount = table.Columns.Count;
         DataColumn col = table.Columns.Add(columnName, columnType);

         // Move the column to the specified position, unless it is the last column
         if (ordinalPosition < columnCount - 1)
         {            
            col.SetOrdinal(ordinalPosition);
         }

         // Add geo column metadata
         col.SetGeoFormat(format);
         col.ExtendedProperties.Add("is_geo_column", true);
         return col;
      }

      /// <summary>
      /// Returns the format of the geometries in a <see cref="DataColumn"/>.
      /// </summary>
      /// <param name="column">A <see cref="DataColumn"/>.</param>
      /// <returns>The <see cref="GeometryFormat"/> of the column's geometries.</returns>
      public static GeometryFormat GetGeoFormat(this DataColumn column)
      {
         return column.ExtendedProperties["geo_format"] as GeometryFormat? ?? GeometryFormat.Unknown;
      }

      /// <summary>
      /// Set the format of the geometries in a <see cref="DataColumn"/>.
      /// </summary>
      /// <param name="column">A <see cref="DataColumn"/>.</param>
      /// <param name="format">The <see cref="GeometryFormat"/> for the column's geometries.</param>
      public static void SetGeoFormat(this DataColumn column, GeometryFormat format)
      {
         if (column.ExtendedProperties.ContainsKey("geo_format"))
         {
            column.ExtendedProperties["geo_format"] = format;
         }
         else
         {
            column.ExtendedProperties.Add("geo_format", format);
         }
      }

      /// <summary>
      /// Returns all geometry columns in a <see cref="DataTable"/> with a specific <see cref="GeometryFormat"/>.
      /// </summary>
      /// <param name="table">A <see cref="DataTable"/>.</param>
      /// <param name="format">The required <see cref="GeometryFormat"/>.</param>
      /// <returns>A List with geometry <see cref="DataColumn"/> types of the specified <see cref="GeometryFormat"/>.</returns>
      public static List<DataColumn> GetGeoColumns(this DataTable table, GeometryFormat format)
      {
         List<DataColumn> columns = [];
         foreach (DataColumn column in table.Columns)
         {
            if (column.ExtendedPropertyIsPresentAndTrue("is_geo_column"))
            {
               if (column.GetGeoFormat() == format)
               {
                  columns.Add(column);
               }
            }
         }
         return columns;
      }

      /// <summary>
      /// Determines whether a <see cref="DataColumn"/> is the primary geometry column in a <see cref="DataTable"/>.
      /// </summary>
      /// <param name="column">A <see cref="DataColumn"/>.</param>
      /// <returns>True if the <see cref="DataColumn"/> is the primary geometry column.</returns>
      public static bool IsPrimaryGeoColumn(this DataColumn column)
      {
         return column.ExtendedPropertyIsPresentAndTrue("is_primary_geo_column");
      }

      /// <summary>
      /// Marks a <see cref="DataColumn"/> as the primary geometry column in a <see cref="DataTable"/>.
      /// </summary>
      /// <param name="column">A <see cref="DataColumn"/>.</param>
      public static void SetAsPrimaryGeoColumn(this DataColumn column)
      {
         if (column.ExtendedProperties.ContainsKey("is_primary_geo_column"))
         {
            column.ExtendedProperties["is_primary_geo_column"] = true;
         }
         else
         {
            column.ExtendedProperties.Add("is_primary_geo_column", true);
         }
      }

      /// <summary>
      /// Retrieves the primary geometry column in a <see cref="DataTable"/>.
      /// </summary>
      /// <param name="table">A <see cref="DataTable"/>.</param>
      /// <returns>The primary geometry <see cref="DataColumn"/>.</returns>
      public static DataColumn? GetPrimaryGeoColumn(this DataTable table)
      {
         foreach (DataColumn column in table.Columns)
         {
            if (column.ExtendedPropertyIsPresentAndTrue("is_primary_geo_column"))
            {
               return column;
            }
         }
         return null;
      }

      /// <summary>
      /// Returns the name of the primary geometry column in a <see cref="DataTable"/>.
      /// </summary>
      /// <param name="table">A <see cref="DataTable"/>.</param>
      /// <exception cref="InvalidOperationException"></exception>
      public static string GetPrimaryGeoColumnName(this DataTable table)
      {
         DataColumn? primaryGeoColumn = table.GetPrimaryGeoColumn() ?? throw new InvalidOperationException("No primary geometry column found.");
         return primaryGeoColumn.ColumnName;
      }

      /// <summary>
      /// Renames a <see cref="DataColumn"/>.
      /// </summary>
      /// <param name="column">A <see cref="DataColumn"/>.</param>
      /// <param name="newName">The new name for the column.</param>
      public static void RenameColumn(this DataColumn column, string newName)
      {
         column.ColumnName = newName;
      }

      /// <summary>
      /// Writes column data to a new <see cref="DataRow"/> in the <see cref="DataTable"/>.
      /// </summary>
      /// <param name="table">A <see cref="DataTable"/>.</param>
      /// <param name="columnData">The column data to write.</param>
      public static void WriteColumnData(this DataTable table, object[][] columnData)
      {
         for (int rowIndex = 0; rowIndex < columnData[0].Length; rowIndex++)
         {
            DataRow dataRow = table.NewRow();
            for (int colIndex = 0; colIndex < columnData.Length; colIndex++)
            {
               if (columnData[colIndex][rowIndex] != null)
               {
                  dataRow[colIndex] = columnData[colIndex][rowIndex];
               }               
            }
            table.Rows.Add(dataRow);
         }
      }

      /// <summary>
      /// Determines whether a <see cref="DataColumn"/> contains a specific extended property and if it is set to true.
      /// </summary>
      /// <param name="column">A <see cref="DataColumn"/>.</param>
      /// <param name="key">The key of the extended property.</param>
      /// <returns>True if the key exists and its value is set to true.</returns>
      private static bool ExtendedPropertyIsPresentAndTrue(this DataColumn column, string key)
      {
         if (column.ExtendedProperties.ContainsKey(key))
         {
            return column.ExtendedProperties[key] as bool? ?? false;
         }
         else
         {
            return false;
         }
      }
   }
}