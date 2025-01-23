using GISBlox.IO.GeoParquet.Common;

namespace GISBlox.IO.GeoParquet.Extensions
{
   internal static class ListExtensions
   {
      /// <summary>
      /// Selects a column from a list of Parquet column metadata.
      /// </summary>
      /// <param name="columnMetadata">A list of <see cref="ParquetColumnMetadata"/> instances.</param>
      /// <param name="columnIndex">The index of the column to select.</param>      
      public static List<ParquetColumnMetadata> SelectColumn(this List<ParquetColumnMetadata> columnMetadata, int columnIndex)
      {
         ICollection<int> columnIndexes = [columnIndex];
         return columnMetadata.SelectColumns(columnIndexes);
      }

      /// <summary>
      /// Selects columns from a list of Parquet column metadata.
      /// </summary>
      /// <param name="columnMetadata">A list of <see cref="ParquetColumnMetadata"/> instances.</param>
      /// <param name="columnIndexes">The indexes of the columns to select.</param>     
      public static List<ParquetColumnMetadata> SelectColumns(this List<ParquetColumnMetadata> columnMetadata, ICollection<int> columnIndexes)
      {
         List<ParquetColumnMetadata> columnsFound = [];
         foreach (var columnIndex in columnIndexes)
         {
            var column = columnMetadata[columnIndex];
            if (column != null)
            {
               columnsFound.Add(column);
            }
         }
         return columnsFound;
      }

      /// <summary>
      /// Selects a column from a list of Parquet column metadata.
      /// </summary>
      /// <param name="columnMetadata">A list of <see cref="ParquetColumnMetadata"/> instances.</param>
      /// <param name="columnName">The name of the column to select.</param>
      /// <returns></returns>
      public static List<ParquetColumnMetadata> SelectColumn(this List<ParquetColumnMetadata> columnMetadata, string columnName)
      {
         ICollection<string> columnNames = [columnName];
         return columnMetadata.SelectColumns(columnNames);
      }

      /// <summary>
      /// Selects columns from a list of Parquet column metadata.
      /// </summary>
      /// <param name="columnMetadata">A list of <see cref="ParquetColumnMetadata"/> instances.</param>
      /// <param name="columnNames">The names of the columns to select.</param>
      /// <returns></returns>
      public static List<ParquetColumnMetadata> SelectColumns(this List<ParquetColumnMetadata> columnMetadata, ICollection<string> columnNames)
      {
         List<ParquetColumnMetadata> columnsFound = [];
         foreach (var columnName in columnNames)
         {
            var column = columnMetadata.Find(col => col.Name == columnName);
            if (column != null)
            {
               columnsFound.Add(column);
            }
         }
         return columnsFound;
      }
   }
}
