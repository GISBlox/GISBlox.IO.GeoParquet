using GISBlox.IO.GeoParquet.Common;
using ParquetSharp;
using System.Data;
using System.Text.Json;

namespace GISBlox.IO.GeoParquet.Extensions
{
   internal static class ParquetExtensions
   {
      /// <summary>
      /// Creates a list of <see cref="ParquetColumnMetadata"/> instances based on <see cref="DataColumn"/>s in a <see cref="DataTable"/>.
      /// </summary>
      /// <param name="table">A data table.</param>
      /// <returns>A list of <see cref="ParquetColumnMetadata"/> instances.</returns>
      public static List<ParquetColumnMetadata> ToParquetColumnMetadata(this DataTable table)
      {
         List<ParquetColumnMetadata> columns = [];
         foreach (DataColumn column in table.Columns)
         {
            // Get the DataColumn type and create a ParquetColumn of the same type
            Type columnType = column.DataType;
            Type parquetColumnType = typeof(ParquetColumn<>).MakeGenericType(columnType);

            // Create a ParquetColumnMetadata instance based on the ParquetColumn and add it to the result list (if not null)
            if (Activator.CreateInstance(parquetColumnType, column.ColumnName) is ParquetColumnMetadata obj)
            {
               columns.Add(obj);
            }
         }
         return columns;
      }

      /// <summary>
      /// Creates a list of <see cref="ParquetColumnMetadata"/> instances based on the columns in a <see cref="ParquetFileReader"/>.
      /// </summary>
      /// <param name="reader">A <see cref="ParquetFileReader"/>.</param>
      /// <returns>A list of <see cref="ParquetColumnMetadata"/> instances.</returns>
      public static List<ParquetColumnMetadata> ToParquetColumnMetadata(this ParquetFileReader reader)
      {
         List<ParquetColumnMetadata> columns = [];
         SchemaDescriptor schema = reader.FileMetaData.Schema;
         for (int columnIndex = 0; columnIndex < schema.NumColumns; ++columnIndex)
         {
            ColumnDescriptor column = schema.Column(columnIndex);

            Type parquetColumnType = typeof(ParquetColumn<>).MakeGenericType(column.GetClrType());
            if (Activator.CreateInstance(parquetColumnType, column.Name) is ParquetColumnMetadata obj)
            {
               obj.FieldId = columnIndex;
               columns.Add(obj);
            }
         }
         return columns;
      }

      /// <summary>
      /// Gets the CLR type of a <see cref="ColumnDescriptor"/>.
      /// </summary>
      /// <param name="column">A <see cref="ColumnDescriptor"/> type.</param>
      /// <returns>The CLR type of the <see cref="ColumnDescriptor"/>.</returns>
      /// <exception cref="NotSupportedException"></exception>
      public static Type GetClrType(this ColumnDescriptor column)
      {
         return column.PhysicalType switch
         {
            var t when t == PhysicalType.Int32 => typeof(int),
            var t when t == PhysicalType.Float => typeof(float),
            var t when t == PhysicalType.Double => typeof(double),
            var t when t == PhysicalType.ByteArray && column.LogicalType.Type == LogicalTypeEnum.String => typeof(string),
            var t when t == PhysicalType.ByteArray => typeof(byte[]),
            var t when t == PhysicalType.Int64 => typeof(long),
            var t when t == PhysicalType.Boolean => typeof(bool),
            _ => throw new NotSupportedException($"Unsupported column type: {column.PhysicalType}")
         };         
      }

      /// <summary>
      /// Converts a list of <see cref="ParquetColumnMetadata"/> instances to a list of ParquetSharp.<see cref="Column"/> instances.
      /// </summary>
      /// <param name="columnMetadata">A list of <see cref="ParquetColumnMetadata"/> types.</param>
      /// <returns>A List with ParquetSharp.<see cref="Column"/> types.</returns>
      public static List<Column> ToParquetSharpColumns(this List<ParquetColumnMetadata> columnMetadata)
      {
         List<Column> columns = [];
         foreach (ParquetColumnMetadata metaDataColumn in columnMetadata)
         {
            columns.Add(new Column(metaDataColumn.GetClrType(), metaDataColumn.Name, metaDataColumn.GetLogicalType()));
         }
         return columns;
      }

      /// <summary>
      /// Creates a <see cref="DataTable"/> based on the columns in a <see cref="ParquetFileReader"/>.
      /// </summary>
      /// <param name="reader">A <see cref="ParquetFileReader"/>.</param>
      /// <returns>A <see cref="DataTable"/>.</returns>
      public static DataTable CreateDataTable(this ParquetFileReader reader)
      {
         DataTable dataTable = new();
         SchemaDescriptor schema = reader.FileMetaData.Schema;
         for (int columnIndex = 0; columnIndex < schema.NumColumns; ++columnIndex)
         {
            ColumnDescriptor column = schema.Column(columnIndex);
            dataTable.Columns.Add(column.Name, column.GetClrType());
         }
         dataTable.AddGeoProcessingMetadata(reader.GeoFileMetadata());
         return dataTable;
      }

      /// <summary>
      /// Creates a <see cref="DataTable"/> based on the specified column name in a <see cref="ParquetFileReader"/>.
      /// </summary>
      /// <param name="reader">A <see cref="ParquetFileReader"/>.</param>
      /// <param name="columnName">The name of the column.</param>
      /// <returns>A <see cref="DataTable"/>.</returns>
      public static DataTable CreateDataTable(this ParquetFileReader reader, string columnName)
      {
         ICollection<string> columnNames = [columnName];
         return CreateDataTable(reader, columnNames);
      }

      /// <summary>
      /// Creates a <see cref="DataTable"/> based on the specified column names in a <see cref="ParquetFileReader"/>.
      /// </summary>
      /// <param name="reader">A <see cref="ParquetFileReader"/>.</param>
      /// <param name="columnNames">A collection of column names.</param>
      /// <returns>A <see cref="DataTable"/>.</returns>
      public static DataTable CreateDataTable(this ParquetFileReader reader, ICollection<string> columnNames)
      {
         DataTable dataTable = new();
         SchemaDescriptor schema = reader.FileMetaData.Schema;
         foreach(string columnName in columnNames)
         { 
            ColumnDescriptor? column = GetColumnDescriptor(schema, columnName);
            if (column != null)
            {
               dataTable.Columns.Add(column.Name, column.GetClrType());
            }
         }
         dataTable.AddGeoProcessingMetadata(reader.GeoFileMetadata());
         return dataTable;
      }
     
      /// <summary>
      /// Creates a <see cref="DataTable"/> based on the specified column index in a <see cref="ParquetFileReader"/>.
      /// </summary>
      /// <param name="reader">A <see cref="ParquetFileReader"/>.</param>
      /// <param name="columnIndex">The column index.</param>
      /// <returns>A <see cref="DataTable"/>.</returns>
      public static DataTable CreateDataTable(this ParquetFileReader reader, int columnIndex)
      {
         ICollection<int> columnIndexes = [columnIndex];
         return CreateDataTable(reader, columnIndexes);
      }

      /// <summary>
      /// Creates a <see cref="DataTable"/> based on the specified column indexes in a <see cref="ParquetFileReader"/>.
      /// </summary>
      /// <param name="reader">A <see cref="ParquetFileReader"/>.</param>
      /// <param name="columnIndexes">A collection of column indexes.</param>
      /// <returns></returns>
      public static DataTable CreateDataTable(this ParquetFileReader reader, ICollection<int> columnIndexes)
      {
         DataTable dataTable = new();
         SchemaDescriptor schema = reader.FileMetaData.Schema;
         foreach (int columnIndex in columnIndexes)
         {
            ColumnDescriptor column = schema.Column(columnIndex);
            dataTable.Columns.Add(column.Name, column.GetClrType());
         }
         dataTable.AddGeoProcessingMetadata(reader.GeoFileMetadata());
         return dataTable;
      }

      /// <summary>
      /// Reads column data from every RowGroup into a <see cref="DataTable"/>.
      /// </summary>
      /// <param name="fileReader">A <see cref="ParquetFileReader"/>.</param>
      /// <param name="columnMetadata">A List with <see cref="ParquetColumnMetadata"/> types.</param>
      /// <param name="dataTable">The target <see cref="DataTable"/>.</param>
      /// <param name="batchSize">The number of rows to be processed in a single operation.</param>
      public static void ReadRowGroupData(this ParquetFileReader fileReader, List<ParquetColumnMetadata> columnMetadata, DataTable dataTable, int batchSize)
      {
         for (int rowGroup = 0; rowGroup < fileReader.FileMetaData.NumRowGroups; ++rowGroup)
         {
            using (var rowGroupReader = fileReader.RowGroup(rowGroup))
            {
               // Process the row group in batches
               long numRows = rowGroupReader.MetaData.NumRows;
               for (long startRow = 0; startRow < numRows; startRow += batchSize)
               {
                  int currentBatchSize = (int)Math.Min(batchSize, numRows - startRow);

                  var columnData = rowGroupReader.ReadColumnData(columnMetadata, currentBatchSize);

                  dataTable.WriteColumnData(columnData);
               }
            }
         }
      }

      /// <summary>
      /// Reads column data from a row group.
      /// </summary>
      /// <param name="rowGroupReader">A <see cref="RowGroupReader"/>.</param>
      /// <param name="columnMetadata">A List with <see cref="ParquetColumnMetadata"/> types.</param>
      /// <param name="batchSize">The number of rows to be processed in a single operation.</param>
      /// <returns>A two dimensional array with column data of all processed rows.</returns>
      public static object[][] ReadColumnData(this RowGroupReader rowGroupReader, List<ParquetColumnMetadata> columnMetadata, int batchSize)
      {
         int numColumns = columnMetadata.Count;
         object[][] columnData = new object[numColumns][];

         for (int columnIndex = 0; columnIndex < numColumns; columnIndex++)
         {
            // Get the column metadata
            var col = columnMetadata[columnIndex];

            // Read the row group column index
            int rowGroupColumnIndex = col.FieldId;

            // Read the column data
            columnData[columnIndex] = col.ReadData(rowGroupReader.Column(rowGroupColumnIndex), batchSize); 
         }

         return columnData;
      }

      /// <summary>
      /// Writes data from a <see cref="DataTable"/> to a RowGroup.
      /// </summary>
      /// <param name="fileWriter">A <see cref="ParquetFileWriter"/>.</param>
      /// <param name="columnMetadata">A List with <see cref="ParquetColumnMetadata"/> types.</param>
      /// <param name="dataTable">The source <see cref="DataTable"/>.</param>
      /// <param name="batchSize">The number of rows to be processed in a single operation.</param>
      public static void WriteRowGroupData(this ParquetFileWriter fileWriter, List<ParquetColumnMetadata> columnMetadata, DataTable dataTable, int batchSize)
      {
         using (var rowGroupWriter = fileWriter.AppendRowGroup())
         {
            foreach (var column in columnMetadata)
            {
               ColumnWriter columnWriter = rowGroupWriter.NextColumn();

               // Process the column data in batches
               int totalRows = dataTable.Rows.Count;
               for (int batchStart = 0; batchStart < totalRows; batchStart += batchSize)
               {
                  var rows = dataTable.AsEnumerable().Skip(batchStart).Take(batchSize);                              

                  column.WriteData(columnWriter, rows, batchSize);
               }
            }
         }
      }

      /// <summary>
      /// Determines whether a Parquet file is a GeoParquet file.
      /// </summary>
      /// <param name="reader">A <see cref="ParquetFileReader"/>.</param>
      /// <returns>True if the file is a GeoParquet file.</returns>
      public static bool IsGeoParquetFile(this ParquetFileReader reader)
      {
         return reader.GeoFileMetadataAsString() != null;
      }

      /// <summary>
      /// Gets the GeoFileMetadata from a Parquet file.
      /// </summary>
      /// <param name="reader">A <see cref="ParquetFileReader"/>.</param>
      /// <returns>A string with the geo file metadata.</returns>
      public static string? GeoFileMetadataAsString(this ParquetFileReader reader)
      {
         var metadata = reader.FileMetaData.KeyValueMetadata;
         return metadata.GetValueOrDefault("geo");
      }

      /// <summary>
      /// Gets the GeoFileMetadata from a Parquet file.
      /// </summary>
      /// <param name="reader">A <see cref="ParquetFileReader"/>.</param>
      /// <returns>A <see cref="GeoFileMetadata"/> instance.</returns>
      public static GeoFileMetadata? GeoFileMetadata(this ParquetFileReader reader)
      {
         string? geoMetadata = reader.GeoFileMetadataAsString();
         if (!string.IsNullOrEmpty(geoMetadata))
         {
            return JsonSerializer.Deserialize<GeoFileMetadata>(geoMetadata);
         }
         return null;
      }

      /// <summary>
      /// Gets a <see cref="ColumnDescriptor"/> based on the specified column name in a <see cref="SchemaDescriptor"/>.
      /// </summary>
      /// <param name="schema">A <see cref="SchemaDescriptor"/>.</param>
      /// <param name="columnName">A column name.</param>
      /// <returns>A <see cref="ColumnDescriptor"/> for the specified column.</returns>
      private static ColumnDescriptor? GetColumnDescriptor(this SchemaDescriptor schema, string columnName)
      {
         for (int columnIndex = 0; columnIndex < schema.NumColumns; ++columnIndex)
         {
            ColumnDescriptor column = schema.Column(columnIndex);
            if (column.Name == columnName)
            {
               return column;
            }
         }
         return null;
      }
   }
}