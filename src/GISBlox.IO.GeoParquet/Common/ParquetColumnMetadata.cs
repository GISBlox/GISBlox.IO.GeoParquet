using ParquetSharp;
using System.Data;

namespace GISBlox.IO.GeoParquet.Common
{
   /// <summary>
   /// Represents a column in a Parquet file.
   /// </summary>  
   internal abstract class ParquetColumnMetadata(string name)
   {
      /// <summary>
      /// The name of the column.
      /// </summary>
      public string Name { get; } = name;

      /// <summary>
      /// The index of the column in the row group.
      /// </summary>
      public int FieldId { get; set; } = -1;

      /// <summary>
      /// The physical type of the column.
      /// </summary>      
      public abstract PhysicalType GetPhysicalType();

      /// <summary>
      /// The logical type of the column.
      /// </summary>      
      public abstract LogicalType? GetLogicalType();

      /// <summary>
      /// The CLR type of the column.
      /// </summary>      
      public abstract Type GetClrType();

      /// <summary>
      /// Reads the data of the column from the Parquet file.
      /// </summary>
      /// <param name="columnReader">A <see cref="ColumnReader"/>.</param>
      /// <param name="batchSize">The number of column data rows to process in a single operation.</param>
      public abstract object[] ReadData(ColumnReader columnReader, int batchSize);

      /// <summary>
      /// Writes the data of the column to the Parquet file.
      /// </summary>
      /// <param name="columnWriter">A <see cref="ColumnWriter"/>.</param>
      /// <param name="dataRows">An <see cref="IEnumerable{T}"/> of <see cref="DataRow"/>.</param>
      /// <param name="batchSize">The number of column data rows to process in a single operation.</param>
      public abstract void WriteData(ColumnWriter columnWriter, IEnumerable<DataRow> dataRows, int batchSize);
   }
}