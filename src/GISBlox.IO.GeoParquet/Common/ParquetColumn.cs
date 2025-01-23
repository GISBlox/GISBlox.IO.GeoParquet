using ParquetSharp;
using System.Data;

namespace GISBlox.IO.GeoParquet.Common
{
   /// <summary>
   /// Represents a column in a Parquet file.
   /// </summary>   
   internal class ParquetColumn<T>(string name) : ParquetColumnMetadata(name)
   {
      private LogicalColumnReader<T>? _reader;
      private LogicalColumnWriter<T>? _writer;

      /// <summary>
      /// The physical type of the column.
      /// </summary>
      /// <returns>A <see cref="PhysicalType"/>.</returns>
      /// <exception cref="NotSupportedException"></exception>
      public override PhysicalType GetPhysicalType()
      {
         return typeof(T) switch
         {
            var t when t == typeof(int) => PhysicalType.Int32,
            var t when t == typeof(float) => PhysicalType.Float,
            var t when t == typeof(double) => PhysicalType.Double,
            var t when t == typeof(byte[]) => PhysicalType.ByteArray,
            var t when t == typeof(string) => PhysicalType.ByteArray,
            var t when t == typeof(long) => PhysicalType.Int64,
            var t when t == typeof(bool) => PhysicalType.Boolean,
            _ => throw new NotSupportedException($"Unsupported column type: {typeof(T)}")
         };
      }

      /// <summary>
      /// The logical type of the column.
      /// </summary>
      /// <returns>A <see cref="LogicalType"/>.</returns>
      public override LogicalType? GetLogicalType()
      {
         LogicalType? logicalType = null;
         if (typeof(T) == typeof(string))
         {
            logicalType = LogicalType.String();
         }
         return logicalType;
      }

      /// <summary>
      /// The CLR type of the column.
      /// </summary>
      /// <returns>A <see cref="Type"/>.</returns>
      public override Type GetClrType()
      {
         return typeof(T);
      }
      
      /// <summary>
      /// Reads the data of the column from the Parquet file.
      /// </summary>
      /// <param name="columnReader">A <see cref="ColumnReader"/>.</param>
      /// <param name="batchSize">The amount of column data rows to process in a single operation.</param>
      /// <returns>An array with the column data.</returns>
      public override object[] ReadData(ColumnReader columnReader, int batchSize)
      {
         // Initialize a new LogicalReader for the column (if not already created) and a buffer to store the column values
         T[] buffer = new T[batchSize];
         _reader ??= columnReader.LogicalReader<T>();

         // Read the column values into the buffer
         _reader.ReadBatch(buffer, 0, batchSize);

         // Return the buffer as an array of objects
         return buffer.Cast<object>().ToArray();
      }

      /// <summary>
      /// Writes the data of the column to the Parquet file.
      /// </summary>
      /// <param name="columnWriter">A <see cref="ColumnWriter"/>.</param>
      /// <param name="dataRows">An <see cref="IEnumerable{T}"/> of <see cref="DataRow"/>.</param>
      /// <param name="batchSize">The amount of column data rows to process in a single operation.</param>
      public override void WriteData(ColumnWriter columnWriter, IEnumerable<DataRow> dataRows, int batchSize)
      {
         // Initialize a new LogicalWriter for the column (if not already created) and a list to store the column values
         var values = new List<T?>(batchSize);
         _writer ??= columnWriter.LogicalWriter<T>();
   
         // Iterate over the data rows, retrieve the current column's value and add it to the list
         foreach (DataRow row in dataRows)
         {
            if (!row.IsNull(Name))
            {
               values.Add(ParquetColumn<T>.ConvertValue(row[Name]));
            }
            else
            {
               values.Add(default);
            }
         }

         // Write the column values to the Parquet file
         _writer.WriteBatch([..values]);         
      }

      /// <summary>
      /// Handles the conversion of string to a byte array. Necessary because of the way ParquetSharp handles string data.
      /// </summary>
      /// <param name="value">The value.</param>
      /// <returns>The type.</returns>
      /// <exception cref="InvalidCastException"></exception>
      private static T? ConvertValue(object value)
      {
         return value switch
         {
            null => default,
            T t => t,
            string s when typeof(T) == typeof(byte[]) => (T)(object)System.Text.Encoding.UTF8.GetBytes(s),
            _ => throw new InvalidCastException($"Cannot convert value '{value}' of type {value.GetType()} to type {typeof(T)}")
         };
      }
   }
}