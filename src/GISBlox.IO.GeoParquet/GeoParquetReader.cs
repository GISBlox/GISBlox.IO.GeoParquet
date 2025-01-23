using GISBlox.IO.GeoParquet.Common;
using GISBlox.IO.GeoParquet.Extensions;
using GISBlox.IO.GeoParquet.Utils;
using ParquetSharp;
using System.Data;

namespace GISBlox.IO.GeoParquet
{
   /// <summary>
   /// A GeoParquet reader.
   /// </summary>
   public class GeoParquetReader
   {
      /// <summary>
      /// Reads the metadata of a Parquet file.
      /// </summary>
      /// <param name="filePath">The source file path.</param>
      /// <returns>A <see cref="ParquetFileMetadata"/> instance that contains file metadata.</returns>
      public static ParquetFileMetadata ReadFileMetadata(string filePath)
      {
         try
         {
            #region Validate

            ArgumentNullException.ThrowIfNullOrEmpty(filePath);

            #endregion

            using (var fileReader = new ParquetFileReader(filePath))
            {
               var dataTable = fileReader.CreateDataTable();

               ParquetFileMetadata fileMetadata = new()
               {
                  Columns = new Dictionary<string, Type>(),
                  NumRowGroups = fileReader.FileMetaData.NumRowGroups,
                  NumRows = fileReader.FileMetaData.NumRows,
                  Size = fileReader.FileMetaData.Size,
                  Version = fileReader.FileMetaData.Version.ToString()
               };
               foreach (DataColumn column in dataTable.Columns)
               {
                  fileMetadata.Columns.Add(column.ColumnName, column.DataType);
               }
               return fileMetadata;
            }
         }
         catch (Exception)
         {
            throw;
         }
      }

      /// <summary>
      /// Reads the metadata of a GeoParquet file.
      /// </summary>
      /// <param name="filePath">The source file path.</param>
      /// <returns>A <see cref="GeoFileMetadata"/> instance that contains the geo file metadata.</returns>
      /// <exception cref="InvalidDataException"></exception>
      public static GeoFileMetadata? ReadGeoMetadata(string filePath)
      {
         try
         {
            #region Validate

            ArgumentNullException.ThrowIfNullOrEmpty(filePath);

            #endregion

            using (var fileReader = new ParquetFileReader(filePath))
            {
               GeoFileMetadata? metadata = fileReader.GeoFileMetadata();
               return metadata == null
                  ? throw new InvalidDataException($"The file '{filePath}' is not a GeoParquet file.")
                  : fileReader.GeoFileMetadata();
            }
         }
         catch (Exception)
         {
            throw;
         }
      }

      /// <summary>
      /// Reads all data from a GeoParquet file into a <see cref="DataTable"/>.
      /// </summary>
      /// <param name="filePath">The source file path.</param>
      /// <param name="format">Specifies the desired format for the geometry types in the <see cref="DataTable"/>.</param>
      /// <param name="batchSize">The number of data rows to be processed in a single operation. 
      /// Adjust this parameter according to your system's memory capacity. 
      /// Smaller batches reduce memory pressure but may increase I/O operations.</param>
      /// <returns>A <see cref="DataTable"/> with the data from the GeoParquet file.</returns>
      public static DataTable ReadAll(string filePath, GeometryFormat format, int batchSize = 65536)
      {
         try
         {
            #region Validate

            ArgumentNullException.ThrowIfNullOrEmpty(filePath);
            ArgumentOutOfRangeException.ThrowIfLessThan(batchSize, 1);
            if (format == GeometryFormat.Unknown)
            {
               throw new ArgumentOutOfRangeException(nameof(format), "The geometry format cannot be unknown.");
            }

            #endregion

            using (var fileReader = new ParquetFileReader(filePath))
            {
               // Create parquet column metadata and the target data table               
               var columnMetadata = fileReader.ToParquetColumnMetadata();
               var tableStructure = fileReader.CreateDataTable();

               return ReadAndTransformData(fileReader, columnMetadata, tableStructure, format, batchSize);
            }
         }
         catch (Exception)
         {
            throw;
         }
      }

      /// <summary>
      /// Reads a single column from a GeoParquet file into a <see cref="DataTable"/>.
      /// </summary>
      /// <param name="filePath">The source file path.</param>
      /// <param name="columnIndex">The index of the column to read from the file.</param>
      /// <param name="format">Specifies the desired format for the geometry types in the <see cref="DataTable"/>.</param>
      /// <param name="batchSize">The number of data rows to be processed in a single operation. 
      /// Adjust this parameter according to your system's memory capacity. 
      /// Smaller batches reduce memory pressure but may increase I/O operations.</param>
      /// <returns>A <see cref="DataTable"/> with the data from the GeoParquet file.</returns>
      /// <exception cref="ArgumentOutOfRangeException"></exception>
      public static DataTable ReadColumn(string filePath, int columnIndex, GeometryFormat format, int batchSize = 65536)
      {
         try
         {
            #region Validate

            ArgumentNullException.ThrowIfNullOrEmpty(filePath);
            ArgumentOutOfRangeException.ThrowIfLessThan(columnIndex, 0);
            ArgumentOutOfRangeException.ThrowIfLessThan(batchSize, 1);
            if (format == GeometryFormat.Unknown)
            {
               throw new ArgumentOutOfRangeException(nameof(format), "The geometry format cannot be unknown.");
            }

            #endregion

            ICollection<int> columnIndexes = [columnIndex];
            return ReadColumns(filePath, columnIndexes, format, batchSize);
         }
         catch (Exception)
         {
            throw;
         }
      }

      /// <summary>
      /// Reads data from a GeoParquet file into a <see cref="DataTable"/>.
      /// </summary>
      /// <param name="filePath">The source file path.</param>
      /// <param name="columnIndexes">The indexes of the columns to read from the file.</param>
      /// <param name="format">Specifies the desired format for the geometry types in the <see cref="DataTable"/>.</param>
      /// <param name="batchSize">The number of data rows to be processed in a single operation. 
      /// Adjust this parameter according to your system's memory capacity. 
      /// Smaller batches reduce memory pressure but may increase I/O operations.</param>
      /// <returns>A <see cref="DataTable"/> with the data from the GeoParquet file.</returns>
      /// <exception cref="ArgumentOutOfRangeException"></exception>
      public static DataTable ReadColumns(string filePath, ICollection<int> columnIndexes, GeometryFormat format, int batchSize = 65536)
      {
         try
         {
            #region Validate

            ArgumentNullException.ThrowIfNullOrEmpty(filePath);
            ArgumentOutOfRangeException.ThrowIfLessThan(batchSize, 1);
            if (columnIndexes.Count == 0)
            {
               throw new ArgumentOutOfRangeException(nameof(columnIndexes), "At least one column index must be specified.");
            }
            if (format == GeometryFormat.Unknown)
            {
               throw new ArgumentOutOfRangeException(nameof(format), "The geometry format cannot be unknown.");
            }

            #endregion

            using (var fileReader = new ParquetFileReader(filePath))
            {
               // Check if the file is a GeoParquet file
               if (!fileReader.IsGeoParquetFile())
               {
                  throw new InvalidDataException($"The file '{filePath}' is not a GeoParquet file.");
               }

               // Create parquet column metadata and the target data table
               var columnMetadata = fileReader.ToParquetColumnMetadata();
               var tableStructure = fileReader.CreateDataTable(columnIndexes);

               // Select the columns to read
               var selectedColumns = columnMetadata.SelectColumns(columnIndexes);
               if (selectedColumns.Count == 0)
               {
                  throw new ArgumentOutOfRangeException(nameof(columnIndexes), "Column indexes not found.");
               }
               
               return ReadAndTransformData(fileReader, selectedColumns, tableStructure, format, batchSize);               
            }
         }
         catch (Exception)
         {
            throw;
         }
      }

      /// <summary>
      /// Reads a single column from a GeoParquet file into a <see cref="DataTable"/>.
      /// </summary>
      /// <param name="filePath">The source file path.</param>
      /// <param name="columnName">The name of the column to read from the file.</param>
      /// <param name="format">Specifies the desired format for the geometry types in the <see cref="DataTable"/>.</param>
      /// <param name="batchSize">The number of data rows to be processed in a single operation. 
      /// Adjust this parameter according to your system's memory capacity. 
      /// Smaller batches reduce memory pressure but may increase I/O operations.</param>
      /// <returns>A <see cref="DataTable"/> with the data from the GeoParquet file.</returns>
      /// <exception cref="ArgumentOutOfRangeException"></exception>
      public static DataTable ReadColumn(string filePath, string columnName, GeometryFormat format, int batchSize = 65536)
      {
         try
         {
            #region Validate

            ArgumentNullException.ThrowIfNullOrEmpty(filePath);
            ArgumentNullException.ThrowIfNullOrEmpty(columnName);
            ArgumentOutOfRangeException.ThrowIfLessThan(batchSize, 1);
            if (format == GeometryFormat.Unknown)
            {
               throw new ArgumentOutOfRangeException(nameof(format), "The geometry format cannot be unknown.");
            }

            #endregion

            ICollection<string> columnNames = [columnName];
            return ReadColumns(filePath, columnNames, format, batchSize);
         }
         catch (Exception)
         {
            throw;
         }
      }

      /// <summary>
      /// Reads data from a GeoParquet file into a <see cref="DataTable"/>.
      /// </summary>
      /// <param name="filePath">The source file path.</param>
      /// <param name="columnNames">The names of the columns to read from the file.</param>
      /// <param name="format">Specifies the desired format for the geometry types in the <see cref="DataTable"/>.</param>
      /// <param name="batchSize">The number of data rows to be processed in a single operation. 
      /// Adjust this parameter according to your system's memory capacity. 
      /// Smaller batches reduce memory pressure but may increase I/O operations.</param>
      /// <returns>A <see cref="DataTable"/> with the data from the GeoParquet file.</returns>
      /// <exception cref="ArgumentOutOfRangeException"></exception>
      public static DataTable ReadColumns(string filePath, ICollection<string> columnNames, GeometryFormat format, int batchSize = 65536)
      {
         try
         {
            #region Validate

            ArgumentNullException.ThrowIfNullOrEmpty(filePath);
            ArgumentOutOfRangeException.ThrowIfLessThan(batchSize, 1);
            if (columnNames.Count == 0)
            {
               throw new ArgumentOutOfRangeException(nameof(columnNames), "At least one column name must be specified.");
            }
            if (format == GeometryFormat.Unknown)
            {
               throw new ArgumentOutOfRangeException(nameof(format), "The geometry format cannot be unknown.");
            }

            #endregion

            using (var fileReader = new ParquetFileReader(filePath))
            {
               // Check if the file is a GeoParquet file
               if (!fileReader.IsGeoParquetFile())
               {
                  throw new InvalidDataException($"The file '{filePath}' is not a GeoParquet file.");
               }

               // Create parquet column metadata and the target data table
               var columnMetadata = fileReader.ToParquetColumnMetadata();
               var tableStructure = fileReader.CreateDataTable(columnNames);

               // Select the columns to read
               var selectedColumns = columnMetadata.SelectColumns(columnNames);
               if (selectedColumns.Count == 0)
               {
                  throw new ArgumentOutOfRangeException(nameof(columnNames), "Column names not found.");
               }

               return ReadAndTransformData(fileReader, selectedColumns, tableStructure, format, batchSize);               
            }
         }
         catch (Exception)
         {
            throw;
         }
      }

      /// <summary>
      /// Reads data from a <see cref="ParquetFileReader"/> into a <see cref="DataTable"/>.
      /// </summary>
      /// <param name="fileReader">A <see cref="ParquetFileReader"/>.</param>
      /// <param name="columnMetadata">A list of <see cref="ParquetColumnMetadata"/> instances specifying the source parquet columns to read.</param>
      /// <param name="dataTable">A <see cref="DataTable"/> with a structure that matches the source columns to read.</param>
      /// <param name="format">Specifies the desired format for the geometry types in the <see cref="DataTable"/>.</param>
      /// <param name="batchSize">The number of rows to be processed in a single operation.</param>
      /// <returns>A <see cref="DataTable"/> with the selected column data from the parquet file.</returns>
      private static DataTable ReadAndTransformData(ParquetFileReader fileReader, List<ParquetColumnMetadata> columnMetadata, DataTable dataTable, GeometryFormat format, int batchSize)
      {
         // Read the data in batches
         fileReader.ReadRowGroupData(columnMetadata, dataTable, batchSize);

         // Transform geometries if desired
         if (format == GeometryFormat.WKT)
         {
            WKTColumnWriter wktColumnWriter = new(dataTable);
            wktColumnWriter.TransformGeometries(batchSize);
         }

         return dataTable;
      }
   }
}
