using GISBlox.IO.GeoParquet.Common;
using GISBlox.IO.GeoParquet.Extensions;
using GISBlox.IO.GeoParquet.Utils;
using ParquetSharp;
using ParquetSharp.IO;
using System.Data;

namespace GISBlox.IO.GeoParquet
{
   /// <summary>
   /// A GeoParquet writer.
   /// </summary>
   public class GeoParquetWriter
   {
      /// <summary>
      /// Writes a <see cref="DataTable"/> to a GeoParquet file.
      /// </summary>
      /// <param name="filePath">The target file path.</param>
      /// <param name="dataTable">A <see cref="DataTable"/> with a single geometry column.</param>
      /// <param name="geoColumn">The name of the geometry column in the <see cref="DataTable"/>.</param>
      /// <param name="geoColumnFormat">The current format of the geometry types in the geometry column.</param>
      /// <param name="batchSize">The number of data rows to be processed in a single operation. 
      /// Adjust this parameter according to your system's memory capacity. 
      /// Smaller batches reduce memory pressure but may increase I/O operations.</param>
      public static void Write(string filePath, DataTable dataTable, string geoColumn, GeometryFormat geoColumnFormat, int batchSize = 65536)
      {
         Write(filePath, dataTable, new Dictionary<string, GeometryFormat> { { geoColumn, geoColumnFormat } }, geoColumn, batchSize);
      }

      /// <summary>
      /// Writes a <see cref="DataTable"/> to a GeoParquet file.
      /// </summary>
      /// <param name="filePath">The target file path.</param>
      /// <param name="dataTable">A <see cref="DataTable"/> with one or more geometry columns.</param>
      /// <param name="geoColumns">A list of the geometry column names in the <see cref="DataTable"/>, together with their types.</param>
      /// <param name="primaryGeoColumn">The name of the primary geometry column.</param>
      /// <param name="batchSize">The number of data rows to be processed in a single operation. 
      /// Adjust this parameter according to your system's memory capacity. 
      /// Smaller batches reduce memory pressure but may increase I/O operations.</param>
      /// <exception cref="ArgumentException"></exception>
      public static void Write(string filePath, DataTable dataTable, Dictionary<string, GeometryFormat> geoColumns, string primaryGeoColumn, int batchSize = 65536)
      {
         try
         {
            #region Validate

            ArgumentNullException.ThrowIfNullOrEmpty(filePath);
            ArgumentNullException.ThrowIfNull(dataTable);
            ArgumentNullException.ThrowIfNullOrEmpty(primaryGeoColumn);

            if (geoColumns == null || geoColumns.Count == 0)
            {
               throw new ArgumentException("At least one geometry column must be specified.");
            }

            #endregion

            // Add processing metadata to the geo columns in the data table
            dataTable.AddGeoProcessingMetadata(geoColumns, primaryGeoColumn);

            // Build GeoParquet metadata
            MetadataBuilder metaDataBuilder = new(dataTable);
            var geoMetadata = metaDataBuilder.BuildGeoParquetMetadata(batchSize);

            // Create parquet columns
            var columnMetadata = dataTable.ToParquetColumnMetadata();
            var columns = columnMetadata.ToParquetSharpColumns();

            // Write data to the file column by column
            using (var fileWriter = new ParquetFileWriter(filePath, [.. columns], keyValueMetadata: geoMetadata))
            {
               fileWriter.WriteRowGroupData(columnMetadata, dataTable, batchSize);               
            }
         }
         catch (Exception)
         {
            throw;
         }
      }

      /// <summary>
      /// Writes a <see cref="DataTable"/> to a GeoParquet file.
      /// </summary>
      /// <param name="stream">The output stream.</param>
      /// <param name="dataTable">A <see cref="DataTable"/> with a single geometry column.</param>
      /// <param name="geoColumn">The name of the geometry column in the <see cref="DataTable"/>.</param>
      /// <param name="geoColumnFormat">The current format of the geometry types in the geometry column.</param>
      /// <param name="batchSize">The number of data rows to be processed in a single operation. 
      /// Adjust this parameter according to your system's memory capacity. 
      /// Smaller batches reduce memory pressure but may increase I/O operations.</param>
      public static void Write(Stream stream, DataTable dataTable, string geoColumn, GeometryFormat geoColumnFormat, int batchSize = 65536)
      {
         Write(stream, dataTable, new Dictionary<string, GeometryFormat> { { geoColumn, geoColumnFormat } }, geoColumn, batchSize);
      }

      /// <summary>
      /// Writes a <see cref="DataTable"/> to a GeoParquet file.
      /// </summary>
      /// <param name="stream">The output stream.</param>
      /// <param name="dataTable">A <see cref="DataTable"/> with one or more geometry columns.</param>
      /// <param name="geoColumns">A list of the geometry column names in the <see cref="DataTable"/>, together with their types.</param>
      /// <param name="primaryGeoColumn">The name of the primary geometry column.</param>
      /// <param name="batchSize">The number of data rows to be processed in a single operation. 
      /// Adjust this parameter according to your system's memory capacity. 
      /// Smaller batches reduce memory pressure but may increase I/O operations.</param>
      /// <exception cref="ArgumentException"></exception>
      public static void Write(Stream stream, DataTable dataTable, Dictionary<string, GeometryFormat> geoColumns, string primaryGeoColumn, int batchSize = 65536)
      {
         try
         {
            #region Validate

            ArgumentNullException.ThrowIfNull(dataTable);
            ArgumentNullException.ThrowIfNullOrEmpty(primaryGeoColumn);

            if (geoColumns == null || geoColumns.Count == 0)
            {
               throw new ArgumentException("At least one geometry column must be specified.");
            }

            #endregion

            // Add processing metadata to the geo columns in the data table
            dataTable.AddGeoProcessingMetadata(geoColumns, primaryGeoColumn);

            // Build GeoParquet metadata
            MetadataBuilder metaDataBuilder = new(dataTable);
            var geoMetadata = metaDataBuilder.BuildGeoParquetMetadata(batchSize);

            // Create parquet columns
            var columnMetadata = dataTable.ToParquetColumnMetadata();
            var columns = columnMetadata.ToParquetSharpColumns();

            // Write data to the stream column by column
            using (var writer = new ManagedOutputStream(stream))
            {
               using (var fileWriter = new ParquetFileWriter(writer, [.. columns], keyValueMetadata: geoMetadata))
               {
                  fileWriter.WriteRowGroupData(columnMetadata, dataTable, batchSize);
               }
            }
         }
         catch (Exception)
         {
            throw;
         }
      }
   }
}