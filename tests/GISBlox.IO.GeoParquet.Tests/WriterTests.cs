using GISBlox.IO.GeoParquet.Common;
using System.Data;

namespace GISBlox.IO.GeoParquet.Tests
{
   [TestClass]
   public class WriterTests
   {
      private static readonly string BASE_PATH = Path.Combine(AppDomain.CurrentDomain.BaseDirectory);
      private static readonly string SAMPLES_PATH = Path.GetFullPath(Path.Combine(BASE_PATH, "..\\..\\..\\..\\.\\Samples"));

      [TestMethod]
      public void WriteSimpleWkb()
      {
         string fileName = Path.Combine(SAMPLES_PATH, "simple.parquet");

         // Define data table
         var dataTable = new DataTable();
         dataTable.Columns.Add("id", typeof(int));
         dataTable.Columns.Add("name", typeof(string));
         dataTable.Columns.Add("geometry", typeof(byte[]));

         // Add rows
         dataTable.Rows.Add(1, "Amsterdam", new byte[] { 1, 1, 0, 0, 0, 255, 178, 123, 242, 176, 144, 19, 64, 87, 236, 47, 187, 39, 47, 74, 64 });
         dataTable.Rows.Add(2, "Rotterdam", new byte[] { 1, 1, 0, 0, 0, 109, 197, 254, 178, 123, 242, 17, 64, 190, 159, 26, 47, 221, 244, 73, 64 });

         GeoParquetWriter.Write(fileName, dataTable, "geometry", GeometryFormat.WKB);
      }

      [TestMethod]
      public void WriteSimpleWkt()
      {
         string fileName = Path.Combine(SAMPLES_PATH, "simple.parquet");

         // Define data table
         var dataTable = new DataTable();
         dataTable.Columns.Add("id", typeof(int));
         dataTable.Columns.Add("name", typeof(string));
         dataTable.Columns.Add("geometry", typeof(string));

         // Add rows
         dataTable.Rows.Add(1, "Amsterdam", "POINT (4.8913 52.3684)");
         dataTable.Rows.Add(2, "Rotterdam", "POINT (4.4868 51.913)");

         GeoParquetWriter.Write(fileName, dataTable, "geometry", GeometryFormat.WKT);
      }

      [TestMethod]
      public void WriteSimpleWkbToStream()
      {
         string fileName = Path.Combine(SAMPLES_PATH, "simple-streamed-from-wkb.parquet");

         // Define data table
         var dataTable = new DataTable();
         dataTable.Columns.Add("id", typeof(int));
         dataTable.Columns.Add("name", typeof(string));
         dataTable.Columns.Add("geometry", typeof(byte[]));

         // Add rows
         dataTable.Rows.Add(1, "Amsterdam", new byte[] { 1, 1, 0, 0, 0, 255, 178, 123, 242, 176, 144, 19, 64, 87, 236, 47, 187, 39, 47, 74, 64 });
         dataTable.Rows.Add(2, "Rotterdam", new byte[] { 1, 1, 0, 0, 0, 109, 197, 254, 178, 123, 242, 17, 64, 190, 159, 26, 47, 221, 244, 73, 64 });

         using var fileStream = new FileStream(fileName, FileMode.Create, FileAccess.Write);
         GeoParquetWriter.Write(fileStream, dataTable, "geometry", GeometryFormat.WKB);
      }

      [TestMethod]
      public void WriteSimpleWktToStream()
      {
         string fileName = Path.Combine(SAMPLES_PATH, "simple-streamed-from-wkt.parquet");

         // Define data table
         var dataTable = new DataTable();
         dataTable.Columns.Add("id", typeof(int));
         dataTable.Columns.Add("name", typeof(string));
         dataTable.Columns.Add("geometry", typeof(string));

         // Add rows
         dataTable.Rows.Add(1, "Amsterdam", "POINT (4.8913 52.3684)");
         dataTable.Rows.Add(2, "Rotterdam", "POINT (4.4868 51.913)");

         using var fileStream = new FileStream(fileName, FileMode.Create, FileAccess.Write);
         GeoParquetWriter.Write(fileStream, dataTable, "geometry", GeometryFormat.WKT);
      }

      [TestMethod]
      public void WriteSimpleMany()
      {
         string fileName = Path.Combine(SAMPLES_PATH, "simple-many.parquet");

         // Define data table
         var dataTable = new DataTable();
         dataTable.Columns.Add("id", typeof(int));
         dataTable.Columns.Add("name", typeof(string));
         dataTable.Columns.Add("manygeometries", typeof(string));

         // Add 1 million rows
         for (int i = 0; i < 1000000; i++)
         {
            dataTable.Rows.Add(i, $"RND{i}", $"POINT ({i % 100} {i % 100})");
         }

         GeoParquetWriter.Write(fileName, dataTable, "manygeometries", GeometryFormat.WKT, 500000);
      }

      [TestMethod]
      public void WriteSimpleManyToStream()
      {
         string fileName = Path.Combine(SAMPLES_PATH, "simple-many-streamed.parquet");

         // Define data table
         var dataTable = new DataTable();
         dataTable.Columns.Add("id", typeof(int));
         dataTable.Columns.Add("name", typeof(string));
         dataTable.Columns.Add("manygeometries", typeof(string));

         // Add 1 million rows
         for (int i = 0; i < 1000000; i++)
         {
            dataTable.Rows.Add(i, $"RND{i}", $"POINT ({i % 100} {i % 100})");
         }

         using var fileStream = new FileStream(fileName, FileMode.Create, FileAccess.Write);
         GeoParquetWriter.Write(fileStream, dataTable, "manygeometries", GeometryFormat.WKT);
      }

      [TestMethod]
      public void WriteMultipleGeometryTypes()
      {
         string fileName = Path.Combine(SAMPLES_PATH, "multiple-types.parquet");

         // Define data table
         var dataTable = new DataTable();
         dataTable.Columns.Add("id", typeof(int));
         dataTable.Columns.Add("name", typeof(string));
         dataTable.Columns.Add("geometry", typeof(string));

         // Add rows
         dataTable.Rows.Add(1, "Amsterdam", "POINT (4.8913 52.3684)");
         dataTable.Rows.Add(2, "Rotterdam", "POINT (4.4868 51.913)");
         dataTable.Rows.Add(3, "Connection", "LINESTRING (4.8913 52.3684, 4.4868 51.9130, 4.2949 52.0641)");
         dataTable.Rows.Add(4, "Area", "POLYGON ((4.8913 52.3684, 4.4868 51.9130, 4.2949 52.0641, 4.8913 52.3684))");
         dataTable.Rows.Add(5, "Den Haag", "POINT (4.2949 52.0641)");

         GeoParquetWriter.Write(fileName, dataTable, "geometry", GeometryFormat.WKT);
      }

      [TestMethod]
      public void WriteThreeDimensions()
      {
         string fileName = Path.Combine(SAMPLES_PATH, "3D.parquet");

         // Define data table
         var dataTable = new DataTable();
         dataTable.Columns.Add("id", typeof(int));
         dataTable.Columns.Add("name", typeof(string));
         dataTable.Columns.Add("geometry", typeof(string));

         // Add rows
         dataTable.Rows.Add(1, "Amsterdam", "POINT Z(4.8913 52.3684 6)");
         dataTable.Rows.Add(2, "Rotterdam", "POINT Z(4.4868 51.913 -3.2)");

         // Writer does not support 3D geometries, Z values will be ignored
         GeoParquetWriter.Write(fileName, dataTable, "geometry", GeometryFormat.WKT);
      }

      [TestMethod]
      public void WriteTwoGeometryColumns()
      {
         string fileName = Path.Combine(SAMPLES_PATH, "two-columns.parquet");

         // Define data table with two geometry columns
         var dataTable = new DataTable();
         dataTable.Columns.Add("id", typeof(int));
         dataTable.Columns.Add("name", typeof(string));
         dataTable.Columns.Add("geometry1", typeof(string));
         dataTable.Columns.Add("other name", typeof(string));
         dataTable.Columns.Add("geometry2", typeof(string));

         // Add rows
         dataTable.Rows.Add(1, "Amsterdam", "POINT (4.8913 52.3684)", "Utrecht", "POINT (5.1037 52.0988)");
         dataTable.Rows.Add(2, "Rotterdam", "POINT (4.4868 51.913)", "Den Haag", "POINT (4.2949 52.0641)");

         // Specify geometry columns
         Dictionary<string, GeometryFormat> geoColumns = new()
         {
            { "geometry1", GeometryFormat.WKT },
            { "geometry2", GeometryFormat.WKT }
         };

         GeoParquetWriter.Write(fileName, dataTable, geoColumns, "geometry1");
      }

      [TestMethod]
      public void WriteEmptyDataTable()
      {
         string fileName = Path.Combine(SAMPLES_PATH, "empty.parquet");

         // Define empty data table
         var dataTable = new DataTable();
         dataTable.Columns.Add("id", typeof(int));
         dataTable.Columns.Add("name", typeof(string));
         dataTable.Columns.Add("geometry", typeof(string));

         // No rows added
         GeoParquetWriter.Write(fileName, dataTable, "geometry", GeometryFormat.WKT);
      }

      [TestMethod]
      public void WriteWithNullGeometryColumnWkb()
      {
         string fileName = Path.Combine(SAMPLES_PATH, "null-geometry-column-from-wkb.parquet");

         // Define data table
         var dataTable = new DataTable();
         dataTable.Columns.Add("id", typeof(int));
         dataTable.Columns.Add("name", typeof(string));
         dataTable.Columns.Add("geometry", typeof(byte[]));

         // Add rows with null geometry
         dataTable.Rows.Add(1, "Utrecht", null);
         dataTable.Rows.Add(2, "Rotterdam", new byte[] { 1, 1, 0, 0, 0, 109, 197, 254, 178, 123, 242, 17, 64, 190, 159, 26, 47, 221, 244, 73, 64 });

         GeoParquetWriter.Write(fileName, dataTable, "geometry", GeometryFormat.WKB);
      }

      [TestMethod]
      public void WriteWithNullGeometryColumnWkt()
      {
         string fileName = Path.Combine(SAMPLES_PATH, "null-geometry-column-from-wkt.parquet");

         // Define data table
         var dataTable = new DataTable();
         dataTable.Columns.Add("id", typeof(int));
         dataTable.Columns.Add("name", typeof(string));
         dataTable.Columns.Add("geometry", typeof(string));

         // Add rows with null geometry
         dataTable.Rows.Add(1, "Utrecht", null);
         dataTable.Rows.Add(2, "Den Haag", "POINT(4.2949 52.0641)");

         GeoParquetWriter.Write(fileName, dataTable, "geometry", GeometryFormat.WKT);
      }

      [TestMethod]
      public void WriteWithMixedGeometryFormats()
      {
         string fileName = Path.Combine(SAMPLES_PATH, "mixed-geometry-formats.parquet");

         // Define data table with mixed geometry formats
         var dataTable = new DataTable();
         dataTable.Columns.Add("id", typeof(int));
         dataTable.Columns.Add("name", typeof(string));
         dataTable.Columns.Add("geometry_wkt", typeof(string));
         dataTable.Columns.Add("geometry_wkb", typeof(byte[]));

         // Add rows
         dataTable.Rows.Add(1, "Amsterdam", "POINT (4.8913 52.3684)", new byte[] { 1, 1, 0, 0, 0, 255, 178, 123, 242, 176, 144, 19, 64, 87, 236, 47, 187, 39, 47, 74, 64 });
         dataTable.Rows.Add(2, "Rotterdam", "POINT (4.4868 51.913)", new byte[] { 1, 1, 0, 0, 0, 109, 197, 254, 178, 123, 242, 17, 64, 190, 159, 26, 47, 221, 244, 73, 64 });

         // Specify geometry columns
         Dictionary<string, GeometryFormat> geoColumns = new()
          {
              { "geometry_wkt", GeometryFormat.WKT },
              { "geometry_wkb", GeometryFormat.WKB }
          };

         // Writer does not support mixed geometry formats
         Assert.ThrowsException<GeometryException>(() =>
         {
            GeoParquetWriter.Write(fileName, dataTable, geoColumns, "geometry_wkt");
         });
      }
   }
}