using GISBlox.IO.GeoParquet.Common;
using System.Data;

namespace GISBlox.IO.GeoParquet.Tests
{
   [TestClass]
   public class RoundRobinTests
   {
      private static readonly string BASE_PATH = Path.Combine(AppDomain.CurrentDomain.BaseDirectory);
      private static readonly string SAMPLES_PATH = Path.GetFullPath(Path.Combine(BASE_PATH, "..\\..\\..\\..\\.\\Samples"));

      [TestMethod]
      public void WriteAndReadSingleGeometryType()
      {
         string fileName = Path.Combine(SAMPLES_PATH, "rr-single.parquet");

         // Define data table
         var source = new DataTable();
         source.Columns.Add("id", typeof(int));
         source.Columns.Add("name", typeof(string));
         source.Columns.Add("geometry", typeof(string));

         // Add rows
         source.Rows.Add(1, "Amsterdam", "POINT (4.8913 52.3684)");
         source.Rows.Add(2, "Rotterdam", "POINT (4.4868 51.913)");
         source.Rows.Add(3, "Den Haag", "POINT (4.2949 52.0641)");

         // Write to file
         GeoParquetWriter.Write(fileName, source, "geometry", GeometryFormat.WKT);

         // Read from file
         Assert.IsTrue(File.Exists(fileName));
         DataTable target = GeoParquetReader.ReadAll(fileName, GeometryFormat.WKT);

         // Validate
         Assert.IsTrue(target.Columns.Count == 3);
         Assert.IsTrue(target.Rows.Count == 3);

         Assert.IsTrue(target.Columns["id"]?.DataType == typeof(int));
         Assert.IsTrue(target.Columns["name"]?.DataType == typeof(string));
         Assert.IsTrue(target.Columns["geometry"]?.DataType == typeof(string));

         // Sample rows
         Assert.IsTrue(target.Rows[0]["id"].Equals(1));
         Assert.IsTrue(target.Rows[0]["name"].Equals("Amsterdam"));
         Assert.IsTrue(target.Rows[0]["geometry"].Equals("POINT (4.8913 52.3684)"));

         Assert.IsTrue(target.Rows[1]["id"].Equals(2));
         Assert.IsTrue(target.Rows[1]["name"].Equals("Rotterdam"));
         Assert.IsTrue(target.Rows[1]["geometry"].Equals("POINT (4.4868 51.913)"));

         Assert.IsTrue(target.Rows[2]["id"].Equals(3));
         Assert.IsTrue(target.Rows[2]["name"].Equals("Den Haag"));
         Assert.IsTrue(target.Rows[2]["geometry"].Equals("POINT (4.2949 52.0641)"));
      }

      [TestMethod]
      public void WriteAndReadSingleGeometryTypeMany()
      {
         string fileName = Path.Combine(SAMPLES_PATH, "rr-single-type-many.parquet");

         // Define data table
         var source = new DataTable();
         source.Columns.Add("id", typeof(int));
         source.Columns.Add("name", typeof(string));
         source.Columns.Add("manygeometries", typeof(string));

         // Add 1 million rows
         for (int i = 0; i < 1000000; i++)
         {
            source.Rows.Add(i, $"RND{i}", $"POINT ({i % 100} {i % 100})");
         }

         // Write to file
         GeoParquetWriter.Write(fileName, source, "manygeometries", GeometryFormat.WKT);

         // Read from file
         Assert.IsTrue(File.Exists(fileName));
         DataTable target = GeoParquetReader.ReadAll(fileName, GeometryFormat.WKT);

         // Validate         
         Assert.IsTrue(target.Columns.Count == 3);
         Assert.IsTrue(target.Rows.Count == 1000000);

         Assert.IsTrue(target.Columns["id"]?.DataType == typeof(int));
         Assert.IsTrue(target.Columns["name"]?.DataType == typeof(string));
         Assert.IsTrue(target.Columns["manygeometries"]?.DataType == typeof(string));

         // Sample rows
         for (int i = 0; i < 1000000; i++)
         {
            Assert.IsTrue(target.Rows[i]["id"].Equals(i));
            Assert.IsTrue(target.Rows[i]["name"].Equals($"RND{i}"));
            Assert.IsTrue(target.Rows[i]["manygeometries"].Equals($"POINT ({i % 100} {i % 100})"));
         }
      }

      [TestMethod]
      public void WriteAndReadMultipleGeometryTypes()
      {
         string fileName = Path.Combine(SAMPLES_PATH, "rr-multiple-types.parquet");

         // Define data table
         var source = new DataTable();
         source.Columns.Add("id", typeof(int));
         source.Columns.Add("name", typeof(string));
         source.Columns.Add("geometry", typeof(string));

         // Add rows
         source.Rows.Add(1, "Amsterdam", "POINT (4.8913 52.3684)");
         source.Rows.Add(2, "Rotterdam", "POINT (4.4868 51.913)");
         source.Rows.Add(3, "Connection", "LINESTRING (4.8913 52.3684, 4.4868 51.913, 4.2949 52.0641)");
         source.Rows.Add(4, "Area", "POLYGON ((4.8913 52.3684, 4.4868 51.913, 4.2949 52.0641, 4.8913 52.3684))");
         source.Rows.Add(5, "Den Haag", "POINT (4.2949 52.0641)");

         // Write to file
         GeoParquetWriter.Write(fileName, source, "geometry", GeometryFormat.WKT);

         // Read from file
         Assert.IsTrue(File.Exists(fileName));
         DataTable target = GeoParquetReader.ReadAll(fileName, GeometryFormat.WKT);

         // Validate         
         Assert.IsTrue(target.Columns.Count == 3);
         Assert.IsTrue(target.Rows.Count == 5);

         Assert.IsTrue(target.Columns["id"]?.DataType == typeof(int));
         Assert.IsTrue(target.Columns["name"]?.DataType == typeof(string));
         Assert.IsTrue(target.Columns["geometry"]?.DataType == typeof(string));

         // Sample rows
         Assert.IsTrue(target.Rows[0]["id"].Equals(1));
         Assert.IsTrue(target.Rows[0]["name"].Equals("Amsterdam"));
         Assert.IsTrue(target.Rows[0]["geometry"].Equals("POINT (4.8913 52.3684)"));

         Assert.IsTrue(target.Rows[1]["id"].Equals(2));
         Assert.IsTrue(target.Rows[1]["name"].Equals("Rotterdam"));
         Assert.IsTrue(target.Rows[1]["geometry"].Equals("POINT (4.4868 51.913)"));

         Assert.IsTrue(target.Rows[2]["id"].Equals(3));
         Assert.IsTrue(target.Rows[2]["name"].Equals("Connection"));
         Assert.IsTrue(target.Rows[2]["geometry"].Equals("LINESTRING (4.8913 52.3684, 4.4868 51.913, 4.2949 52.0641)"));

         Assert.IsTrue(target.Rows[3]["id"].Equals(4));
         Assert.IsTrue(target.Rows[3]["name"].Equals("Area"));
         Assert.IsTrue(target.Rows[3]["geometry"].Equals("POLYGON ((4.8913 52.3684, 4.4868 51.913, 4.2949 52.0641, 4.8913 52.3684))"));

         Assert.IsTrue(target.Rows[4]["id"].Equals(5));
         Assert.IsTrue(target.Rows[4]["name"].Equals("Den Haag"));
         Assert.IsTrue(target.Rows[4]["geometry"].Equals("POINT (4.2949 52.0641)"));
      }

      [TestMethod]
      public void WriteAndReadSingleTypeWithNullGeometry()
      {
         string fileName = Path.Combine(SAMPLES_PATH, "rr-single-with-null.parquet");

         // Define data table
         var source = new DataTable();
         source.Columns.Add("id", typeof(int));
         source.Columns.Add("name", typeof(string));
         source.Columns.Add("geometry", typeof(string));

         // Add rows
         source.Rows.Add(1, "Amsterdam", "POINT (4.8913 52.3684)");
         source.Rows.Add(2, "Rotterdam", null);
         source.Rows.Add(3, "Den Haag", "POINT (4.2949 52.0641)");

         // Write to file
         GeoParquetWriter.Write(fileName, source, "geometry", GeometryFormat.WKT);

         // Read from file
         Assert.IsTrue(File.Exists(fileName));
         DataTable target = GeoParquetReader.ReadAll(fileName, GeometryFormat.WKT);

         // Validate
         Assert.IsTrue(target.Columns.Count == 3);
         Assert.IsTrue(target.Rows.Count == 3);

         Assert.IsTrue(target.Columns["id"]?.DataType == typeof(int));
         Assert.IsTrue(target.Columns["name"]?.DataType == typeof(string));
         Assert.IsTrue(target.Columns["geometry"]?.DataType == typeof(string));

         // Sample rows
         Assert.IsTrue(target.Rows[0]["id"].Equals(1));
         Assert.IsTrue(target.Rows[0]["name"].Equals("Amsterdam"));
         Assert.IsTrue(target.Rows[0]["geometry"].Equals("POINT (4.8913 52.3684)"));

         Assert.IsTrue(target.Rows[1]["id"].Equals(2));
         Assert.IsTrue(target.Rows[1]["name"].Equals("Rotterdam"));
         Assert.IsTrue(target.Rows[1]["geometry"].Equals(DBNull.Value));

         Assert.IsTrue(target.Rows[2]["id"].Equals(3));
         Assert.IsTrue(target.Rows[2]["name"].Equals("Den Haag"));
         Assert.IsTrue(target.Rows[2]["geometry"].Equals("POINT (4.2949 52.0641)"));
      }
   }
}
