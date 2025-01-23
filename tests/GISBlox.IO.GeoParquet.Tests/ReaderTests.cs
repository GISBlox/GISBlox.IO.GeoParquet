using GISBlox.IO.GeoParquet.Common;
using System.Data;

namespace GISBlox.IO.GeoParquet.Tests
{
   [TestClass]
   public class ReaderTests
   {
      private static readonly string BASE_PATH = Path.Combine(AppDomain.CurrentDomain.BaseDirectory);
      private static readonly string SAMPLES_PATH = Path.GetFullPath(Path.Combine(BASE_PATH, "..\\..\\..\\..\\.\\Samples"));

      [TestMethod]
      public void ReadFileMetadata()
      {
         string fileName = Path.Combine(SAMPLES_PATH, "simple.parquet");

         ParquetFileMetadata metadata = GeoParquetReader.ReadFileMetadata(fileName);

         Assert.IsTrue(metadata.NumRowGroups == 1);
         Assert.IsTrue(metadata.NumRows == 2);
         Assert.IsTrue(metadata.Columns.Count == 3);
      }

      [TestMethod]
      public void ReadGeoMetadata()
      {
         string fileName = Path.Combine(SAMPLES_PATH, "simple.parquet");

         GeoFileMetadata? metadata = GeoParquetReader.ReadGeoMetadata(fileName);

         Assert.IsNotNull(metadata, "Metadata is null");

         Assert.IsTrue(metadata.Version == "1.1.0");
         Assert.IsTrue(metadata.Columns.Count == 1);
         Assert.IsTrue(metadata.Primary_column == "geometry");
         Assert.IsTrue(metadata.Columns["geometry"].Encoding == "WKB");

         Assert.IsNotNull(metadata.Columns["geometry"].GeometryTypes);
         ICollection<string>? geometryTypes = metadata.Columns["geometry"].GeometryTypes;
         Assert.IsNotNull(geometryTypes);
         Assert.IsTrue(geometryTypes.SingleOrDefault(x => x == "Point") != null);
      }

      [TestMethod]
      public void ReadAllColumnsWkb()
      {
         string fileName = Path.Combine(SAMPLES_PATH, "simple.parquet");

         DataTable dataTable = GeoParquetReader.ReadAll(fileName, GeometryFormat.WKB, 1000);

         Assert.IsTrue(dataTable.Columns.Count == 3);
         Assert.IsTrue(dataTable.Columns[2].DataType == typeof(byte[]));

         Assert.IsTrue(HasGeoFormat(dataTable.Columns[2], GeometryFormat.WKB));
      }

      [TestMethod]
      public void ReadAllColumnsWkt()
      {
         string fileName = Path.Combine(SAMPLES_PATH, "simple.parquet");

         DataTable dataTable = GeoParquetReader.ReadAll(fileName, GeometryFormat.WKT, 1000);

         Assert.IsTrue(dataTable.Columns.Count == 3);
         Assert.IsTrue(dataTable.Columns[2].DataType == typeof(string));

         Assert.IsTrue(HasGeoFormat(dataTable.Columns[2], GeometryFormat.WKT));
      }

      [TestMethod]
      public void ReadAllColumnsManyWkb()
      {
         string fileName = Path.Combine(SAMPLES_PATH, "simple-many.parquet");

         DataTable dataTable = GeoParquetReader.ReadAll(fileName, GeometryFormat.WKB);

         Assert.IsTrue(dataTable.Columns.Count == 3);
         Assert.IsTrue(dataTable.Rows.Count == 1000000);
      }

      [TestMethod]
      public void ReadAllColumnsManyWkt()
      {
         string fileName = Path.Combine(SAMPLES_PATH, "simple-many.parquet");

         DataTable dataTable = GeoParquetReader.ReadAll(fileName, GeometryFormat.WKT);

         Assert.IsTrue(dataTable.Columns.Count == 3);
         Assert.IsTrue(dataTable.Rows.Count == 1000000);
      }

      [TestMethod]
      public void ReadColumnByNameWkb()
      {
         string fileName = Path.Combine(SAMPLES_PATH, "multiple-types.parquet");

         DataTable dataTable = GeoParquetReader.ReadColumn(fileName, "geometry", GeometryFormat.WKB);

         Assert.IsTrue(dataTable.Columns.Count == 1);
         Assert.IsTrue(dataTable.Columns[0].ColumnName == "geometry");
         Assert.IsTrue(dataTable.Columns[0].DataType == typeof(byte[]));

         // Is the 'geometry' column a valid geometry column that contains WKB geometries?
         Assert.IsTrue(dataTable.Columns[0].ExtendedProperties.ContainsKey("is_geo_column"));
         Assert.IsTrue(HasGeoFormat(dataTable.Columns[0], GeometryFormat.WKB));

         // Sample first row
         var firstRow = dataTable.AsEnumerable().First();
         CollectionAssert.AreEqual(new byte[] { 1, 1, 0, 0, 0, 255, 178, 123, 242, 176, 144, 19, 64, 87, 236, 47, 187, 39, 47, 74, 64 }, (byte[])firstRow["geometry"]);
      }

      [TestMethod]
      public void ReadColumnsByNameWkb()
      {
         string fileName = Path.Combine(SAMPLES_PATH, "multiple-types.parquet");
         IList<string> columnNames = ["name", "geometry"];

         DataTable dataTable = GeoParquetReader.ReadColumns(fileName, columnNames, GeometryFormat.WKB);

         Assert.IsTrue(dataTable.Columns.Count == 2);
         Assert.IsTrue(dataTable.Columns[0].ColumnName == "name");
         Assert.IsTrue(dataTable.Columns[1].ColumnName == "geometry");

         // Is the 'geometry' column a valid geometry column that contains WKB geometries?
         Assert.IsTrue(dataTable.Columns[1].ExtendedProperties.ContainsKey("is_geo_column"));
         Assert.IsTrue(HasGeoFormat(dataTable.Columns[1], GeometryFormat.WKB));

         // Sample first row
         var firstRow = dataTable.AsEnumerable().First();
         Assert.AreEqual("Amsterdam", firstRow["name"]);
         CollectionAssert.AreEqual(new byte[] { 1, 1, 0, 0, 0, 255, 178, 123, 242, 176, 144, 19, 64, 87, 236, 47, 187, 39, 47, 74, 64 }, (byte[])firstRow["geometry"]);
      }

      [TestMethod]
      public void ReadColumnByIndexWkb()
      {
         string fileName = Path.Combine(SAMPLES_PATH, "multiple-types.parquet");
         int columnIndex = 1; // 'name' column

         DataTable dataTable = GeoParquetReader.ReadColumn(fileName, columnIndex, GeometryFormat.WKB);

         Assert.IsTrue(dataTable.Columns.Count == 1);
         Assert.IsTrue(dataTable.Columns[0].ColumnName == "name");

         // Is the 'name' column a valid geometry column?
         Assert.IsFalse(dataTable.Columns[0].ExtendedProperties.ContainsKey("is_geo_column"));

         // Sample first row
         var firstRow = dataTable.AsEnumerable().First();
         Assert.AreEqual("Amsterdam", firstRow["name"]);
      }

      [TestMethod]
      public void ReadColumnsByIndexWkb()
      {
         string fileName = Path.Combine(SAMPLES_PATH, "multiple-types.parquet");
         IList<int> columnIndexes = [1, 2];

         DataTable dataTable = GeoParquetReader.ReadColumns(fileName, columnIndexes, GeometryFormat.WKB);

         Assert.IsTrue(dataTable.Columns.Count == 2);
         Assert.IsTrue(dataTable.Columns[0].ColumnName == "name");

         // Is the 'name' column a valid geometry column?
         Assert.IsFalse(dataTable.Columns[0].ExtendedProperties.ContainsKey("is_geo_column"));

         // Is the 'geometry' column a valid geometry column that contains WKB geometries?
         Assert.IsTrue(dataTable.Columns[1].ColumnName == "geometry");
         Assert.IsTrue(dataTable.Columns[1].ExtendedProperties.ContainsKey("is_geo_column"));
         Assert.IsTrue(HasGeoFormat(dataTable.Columns[1], GeometryFormat.WKB));

         // Sample first row
         var firstRow = dataTable.AsEnumerable().First();
         Assert.AreEqual("Amsterdam", firstRow["name"]);
         CollectionAssert.AreEqual(new byte[] { 1, 1, 0, 0, 0, 255, 178, 123, 242, 176, 144, 19, 64, 87, 236, 47, 187, 39, 47, 74, 64 }, (byte[])firstRow["geometry"]);
      }

      [TestMethod]
      public void ReadColumnByNameWkt()
      {
         string fileName = Path.Combine(SAMPLES_PATH, "multiple-types.parquet");

         DataTable dataTable = GeoParquetReader.ReadColumn(fileName, "geometry", GeometryFormat.WKT);

         Assert.IsTrue(dataTable.Columns.Count == 1);
         Assert.IsTrue(dataTable.Columns[0].ColumnName == "geometry");
         Assert.IsTrue(dataTable.Columns[0].DataType == typeof(string));

         // Is the 'geometry' column a valid geometry column that contains WKT geometries?
         Assert.IsTrue(dataTable.Columns[0].ExtendedProperties.ContainsKey("is_geo_column"));
         Assert.IsTrue(HasGeoFormat(dataTable.Columns[0], GeometryFormat.WKT));

         // Sample first row
         var firstRow = dataTable.AsEnumerable().First();
         Assert.AreEqual("POINT (4.8913 52.3684)", firstRow["geometry"]);
      }

      [TestMethod]
      public void ReadColumnsByNameWkt()
      {
         string fileName = Path.Combine(SAMPLES_PATH, "multiple-types.parquet");
         IList<string> columnNames = ["name", "geometry"];

         DataTable dataTable = GeoParquetReader.ReadColumns(fileName, columnNames, GeometryFormat.WKT);

         Assert.IsTrue(dataTable.Columns.Count == 2);
         Assert.IsTrue(dataTable.Columns[0].ColumnName == "name");
         Assert.IsTrue(dataTable.Columns[1].ColumnName == "geometry");

         // Is the 'geometry' column a valid geometry column that contains WKT geometries?
         Assert.IsTrue(dataTable.Columns[1].ExtendedProperties.ContainsKey("is_geo_column"));
         Assert.IsTrue(HasGeoFormat(dataTable.Columns[1], GeometryFormat.WKT));

         // Sample first row
         var firstRow = dataTable.AsEnumerable().First();
         Assert.AreEqual("Amsterdam", firstRow["name"]);
         Assert.AreEqual("POINT (4.8913 52.3684)", firstRow["geometry"]);
      }

      [TestMethod]
      public void ReadColumnByIndexWkt()
      {
         string fileName = Path.Combine(SAMPLES_PATH, "multiple-types.parquet");
         int columnIndex = 1; // 'name' column

         DataTable dataTable = GeoParquetReader.ReadColumn(fileName, columnIndex, GeometryFormat.WKT);

         Assert.IsTrue(dataTable.Columns.Count == 1);
         Assert.IsTrue(dataTable.Columns[0].ColumnName == "name");

         // Is the 'name' column a valid geometry column?
         Assert.IsFalse(dataTable.Columns[0].ExtendedProperties.ContainsKey("is_geo_column"));

         // Sample first row
         var firstRow = dataTable.AsEnumerable().First();
         Assert.AreEqual("Amsterdam", firstRow["name"]);
      }

      [TestMethod]
      public void ReadColumnsByIndexWkt()
      {
         string fileName = Path.Combine(SAMPLES_PATH, "multiple-types.parquet");
         IList<int> columnIndexes = [1, 2];

         DataTable dataTable = GeoParquetReader.ReadColumns(fileName, columnIndexes, GeometryFormat.WKT);

         Assert.IsTrue(dataTable.Columns.Count == 2);
         Assert.IsTrue(dataTable.Columns[0].ColumnName == "name");

         // Is the 'name' column a valid geometry column?
         Assert.IsFalse(dataTable.Columns[0].ExtendedProperties.ContainsKey("is_geo_column"));

         // Is the 'geometry' column a valid geometry column that contains WKT geometries?
         Assert.IsTrue(dataTable.Columns[1].ColumnName == "geometry");
         Assert.IsTrue(dataTable.Columns[1].ExtendedProperties.ContainsKey("is_geo_column"));
         Assert.IsTrue(HasGeoFormat(dataTable.Columns[1], GeometryFormat.WKT));

         // Sample first row
         var firstRow = dataTable.AsEnumerable().First();
         Assert.AreEqual("Amsterdam", firstRow["name"]);
         Assert.AreEqual("POINT (4.8913 52.3684)", firstRow["geometry"]);
      }

      [TestMethod]
      public void ReadTwoGeometryColumnsWkb()
      {
         string fileName = Path.Combine(SAMPLES_PATH, "two-columns.parquet");
         IList<string> columnNames = ["name", "geometry1", "other name", "geometry2"];

         DataTable dataTable = GeoParquetReader.ReadColumns(fileName, columnNames, GeometryFormat.WKB);

         Assert.IsTrue(dataTable.Columns.Count == 4);
         Assert.IsTrue(dataTable.Columns[0].ColumnName == "name");
         Assert.IsTrue(dataTable.Columns[1].ColumnName == "geometry1");
         Assert.IsTrue(dataTable.Columns[2].ColumnName == "other name");
         Assert.IsTrue(dataTable.Columns[3].ColumnName == "geometry2");

         // Check if the 'geometry1' and 'geometry2' columns are valid geometry columns that contain WKB geometries
         Assert.IsTrue(dataTable.Columns[1].ExtendedProperties.ContainsKey("is_geo_column"));
         Assert.IsTrue(HasGeoFormat(dataTable.Columns[1], GeometryFormat.WKB));
         Assert.IsTrue(dataTable.Columns[1].ExtendedProperties.ContainsKey("is_primary_geo_column"));

         Assert.IsTrue(dataTable.Columns[3].ExtendedProperties.ContainsKey("is_geo_column"));
         Assert.IsTrue(HasGeoFormat(dataTable.Columns[3], GeometryFormat.WKB));

         // Sample first row
         var firstRow = dataTable.AsEnumerable().First();
         Assert.AreEqual("Amsterdam", firstRow["name"]);
         CollectionAssert.AreEqual(new byte[] { 1, 1, 0, 0, 0, 255, 178, 123, 242, 176, 144, 19, 64, 87, 236, 47, 187, 39, 47, 74, 64 }, (byte[])firstRow["geometry1"]);
         Assert.AreEqual("Utrecht", firstRow["other name"]);
         CollectionAssert.AreEqual(new byte[] { 1, 1, 0, 0, 0, 124, 97, 50, 85, 48, 106, 20, 64, 34, 108, 120, 122, 165, 12, 74, 64 }, (byte[])firstRow["geometry2"]);
      }

      [TestMethod]
      public void ReadTwoGeometryColumnsWkt()
      {
         string fileName = Path.Combine(SAMPLES_PATH, "two-columns.parquet");
         IList<string> columnNames = ["name", "geometry1", "other name", "geometry2"];

         DataTable dataTable = GeoParquetReader.ReadColumns(fileName, columnNames, GeometryFormat.WKT);

         Assert.IsTrue(dataTable.Columns.Count == 4);
         Assert.IsTrue(dataTable.Columns[0].ColumnName == "name");
         Assert.IsTrue(dataTable.Columns[1].ColumnName == "geometry1");
         Assert.IsTrue(dataTable.Columns[2].ColumnName == "other name");
         Assert.IsTrue(dataTable.Columns[3].ColumnName == "geometry2");

         // Check if the 'geometry1' and 'geometry2' columns are valid geometry columns that contain WKT geometries
         Assert.IsTrue(dataTable.Columns[1].ExtendedProperties.ContainsKey("is_geo_column"));
         Assert.IsTrue(HasGeoFormat(dataTable.Columns[1], GeometryFormat.WKT));
         Assert.IsTrue(dataTable.Columns[1].ExtendedProperties.ContainsKey("is_primary_geo_column"));

         Assert.IsTrue(dataTable.Columns[3].ExtendedProperties.ContainsKey("is_geo_column"));
         Assert.IsTrue(HasGeoFormat(dataTable.Columns[3], GeometryFormat.WKT));

         // Sample first row
         var firstRow = dataTable.AsEnumerable().First();
         Assert.AreEqual("Amsterdam", firstRow["name"]);
         Assert.AreEqual("POINT (4.8913 52.3684)", firstRow["geometry1"]);
         Assert.AreEqual("Utrecht", firstRow["other name"]);
         Assert.AreEqual("POINT (5.1037 52.0988)", firstRow["geometry2"]);
      }

      [TestMethod]
      public void ReadEmptyDataTable()
      {
         string fileName = Path.Combine(SAMPLES_PATH, "empty.parquet");

         DataTable dataTable = GeoParquetReader.ReadAll(fileName, GeometryFormat.WKB);

         Assert.IsTrue(dataTable.Columns.Count == 3);
         Assert.IsTrue(dataTable.Rows.Count == 0);
      }

      [TestMethod]
      public void ReadNullGeometryColumn()
      {
         string fileName = Path.Combine(SAMPLES_PATH, "null-geometry-column-from-wkt.parquet");

         DataTable dataTable = GeoParquetReader.ReadAll(fileName, GeometryFormat.WKT);

         Assert.IsTrue(dataTable.Columns.Count == 3);
         Assert.IsTrue(dataTable.Rows.Count == 2);

         // Check if the 'geometry1' column is a valid geometry column that contain WKT geometries
         Assert.IsTrue(dataTable.Columns[2].ExtendedProperties.ContainsKey("is_geo_column"));
         Assert.IsTrue(HasGeoFormat(dataTable.Columns[2], GeometryFormat.WKT));
         Assert.IsTrue(dataTable.Columns[2].ExtendedProperties.ContainsKey("is_primary_geo_column"));

         // Sample rows
         var firstRow = dataTable.AsEnumerable().First();
         Assert.AreEqual("Utrecht", firstRow["name"]);
         Assert.AreEqual(DBNull.Value, firstRow["geometry"]);

         var secondRow = dataTable.AsEnumerable().Skip(1).First();
         Assert.AreEqual("Den Haag", secondRow["name"]);
         Assert.AreEqual("POINT (4.2949 52.0641)", secondRow["geometry"]);
      }

      private static bool HasGeoFormat(DataColumn column, GeometryFormat format)
      {
         return column.ExtendedProperties.ContainsKey("geo_format") && Enum.Parse<GeometryFormat>(column.ExtendedProperties["geo_format"]?.ToString() ?? string.Empty) == format;
      }
   }
}
