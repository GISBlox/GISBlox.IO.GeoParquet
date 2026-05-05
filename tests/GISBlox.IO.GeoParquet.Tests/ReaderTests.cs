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

         Assert.AreEqual(1, metadata.NumRowGroups);
         Assert.AreEqual(2, metadata.NumRows);
         Assert.HasCount(3, metadata.Columns);
      }

      [TestMethod]
      public void ReadGeoMetadata()
      {
         string fileName = Path.Combine(SAMPLES_PATH, "simple.parquet");

         GeoFileMetadata? metadata = GeoParquetReader.ReadGeoMetadata(fileName);

         Assert.IsNotNull(metadata, "Metadata is null");

         Assert.AreEqual("1.1.0", metadata.Version);
         Assert.HasCount(1, metadata.Columns);
         Assert.AreEqual("geometry", metadata.Primary_column);
         Assert.AreEqual("WKB", metadata.Columns["geometry"].Encoding);

         Assert.IsNotNull(metadata.Columns["geometry"].GeometryTypes);
         ICollection<string>? geometryTypes = metadata.Columns["geometry"].GeometryTypes;
         Assert.IsNotNull(geometryTypes);
         Assert.IsNotNull(geometryTypes.SingleOrDefault(x => x == "Point"));
      }

      [TestMethod]
      public void ReadAllColumnsWkb()
      {
         string fileName = Path.Combine(SAMPLES_PATH, "simple.parquet");

         DataTable dataTable = GeoParquetReader.ReadAll(fileName, GeometryFormat.WKB, 1000);

         Assert.HasCount(3, dataTable.Columns);
         Assert.AreEqual(typeof(byte[]), dataTable.Columns[2].DataType);

         Assert.IsTrue(HasGeoFormat(dataTable.Columns[2], GeometryFormat.WKB));
      }

      [TestMethod]
      public void ReadAllColumnsWkt()
      {
         string fileName = Path.Combine(SAMPLES_PATH, "simple.parquet");

         DataTable dataTable = GeoParquetReader.ReadAll(fileName, GeometryFormat.WKT, 1000);

         Assert.HasCount(3, dataTable.Columns);
         Assert.AreEqual(typeof(string), dataTable.Columns[2].DataType);

         Assert.IsTrue(HasGeoFormat(dataTable.Columns[2], GeometryFormat.WKT));
      }

      [TestMethod]
      public void ReadAllColumnsManyWkb()
      {
         string fileName = Path.Combine(SAMPLES_PATH, "simple-many.parquet");

         DataTable dataTable = GeoParquetReader.ReadAll(fileName, GeometryFormat.WKB);

         Assert.HasCount(3, dataTable.Columns);
         Assert.HasCount(1000000, dataTable.Rows);
      }

      [TestMethod]
      public void ReadAllColumnsManyWkt()
      {
         string fileName = Path.Combine(SAMPLES_PATH, "simple-many.parquet");

         DataTable dataTable = GeoParquetReader.ReadAll(fileName, GeometryFormat.WKT);

         Assert.HasCount(3, dataTable.Columns);
         Assert.HasCount(1000000, dataTable.Rows);
      }

      [TestMethod]
      public void ReadColumnByNameWkb()
      {
         string fileName = Path.Combine(SAMPLES_PATH, "multiple-types.parquet");

         DataTable dataTable = GeoParquetReader.ReadColumn(fileName, "geometry", GeometryFormat.WKB);

         Assert.HasCount(1, dataTable.Columns);
         Assert.AreEqual("geometry", dataTable.Columns[0].ColumnName);
         Assert.AreEqual(typeof(byte[]), dataTable.Columns[0].DataType);

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

         Assert.HasCount(2, dataTable.Columns);
         Assert.AreEqual("name", dataTable.Columns[0].ColumnName);
         Assert.AreEqual("geometry", dataTable.Columns[1].ColumnName);

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

         Assert.HasCount(1, dataTable.Columns);
         Assert.AreEqual("name", dataTable.Columns[0].ColumnName);

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

         Assert.HasCount(2, dataTable.Columns);
         Assert.AreEqual("name", dataTable.Columns[0].ColumnName);

         // Is the 'name' column a valid geometry column?
         Assert.IsFalse(dataTable.Columns[0].ExtendedProperties.ContainsKey("is_geo_column"));

         // Is the 'geometry' column a valid geometry column that contains WKB geometries?
         Assert.AreEqual("geometry", dataTable.Columns[1].ColumnName);
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

         Assert.HasCount(1, dataTable.Columns);
         Assert.AreEqual("geometry", dataTable.Columns[0].ColumnName);
         Assert.AreEqual(typeof(string), dataTable.Columns[0].DataType);

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

         Assert.HasCount(2, dataTable.Columns);
         Assert.AreEqual("name", dataTable.Columns[0].ColumnName);
         Assert.AreEqual("geometry", dataTable.Columns[1].ColumnName);

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

         Assert.HasCount(1, dataTable.Columns);
         Assert.AreEqual("name", dataTable.Columns[0].ColumnName);

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

         Assert.HasCount(2, dataTable.Columns);
         Assert.AreEqual("name", dataTable.Columns[0].ColumnName);

         // Is the 'name' column a valid geometry column?
         Assert.IsFalse(dataTable.Columns[0].ExtendedProperties.ContainsKey("is_geo_column"));

         // Is the 'geometry' column a valid geometry column that contains WKT geometries?
         Assert.AreEqual("geometry", dataTable.Columns[1].ColumnName);
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

         Assert.HasCount(4, dataTable.Columns);
         Assert.AreEqual("name", dataTable.Columns[0].ColumnName);
         Assert.AreEqual("geometry1", dataTable.Columns[1].ColumnName);
         Assert.AreEqual("other name", dataTable.Columns[2].ColumnName);
         Assert.AreEqual("geometry2", dataTable.Columns[3].ColumnName);

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

         Assert.HasCount(4, dataTable.Columns);
         Assert.AreEqual("name", dataTable.Columns[0].ColumnName);
         Assert.AreEqual("geometry1", dataTable.Columns[1].ColumnName);
         Assert.AreEqual("other name", dataTable.Columns[2].ColumnName);
         Assert.AreEqual("geometry2", dataTable.Columns[3].ColumnName);

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

         Assert.HasCount(3, dataTable.Columns);
         Assert.IsEmpty(dataTable.Rows);
      }

      [TestMethod]
      public void ReadNullGeometryColumn()
      {
         string fileName = Path.Combine(SAMPLES_PATH, "null-geometry-column-from-wkt.parquet");

         DataTable dataTable = GeoParquetReader.ReadAll(fileName, GeometryFormat.WKT);

         Assert.HasCount(3, dataTable.Columns);
         Assert.HasCount(2, dataTable.Rows);

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
