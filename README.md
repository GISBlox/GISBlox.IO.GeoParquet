# GISBlox.IO.GeoParquet

## Introduction
This library allows you to read and write [GeoParquet files](https://github.com/opengeospatial/geoparquet/) in a simple and efficient way to and from [System.Data.DataTable](https://learn.microsoft.com/en-us/dotnet/api/system.data.datatable?view=net-9.0)s.

It uses [ParquetSharp](https://github.com/G-Research/ParquetSharp) under the hood to read and write data from and to GeoParquet files. 

## Features

- Read GeoParquet metadata.
- Read GeoParquet files into a `DataTable`.
- Write a `DataTable` to a GeoParquet file.
- Automatically infers the schema of the `GeoParquet` file from the `DataTable` and vice-versa.
- Support for reading and writing geometries in WKT and WKB formats.

## Installation

Stable releases are hosted on the default NuGet feed. You can install them using the following command on the package manager command line:

```
PM> Install-Package GISBlox.IO.GeoParquet
```

## Reading data

To read data from a GeoParquet file, you can use the `GeoParquetReader` class. 

### Metadata

The following code snippet shows how to read the file's GeoParquet [metadata](https://github.com/opengeospatial/geoparquet/blob/main/format-specs/geoparquet.md#metadata), which defines how geospatial data is stored, including the representation of geometries and the required additional metadata: 

```csharp
using GISBlox.IO.GeoParquet;

string fileName = "path/to/file.parquet";

GeoFileMetadata metadata = GeoParquetReader.ReadGeoMetadata(fileName);
```

The `ReadGeoMetadata` method returns a `GeoFileMetadata` type with information according to version 1.1.0 of the [GeoParquet Specification](https://geoparquet.org/releases/v1.1.0/).
A more detailed example is available [here](/tests/GISBlox.IO.GeoParquet.Tests/ReaderTests.cs#L25).

### Read all columns

To read the entire file you can use the following code snippet:

```csharp
string fileName = "path/to/file.parquet";

DataTable dataTable = GeoParquetReader.ReadAll(fileName, GeometryFormat.WKT);
```

The `ReadAll` method reads all data from a GeoParquet file into a `DataTable`.
The `format` parameter specifies the desired format for the geometries in the `DataTable`. The supported formats are `WKT` (Well-Known Text) and `WKB` (Well-Known Binary).

### Read one column

To read a single column from a file you can use the following code:
```csharp
string fileName = "path/to/file.parquet";

DataTable dataTable = GeoParquetReader.ReadColumn(fileName, "geometry", GeometryFormat.WKT);
```

The `ReadColumn` method reads a single column from a GeoParquet file into a `DataTable`. 
The `column` parameter either specifies the name of the column to read or the column index:

```csharp
string fileName = "path/to/file.parquet";

// Read the second column from the file in WKB format
DataTable dataTable = GeoParquetReader.ReadColumn(fileName, 1, GeometryFormat.WKB);
```

:point_right: You can retrieve any column from the file, not just the geometry column(s).

### Read two or more columns

In case you want to read two or more columns from a file you can use the following code:

```csharp
string fileName = "path/to/file.parquet";
IList<string> columnNames = ["name", "geometry"];

DataTable dataTable = GeoParquetReader.ReadColumns(fileName, columnNames, GeometryFormat.WKT);
```

The `ReadColumns` method reads two or more columns from a GeoParquet file into a `DataTable`.
The `columnNames` parameter specifies the names of the columns to read. Another overload of this method allows you to specify the column indexes instead of the column names:

```csharp
string fileName = "path/to/file.parquet";
IList<int> columnIndexes = [1, 2];

// Read the second and third columns from the file in WKB format
DataTable dataTable = GeoParquetReader.ReadColumns(fileName, columnIndexes, GeometryFormat.WKB);
```
:point_right: You can retrieve any number of columns from the file, not just the geometry column(s).

## Writing data

To write data to a GeoParquet file, you can use the `GeoParquetWriter` class.

### Write WKT geometries

The following code snippet shows how to write a `DataTable` with `WKT` geometries to a GeoParquet file:

```csharp
string fileName = "path/to/file.parquet";

// Define data table
var dataTable = new DataTable();
dataTable.Columns.Add("id", typeof(int));
dataTable.Columns.Add("name", typeof(string));
dataTable.Columns.Add("geometry", typeof(string));

// Add rows
dataTable.Rows.Add(1, "Amsterdam", "POINT (4.8913 52.3684)");
dataTable.Rows.Add(2, "Rotterdam", "POINT (4.4868 51.913)");

GeoParquetWriter.Write(fileName, dataTable, "geometry", GeometryFormat.WKT);
```

The `geometryColumn` parameter specifies the name of the geometry column in the `DataTable`. The `format` parameter specifies the format of the geometries in the `DataTable`. The supported formats are `WKT` (Well-Known Text) and `WKB` (Well-Known Binary).

The GeoParquetWriter supports writing to either a file or a [System.IO.Stream](https://learn.microsoft.com/en-us/dotnet/api/system.io.stream?view=net-9.0). The following sample shows how to write to a stream:
```csharp
// Continuing from the previous example...
using var fileStream = new FileStream(fileName, FileMode.Create, FileAccess.Write);
GeoParquetWriter.Write(fileStream, dataTable, "geometry", GeometryFormat.WKT);
```

### Write WKB geometries

Writing WKB geometries is similar to writing WKT geometries. The following code snippet shows how to write a `DataTable` with `WKB` geometries to a GeoParquet file:
```csharp
string fileName = "path/to/file.parquet";

// Define data table
var dataTable = new DataTable();
dataTable.Columns.Add("id", typeof(int));
dataTable.Columns.Add("name", typeof(string));
dataTable.Columns.Add("geometry", typeof(byte[]));

// Add rows
dataTable.Rows.Add(1, "Amsterdam", new byte[] { 1, 1, 0, 0, 0, 255, 178, 123, 242, 176, 144, 19, 64, 87, 236, 47, 187, 39, 47, 74, 64 });
dataTable.Rows.Add(2, "Rotterdam", new byte[] { 1, 1, 0, 0, 0, 109, 197, 254, 178, 123, 242, 17, 64, 190, 159, 26, 47, 221, 244, 73, 64 });

GeoParquetWriter.Write(fileName, dataTable, "geometry", GeometryFormat.WKB);
```

### Write multiple geometry types

The following code snippet shows how to write a `DataTable` with multiple geometry types:
```csharp
string fileName = "path/to/file.parquet";

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
```

Inspecting the geo metadata of the new file will show that the geometries are stored in the `geometry` column with the correct geometry types:

```json
{
  "version": "1.1.0",
  "primary_column": "geometry",
  "columns": {
    "geometry": {
      "additionalProperties": false,
      "bbox": [
        4.2949,
        51.913,
        4.8913,
        52.3684
      ],
      "edges": "Planar",
      "encoding": "WKB",
      "epoch": 0,
      "geometry_types": [
        "Point",
        "LineString",
        "Polygon"
      ]
    }
  }
}
```

### Write two or more geometry columns

A GeoParquet file may contain more than one geometry column. The following code snippet shows how to write a `DataTable` with two geometry columns:
```csharp
string fileName = "path/to/file.parquet";

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
```

:point_right: All these examples, and more, are available in the [Test](/tests/GISBlox.IO.GeoParquet.Tests) project.

## Dependencies

- [ParquetSharp 16.1.0](https://github.com/G-Research/ParquetSharp)
- [NetTopologySuite 2.5.0](https://github.com/NetTopologySuite/NetTopologySuite)

## Questions

- Do you have questions? Please [join our Discussions Forum](https://github.com/GISBlox/GISBlox.IO.GeoParquet/discussions).
- Do you want to report a bug? Please [create a new issue](https://github.com/GISBlox/GISBlox.IO.GeoParquet/issues).

