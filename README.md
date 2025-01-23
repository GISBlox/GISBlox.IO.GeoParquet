# GISBlox.IO.GeoParquet

## Introduction
This library allows you to read and write [GeoParquet files](https://github.com/opengeospatial/geoparquet/) in a simple and efficient way.

It uses [ParquetSharp](https://github.com/G-Research/ParquetSharp) under the hood to read and write data from and to GeoParquet files. 

## Installation

Either download this repository, make a git clone or install via NuGet:

```
PM> Install-Package GISBlox.IO.GeoParquet -Version 1.0.0
```

## Reading data

To read data from a GeoParquet file, you can use the `GeoParquetReader` class. The following code snippet shows how to read the geo metadata:

```csharp
using GISBlox.IO.GeoParquet;

string fileName = "path/to/file.parquet";
GeoFileMetadata? metadata = GeoParquetReader.ReadGeoMetadata(fileName);

if (metadata != null)
{    
    Console.WriteLine(metadata.Columns.Count);
    Console.WriteLine(metadata.Primary_column);
}
```

