# .NET 10.0 Upgrade Plan

## Execution Steps

Execute steps below sequentially one by one in the order they are listed.

1. Validate that an .NET 10.0 SDK required for this upgrade is installed on the machine and if not, help to get it installed.
2. Ensure that the SDK version specified in global.json files is compatible with the .NET 10.0 upgrade.
3. Upgrade src/GISBlox.IO.GeoParquet/GISBlox.IO.GeoParquet.csproj
4. Upgrade tools/GISBlox.IO.GeoParquet.CLI/GISBlox.IO.GeoParquet.CLI.csproj
5. Upgrade tests/GISBlox.IO.GeoParquet.Tests/GISBlox.IO.GeoParquet.Tests.csproj
6. Run unit tests to validate upgrade in the projects listed below:
  - tests/GISBlox.IO.GeoParquet.Tests/GISBlox.IO.GeoParquet.Tests.csproj

## Settings

This section contains settings and data used by execution steps.

### Excluded projects

Table below contains projects that do belong to the dependency graph for selected projects and should not be included in the upgrade.

| Project name                                   | Description                 |
|:-----------------------------------------------|:---------------------------:|

### Aggregate NuGet packages modifications across all projects

(No NuGet package modifications required for this upgrade.)

### Project upgrade details
This section contains details about each project upgrade and modifications that need to be done in the project.

#### src/GISBlox.IO.GeoParquet/GISBlox.IO.GeoParquet.csproj modifications

Project properties changes:
  - Target framework should be changed from `net8.0` to `net10.0`

#### tools/GISBlox.IO.GeoParquet.CLI/GISBlox.IO.GeoParquet.CLI.csproj modifications

Project properties changes:
  - Target framework should be changed from `net8.0` to `net10.0`

#### tests/GISBlox.IO.GeoParquet.Tests/GISBlox.IO.GeoParquet.Tests.csproj modifications

Project properties changes:
  - Target framework should be changed from `net8.0` to `net10.0`
