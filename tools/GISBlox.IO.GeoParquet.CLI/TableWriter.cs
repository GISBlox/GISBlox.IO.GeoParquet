namespace GISBlox.IO.GeoParquet.CLI
{
   /// <summary>
   /// This class provides methods to output data in table format on the command line.
   /// </summary>
   internal class TableWriter : IDisposable
   {
      Row? _row;
      List<Row>? _rows;
      List<ColumnMetaData>? _columnMetaData = [];
    
      int _maxColumns;
      int _currentPos;
      readonly int _tablePadding;
      readonly bool _isNameValueTable;

      const int COLUMN_PADDING = 6;
      const char TABLE_HEADER_SEPARATOR = '-';

      #region Constructor / Destructor

      /// <summary>
      /// Initializes a new instance of the <see cref="TableWriter"/> class.
      /// </summary>
      /// <param name="padding">The amount of spaces to add to the left of the table when written to the console.</param>
      public TableWriter(int padding = 0)
      {         
         _tablePadding = padding;
      }

      /// <summary>
      /// Initializes a new instance of the <see cref="TableWriter"/> class.
      /// </summary>
      /// <param name="isNameValueTable">Determines whether the table is a 2 column name/value table.</param>
      /// <param name="padding">The amount of spaces to add to the left of the table when written to the console.</param>
      public TableWriter(bool isNameValueTable, int padding = 0)
      { 
         _tablePadding = padding;
         _isNameValueTable = isNameValueTable;
      }

      /// <summary>
      /// Releases used resources.
      /// </summary>
      public void Dispose()
      {
         _row = null;
         _rows = null;
         _columnMetaData = null;
      }

      #endregion

      #region Public methods

      /// <summary>
      /// Creates a table.
      /// </summary>
      /// <param name="columns">The amount of columns in the table.</param>
      public void StartTable(int columns)
      {
         _maxColumns = _isNameValueTable ? 2 : columns;
         _rows = [];
         _columnMetaData = [];
      }

      /// <summary>
      /// Creates a table and initializes it based on the amount of public properties of a type.
      /// </summary>      
      /// <param name="source">An instance of type T.</param>
      public void StartTable<T>(T source)
      {
         StartTable(source?.GetType().GetProperties().Length);
      }

      /// <summary>
      /// Creates a table and initializes it based on the amount of public properties of the first type in the List.
      /// </summary>      
      /// <param name="source">A List with types</param>
      public void StartTable<T>(List<T> source)
      {
         StartTable(source[0]);
      }

      /// <summary>
      /// Creates a table with 2 columns for name/value pairs.
      /// </summary>
      /// <param name="columnName1">The name of the first column.</param>
      /// <param name="columnName2">The name of the second column.</param>
      /// <param name="column1Color">The foreground color of the first column.</param>
      /// <param name="column2Color">The foreground color of the second column.</param>
      public void StartNameValueTable(string columnName1, string columnName2, ConsoleColor column1Color = ConsoleColor.Gray, ConsoleColor column2Color = ConsoleColor.Gray)
      {
         StartTable(2);
         StartRow(isHeaderRow: true);
         AddColumn(columnName1, column1Color);
         AddColumn(columnName2, column2Color);
         EndRow();
      }

      /// <summary>
      /// Creates a table row.
      /// </summary>
      /// <param name="isHeaderRow">Determines whether this row is the table header row.</param>  
      /// <exception cref="InvalidOperationException"></exception>
      public void StartRow(bool isHeaderRow = false)
      {
         if (_row != null)
            throw new InvalidOperationException($"Invalid row state: the current row needs to be ended before a new row can be created.");

         _row = new Row() { IsHeaderRow = isHeaderRow };
      }

      /// <summary>
      /// Creates a column and adds it to the current row.
      /// </summary>
      /// <param name="columnValue">The value of the column.</param>
      /// <param name="color">The foreground color.</param>
      /// <exception cref="ArgumentOutOfRangeException"></exception>
      public void AddColumn(string columnValue, ConsoleColor color = ConsoleColor.Gray)
      {
         if (_row?.Columns.Count == _maxColumns)
            throw new ArgumentOutOfRangeException(nameof(columnValue), $"Cannot exceed the specified amount of columns for this table ({_maxColumns} columns)");

         _row?.Columns.Add(new Column(columnValue, _currentPos, color));
         _currentPos++;
      }

      /// <summary>
      /// Splits data into a single column and multiple rows using the specified separator.
      /// </summary>
      /// <param name="data">The data to be split.</param>
      /// <param name="separator">The separator charactor.</param>
      /// <param name="color">The foreground color.</param>
      public void AddCSVData(string data, char separator, ConsoleColor color = ConsoleColor.Gray)
      {
         if (!string.IsNullOrEmpty(data))
         {
            string[] rowData = data.Split([separator], StringSplitOptions.RemoveEmptyEntries);

            foreach (var colData in rowData)
            {
               StartRow();
               AddColumn(colData, color);
               EndRow();
            }
         }
      }

      /// <summary>
      /// Creates a table row and adds columns to the row for each public property of the type.
      /// </summary>      
      /// <param name="source">An instance of type T.</param>
      /// <param name="color">The foreground color.</param>
      public void AddRow<T>(T source, ConsoleColor color = ConsoleColor.Gray)
      {
         if (source != null)
         {
            StartRow();
            foreach (var prop in source.GetType().GetProperties())
            {
               var value = prop.GetValue(source, null)?.ToString() ?? string.Empty;
               _row?.Columns.Add(new Column(value, _currentPos, color));
               _currentPos++;
            }
            EndRow();
         }
      }

      /// <summary>
      /// Creates table rows and columns based on the types in the List.
      /// </summary>      
      /// <param name="source">A List type.</param>
      /// <param name="color">The foreground color.</param>
      public void AddRows<T>(List<T> source, ConsoleColor color = ConsoleColor.Gray)
      {
         source.ForEach(r => AddRow(r, color));
      }

      /// <summary>
      /// Creates a table header row for each public property of the type.
      /// </summary>
      /// <param name="source">An instance of type T.</param>
      /// <param name="color">The foreground color.</param>
      /// <exception cref="InvalidOperationException"></exception>
      public void AddHeaderRow<T>(T source, ConsoleColor color = ConsoleColor.Gray)
      {
         if (_isNameValueTable)
            throw new InvalidOperationException("Invalid action: a name/value table does not have any headers.");

         if (source != null)
         {
            StartRow(true);
            foreach (var prop in source.GetType().GetProperties())
            {
               AddColumn(prop.Name, color);
            }
            EndRow();
         }
      }

      /// <summary>
      /// Creates a table header row based on the public properties of the first type in the specified List.
      /// </summary>      
      /// <param name="source">A List type.</param>
      /// <param name="color">The foreground color.</param>
      public void AddHeaderRow<T>(List<T> source, ConsoleColor color = ConsoleColor.Gray)
      {
         AddHeaderRow(source[0], color);
      }

      /// <summary>
      /// Finalizes the current row.
      /// </summary>
      /// <exception cref="InvalidOperationException"></exception>
      public void EndRow()
      {
         if (_row?.Columns.Count != _maxColumns)
            throw new InvalidOperationException($"Invalid row state: the current row has {_row?.Columns.Count} columns, while {_maxColumns} are expected.");

         _rows?.Add(_row);
         _row = null;
         _currentPos = 0;
      }

      /// <summary>
      /// Outputs a name/value row to the console.
      /// </summary>
      /// <param name="name">The name of the value.</param>
      /// <param name="value">The value.</param>
      /// <param name="nameColor">The foreground color of the name.</param>
      /// <param name="valueColor">The foreground color of the value.</param>
      public void WriteNameValueRow(string name, object value, ConsoleColor nameColor = ConsoleColor.Gray, ConsoleColor valueColor = ConsoleColor.Gray)
      {         
         StartRow();
         AddColumn(name, nameColor);
         AddColumn(value?.ToString() ?? string.Empty, valueColor);
         EndRow();
      }

      /// <summary>
      /// Outputs text to the console.
      /// </summary>
      /// <param name="data">The text to output.</param>
      /// <param name="foregroundColor">The foreground color.</param>
      public void WriteSection(string data, ConsoleColor foregroundColor = ConsoleColor.White)
      {
         Output(string.Concat("\r\n", data), foregroundColor, ConsoleColor.Black);
         Output(Environment.NewLine);
         Output(Environment.NewLine);
      }

      /// <summary>
      /// Outputs the table to the console.
      /// </summary>      
      public void WriteTable()
      {
         if (_row != null)
            throw new InvalidOperationException($"Invalid table state: the current row needs to be ended before the table can be created.");

         if (_isNameValueTable) { WriteHorizontalTable(); } else { WriteVerticalTable(); }
      }

      #endregion

      #region Private methods

      /// <summary>
      /// Outputs a table in "name: value" format.
      /// </summary>
      private void WriteHorizontalTable()
      {
         if (_rows != null)
         {
            foreach (var row in _rows)
            {
               Output($"{GetTablePadding()}{row.Columns[0].Value}: ", row.Columns[0].Color);
               Output($"{row.Columns[1].Value}", row.Columns[1].Color);
               Output(Environment.NewLine);
            }
         }
      }

      /// <summary>
      /// Outputs a table in tabular format, including column headers (if any).
      /// </summary>
      private void WriteVerticalTable()
      {
         CreateColumnMetaData();

         if (_rows != null)
         {
            foreach (var row in _rows)
            {
               row.Columns.ForEach(c => Output($"{GetTablePadding()}{c.Value}{GetColumnPadding(c, c.Index)}", c.Color));
               Output(Environment.NewLine);

               if (row.IsHeaderRow) { WriteTableHeader(row); }
            }
         }
      }

      /// <summary>
      /// Underlines each table header column. 
      /// </summary>
      /// <param name="row">The header row.</param>
      private void WriteTableHeader(Row row)
      {
         row.Columns.ForEach(c => Output($"{GetTablePadding()}{GetRowHeaderSeparators(c)}{GetColumnPadding(c, c.Index)}", c.Color));
         Output(Environment.NewLine);
      }

      /// <summary>
      /// Returns a string with spaces to allow a column to align properly in the table output.
      /// </summary>
      /// <param name="col">A column.</param>
      /// <param name="columnIndex">The column index.</param>      
      private string GetColumnPadding(Column col, int columnIndex)
      {
         return new string(' ', GetColumnWidthFromMetaData(columnIndex) - col.Value.Length + COLUMN_PADDING);
      }

      /// <summary>
      /// Returns a string with spaces that is added to the left of the table.
      /// </summary>      
      private string GetTablePadding()
      {
         return _tablePadding > 0 ? new string(' ', _tablePadding) : string.Empty;
      }

      /// <summary>
      /// Returns a string with just the right amount of header separators for the specified column.
      /// </summary>
      /// <param name="col">A column.</param>            
      private static string GetRowHeaderSeparators(Column col)
      {
         return new string(TABLE_HEADER_SEPARATOR, col.Value.Length);
      }

      /// <summary>
      /// Determines the maximum width of each column in the rows collection.
      /// </summary>
      private void CreateColumnMetaData()
      {
         if (_rows == null) return;

         int columnCount = _rows[0].Columns.Count;
         for (int col = 0; col < columnCount; col++)
         {
            int maxColumnWidth = 0;
            foreach (var row in _rows)
            {
               int width = row.Columns.Where(c => c.Index == col).Max(c => c.Value.Length);
               if (width > maxColumnWidth) maxColumnWidth = width;
            }
            _columnMetaData?.Add(new ColumnMetaData(col, maxColumnWidth));
         }
      }

      /// <summary>
      /// Returns the column width.
      /// </summary>
      /// <param name="columnPosition">The table column number.</param>
      /// <returns>The width of the specified table column.</returns>
      private int GetColumnWidthFromMetaData(int columnPosition)
      {
         if (_columnMetaData == null)
            throw new InvalidOperationException("Column metadata is not initialized.");

         return _columnMetaData.Single(c => c.Index == columnPosition).MaxWidth;
      }

      private static void Output(string data)
      {
         Output(data, ConsoleColor.White, ConsoleColor.Black);
      }

      private static void Output(string data, ConsoleColor foregroundColor = ConsoleColor.White)
      {
         Output(data, foregroundColor, ConsoleColor.Black);
      }

      private static void Output(string data, ConsoleColor foregroundColor = ConsoleColor.White, ConsoleColor backgroundColor = ConsoleColor.Black)
      {
         Console.BackgroundColor = backgroundColor;
         Console.ForegroundColor = foregroundColor;
         Console.Out.Write(data);
         Console.ResetColor();
      }

      #endregion

      #region Nested classes

      /// <summary>
      /// Represents a column in a table row.
      /// </summary>
      class Column
      {
         /// <summary>
         /// The column index.
         /// </summary>
         public int Index { get; set; }

         /// <summary>
         /// The value of the column.
         /// </summary>
         public string Value { get; set; }

         /// <summary>
         /// The foreground color of the column value.
         /// </summary>
         public ConsoleColor Color { get; set; }

         /// <summary>
         /// Initializes a new instance of the <see cref="Column"/> class.
         /// </summary>
         /// <param name="value">The value of the column.</param>
         /// <param name="index">The column index.</param>
         public Column(string value, int index)
         {
            Value = value;
            Index = index;
            Color = ConsoleColor.Gray;
         }

         /// <summary>
         /// Initializes a new instance of the <see cref="Column"/> class.
         /// </summary>
         /// <param name="value">The value of the column.</param>
         /// <param name="index">The column index.</param>
         /// <param name="color">The foreground color of the column value.</param>
         public Column(string value, int index, ConsoleColor color)
         {
            Value = value;
            Index = index;
            Color = color;
         }
      }

      /// <summary>
      /// Represents a table row.
      /// </summary>
      class Row
      {
         /// <summary>
         /// A list of columns in the current row.
         /// </summary>
         public List<Column> Columns { get; set; }

         /// <summary>
         /// Determines whether the current row is the header row.
         /// </summary>
         public bool IsHeaderRow { get; set; }

         /// <summary>
         /// Initializes a new instance of the <see cref="Row"> class.
         /// </summary>
         public Row()
         {
            Columns = [];
         }
      }

      /// <summary>
      /// Represents column meta data.
      /// </summary>
      /// <remarks>
      /// Initializes a new instance of the <see cref="ColumnMetaData"/> class.
      /// </remarks>
      /// <param name="index">The column index.</param>
      /// <param name="maxWidth">The maximum width of the column.</param>
      class ColumnMetaData(int index, int maxWidth)
      {
         /// <summary>
         /// The column index.
         /// </summary>
         public int Index { get; set; } = index;

         /// <summary>
         /// The maximum width of the column.
         /// </summary>
         public int MaxWidth { get; set; } = maxWidth;
      }

      #endregion
   }
}
