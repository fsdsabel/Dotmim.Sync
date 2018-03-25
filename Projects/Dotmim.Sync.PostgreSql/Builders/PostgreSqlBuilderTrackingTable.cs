using System;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Data;
using Dotmim.Sync.Filter;
using Npgsql;

namespace Dotmim.Sync.PostgreSql.Builders
{
    public class PostgreSqlBuilderTrackingTable : IDbBuilderTrackingTableHelper
    {
        private readonly NpgsqlConnection _connection;
        private readonly PostgreSqlDbMetadata _mySqlDbMetadata;
        private readonly DmTable _tableDescription;
        private readonly ObjectNameParser _tableName;
        private readonly ObjectNameParser _trackingName;
        private readonly NpgsqlTransaction _transaction;


        public PostgreSqlBuilderTrackingTable(DmTable tableDescription, DbConnection connection,
            DbTransaction transaction = null)
        {
            _connection = connection as NpgsqlConnection;
            _transaction = transaction as NpgsqlTransaction;
            _tableDescription = tableDescription;
            (_tableName, _trackingName) = PostgreSqlBuilder.GetParsers(_tableDescription);
            _mySqlDbMetadata = new PostgreSqlDbMetadata();
        }

        public FilterClauseCollection Filters { get; set; }


        public void CreateIndex()
        {
        }

        public string CreateIndexScriptText()
        {
            //var str = string.Concat("Create index on Tracking Table ", _trackingName.QuotedString);
            return "";
        }

        public void CreatePk()
        {
            var alreadyOpened = _connection.State == ConnectionState.Open;
            try
            {
                using (var command = new NpgsqlCommand())
                {
                    if (!alreadyOpened)
                        _connection.Open();

                    if (_transaction != null)
                        command.Transaction = _transaction;

                    command.CommandText = CreatePkCommandText();
                    command.Connection = _connection;

                    // Sometimes we could have an empty string if pk is created during table creation
                    if (!string.IsNullOrEmpty(command.CommandText))
                        command.ExecuteNonQuery();
                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during CreateIndex : {ex}");
                throw;
            }
            finally
            {
                if (!alreadyOpened && _connection.State != ConnectionState.Closed)
                    _connection.Close();
            }
        }

        public string CreatePkScriptText()
        {
            /*var str = string.Concat(
                "No need to Create Primary Key on Tracking Table since it's done during table creation ",
                _trackingName.QuotedString);*/
            return "";
        }

        public void CreateTable()
        {
            var alreadyOpened = _connection.State == ConnectionState.Open;

            try
            {
                using (var command = new NpgsqlCommand())
                {
                    if (!alreadyOpened)
                        _connection.Open();

                    if (_transaction != null)
                        command.Transaction = _transaction;

                    command.CommandText = CreateTableCommandText();
                    command.Connection = _connection;
                    command.ExecuteNonQuery();
                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during CreateIndex : {ex}");
                throw;
            }
            finally
            {
                if (!alreadyOpened && _connection.State != ConnectionState.Closed)
                    _connection.Close();
            }
        }

        public string CreateTableScriptText()
        {
            var str = string.Concat("Create Tracking Table ", _trackingName.QuotedString);
            return PostgreSqlBuilder.WrapScriptTextWithComments(CreateTableCommandText(), str);
        }

        public bool NeedToCreateTrackingTable()
        {
            return !PostgreSqlManagementUtils.TableExists(_connection, _transaction, _trackingName.UnquotedString);
        }

        public void PopulateFromBaseTable()
        {
            var alreadyOpened = _connection.State == ConnectionState.Open;

            try
            {
                using (var command = new NpgsqlCommand())
                {
                    if (!alreadyOpened)
                        _connection.Open();

                    if (_transaction != null)
                        command.Transaction = _transaction;

                    command.CommandText = CreatePopulateFromBaseTableCommandText();
                    command.Connection = _connection;
                    command.ExecuteNonQuery();
                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during CreateIndex : {ex}");
                throw;
            }
            finally
            {
                if (!alreadyOpened && _connection.State != ConnectionState.Closed)
                    _connection.Close();
            }
        }

        public string CreatePopulateFromBaseTableScriptText()
        {
            var str = string.Concat("Populate tracking table ", _trackingName.QuotedString,
                " for existing data in table ", _tableName.QuotedString);
            return PostgreSqlBuilder.WrapScriptTextWithComments(CreatePopulateFromBaseTableCommandText(), str);
        }

        public void PopulateNewFilterColumnFromBaseTable(DmColumn filterColumn)
        {
            throw new NotImplementedException();
        }

        public string ScriptPopulateNewFilterColumnFromBaseTable(DmColumn filterColumn)
        {
            throw new NotImplementedException();
        }

        public void AddFilterColumn(DmColumn filterColumn)
        {
            var alreadyOpened = _connection.State == ConnectionState.Open;

            try
            {
                using (var command = new NpgsqlCommand())
                {
                    if (!alreadyOpened)
                        _connection.Open();

                    if (_transaction != null)
                        command.Transaction = _transaction;

                    command.CommandText = AddFilterColumnCommandText(filterColumn);
                    command.Connection = _connection;
                    command.ExecuteNonQuery();
                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during CreateIndex : {ex}");
                throw;
            }
            finally
            {
                if (!alreadyOpened && _connection.State != ConnectionState.Closed)
                    _connection.Close();
            }
        }

        public string ScriptAddFilterColumn(DmColumn filterColumn)
        {
            var quotedColumnName = new ObjectNameParser(filterColumn.ColumnName, "\"", "\"");

            var str = string.Concat("Add new filter column, ", quotedColumnName.UnquotedString, ", to Tracking Table ",
                _trackingName.QuotedString);
            return PostgreSqlBuilder.WrapScriptTextWithComments(AddFilterColumnCommandText(filterColumn), str);
        }

        public void DropTable()
        {
            var commandText = $"drop table if exists {_trackingName.QuotedString}";

            var alreadyOpened = _connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    _connection.Open();

                using (var command = new NpgsqlCommand(commandText, _connection))
                {
                    if (_transaction != null)
                        command.Transaction = _transaction;

                    command.ExecuteNonQuery();
                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during DropTableCommand : {ex}");
                throw;
            }
            finally
            {
                if (!alreadyOpened && _connection.State != ConnectionState.Closed)
                    _connection.Close();
            }
        }

        public string DropTableScriptText()
        {
            var commandText = $"drop table if exists {_trackingName.QuotedString}";

            var str1 = $"Drop table {_trackingName.QuotedString}";
            return PostgreSqlBuilder.WrapScriptTextWithComments(commandText, str1);
        }


        public string CreatePkCommandText()
        {
            return "";
        }

        public string CreateTableCommandText()
        {
            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine($"CREATE TABLE {_trackingName.QuotedString} (");

            // Adding the primary key
            foreach (var pkColumn in _tableDescription.PrimaryKey.Columns)
            {
                var quotedColumnName = new ObjectNameParser(pkColumn.ColumnName, "\"", "\"").QuotedString;

                var columnTypeString = _mySqlDbMetadata.TryGetOwnerDbTypeString(pkColumn.OriginalDbType,
                    pkColumn.DbType, false, false, _tableDescription.OriginalProvider,
                    PostgreSqlSyncProvider.ProviderType);
                var unQuotedColumnType = new ObjectNameParser(columnTypeString, "\"", "\"").UnquotedString;
                var columnPrecisionString = _mySqlDbMetadata.TryGetOwnerDbTypePrecision(pkColumn.OriginalDbType,
                    pkColumn.DbType, false, false, pkColumn.MaxLength, pkColumn.Precision, pkColumn.Scale,
                    _tableDescription.OriginalProvider, PostgreSqlSyncProvider.ProviderType);
                var columnType = $"{unQuotedColumnType} {columnPrecisionString}";

                stringBuilder.AppendLine($"{quotedColumnName} {columnType} NOT NULL, ");
            }

            // adding the tracking columns
            stringBuilder.AppendLine($"\"create_scope_id\" UUID NULL, ");
            stringBuilder.AppendLine($"\"update_scope_id\" UUID NULL, ");
            stringBuilder.AppendLine($"\"create_timestamp\" INT8 NULL, ");
            stringBuilder.AppendLine($"\"update_timestamp\" INT8 NULL, ");
            stringBuilder.AppendLine($"\"timestamp\" INT8 NULL, ");
            stringBuilder.AppendLine($"\"sync_row_is_tombstone\" INT2 NOT NULL default 0, ");
            stringBuilder.AppendLine($"\"last_change_datetime\" TIMESTAMP NULL, ");

            if (Filters != null && Filters.Count > 0)
                foreach (var filter in Filters)
                {
                    var columnFilter = _tableDescription.Columns[filter.ColumnName];

                    if (columnFilter == null)
                        throw new InvalidExpressionException(
                            $"Column {filter.ColumnName} does not exist in Table {_tableDescription.TableName.ToLowerInvariant()}");

                    var isPk = _tableDescription.PrimaryKey.Columns.Any(dm =>
                        _tableDescription.IsEqual(dm.ColumnName.ToLowerInvariant(),
                            filter.ColumnName.ToLowerInvariant()));
                    if (isPk)
                        continue;


                    var quotedColumnName = new ObjectNameParser(columnFilter.ColumnName, "\"", "\"").QuotedString;

                    var columnTypeString = _mySqlDbMetadata.TryGetOwnerDbTypeString(columnFilter.OriginalDbType,
                        columnFilter.DbType, false, false, _tableDescription.OriginalProvider,
                        PostgreSqlSyncProvider.ProviderType);
                    var unQuotedColumnType = new ObjectNameParser(columnTypeString, "\"", "\"").UnquotedString;
                    var columnPrecisionString = _mySqlDbMetadata.TryGetOwnerDbTypePrecision(columnFilter.OriginalDbType,
                        columnFilter.DbType, false, false, columnFilter.MaxLength, columnFilter.Precision,
                        columnFilter.Scale, _tableDescription.OriginalProvider, PostgreSqlSyncProvider.ProviderType);
                    var columnType = $"{unQuotedColumnType} {columnPrecisionString}";

                    var nullableColumn = columnFilter.AllowDBNull ? "NULL" : "NOT NULL";

                    stringBuilder.AppendLine($"{quotedColumnName} {columnType} {nullableColumn}, ");
                }

            stringBuilder.Append(" PRIMARY KEY (");
            for (var i = 0; i < _tableDescription.PrimaryKey.Columns.Length; i++)
            {
                var pkColumn = _tableDescription.PrimaryKey.Columns[i];
                var quotedColumnName = new ObjectNameParser(pkColumn.ColumnName, "\"", "\"").QuotedObjectName;

                stringBuilder.Append(quotedColumnName);

                if (i < _tableDescription.PrimaryKey.Columns.Length - 1)
                    stringBuilder.Append(", ");
            }

            stringBuilder.Append("))");

            return stringBuilder.ToString();
        }

        private string CreatePopulateFromBaseTableCommandText()
        {
            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine(string.Concat("INSERT INTO ", _trackingName.QuotedString, " ("));
            var stringBuilder1 = new StringBuilder();
            var stringBuilder2 = new StringBuilder();
            var empty = string.Empty;
            var stringBuilderOnClause = new StringBuilder("ON ");
            var stringBuilderWhereClause = new StringBuilder("WHERE ");
            var str = string.Empty;
            var baseTable = "\"base\"";
            var sideTable = "\"side\"";
            foreach (var pkColumn in _tableDescription.PrimaryKey.Columns)
            {
                var quotedColumnName = new ObjectNameParser(pkColumn.ColumnName, "\"", "\"").QuotedString;

                stringBuilder1.Append(string.Concat(empty, quotedColumnName));

                stringBuilder2.Append(string.Concat(empty, baseTable, ".", quotedColumnName));

                string[] quotedName = {str, baseTable, ".", quotedColumnName, " = ", sideTable, ".", quotedColumnName};
                stringBuilderOnClause.Append(string.Concat(quotedName));
                string[] strArrays = {str, sideTable, ".", quotedColumnName, " IS NULL"};
                stringBuilderWhereClause.Append(string.Concat(strArrays));
                empty = ", ";
                str = " AND ";
            }

            var stringBuilder5 = new StringBuilder();
            var stringBuilder6 = new StringBuilder();

            if (Filters != null)
                foreach (var filterColumn in Filters)
                {
                    var isPk = _tableDescription.PrimaryKey.Columns.Any(dm =>
                        _tableDescription.IsEqual(dm.ColumnName.ToLowerInvariant(),
                            filterColumn.ColumnName.ToLowerInvariant()));
                    if (isPk)
                        continue;

                    var quotedColumnName = new ObjectNameParser(filterColumn.ColumnName, "\"", "\"").QuotedString;

                    stringBuilder6.Append(string.Concat(empty, quotedColumnName));
                    stringBuilder5.Append(string.Concat(empty, baseTable, ".", quotedColumnName));
                }

            // (list of pkeys)
            stringBuilder.Append(string.Concat(stringBuilder1.ToString(), ", "));

            stringBuilder.Append("\"create_scope_id\", ");
            stringBuilder.Append("\"update_scope_id\", ");
            stringBuilder.Append("\"create_timestamp\", ");
            stringBuilder.Append("\"update_timestamp\", ");
            stringBuilder.Append("\"timestamp\", "); // timestamp is not a column we update, it's auto
            stringBuilder.Append("\"sync_row_is_tombstone\" ");
            stringBuilder.AppendLine(string.Concat(stringBuilder6.ToString(), ") "));
            stringBuilder.Append(string.Concat("SELECT ", stringBuilder2.ToString(), ", "));
            stringBuilder.Append("NULL, ");
            stringBuilder.Append("NULL, ");
            stringBuilder.Append($"{PostgreSqlObjectNames.TimestampValue}, ");
            stringBuilder.Append("0, ");
            stringBuilder.Append($"{PostgreSqlObjectNames.TimestampValue}, ");
            stringBuilder.Append("0");
            stringBuilder.AppendLine(string.Concat(stringBuilder5.ToString(), " "));
            string[] localName =
            {
                "FROM ", _tableName.QuotedString, " ", baseTable, " LEFT OUTER JOIN ", _trackingName.QuotedString, " ",
                sideTable, " "
            };
            stringBuilder.AppendLine(string.Concat(localName));
            stringBuilder.AppendLine(string.Concat(stringBuilderOnClause.ToString(), " "));
            stringBuilder.AppendLine(string.Concat(stringBuilderWhereClause.ToString(), "; \n"));
            return stringBuilder.ToString();
        }

        private string AddFilterColumnCommandText(DmColumn col)
        {
            var quotedColumnName = new ObjectNameParser(col.ColumnName, "\"", "\"").QuotedString;

            var columnTypeString = _mySqlDbMetadata.TryGetOwnerDbTypeString(col.OriginalDbType, col.DbType, false,
                false, _tableDescription.OriginalProvider, PostgreSqlSyncProvider.ProviderType);
            var columnPrecisionString = _mySqlDbMetadata.TryGetOwnerDbTypePrecision(col.OriginalDbType, col.DbType,
                false, false, col.MaxLength, col.Precision, col.Scale, _tableDescription.OriginalProvider,
                PostgreSqlSyncProvider.ProviderType);
            var columnType = $"{columnTypeString} {columnPrecisionString}";

            return string.Concat("ALTER TABLE ", quotedColumnName, " ADD ", columnType);
        }
    }
}