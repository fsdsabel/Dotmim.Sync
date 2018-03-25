using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Data;
using Dotmim.Sync.Filter;
using Npgsql;
using NpgsqlTypes;

namespace Dotmim.Sync.PostgreSql.Builders
{
    public class PostgreSqlBuilderProcedure : IDbBuilderProcedureHelper
    {
        internal const string PgsqlPrefixParameter = "in_";
        private readonly NpgsqlConnection _connection;
        private readonly PostgreSqlDbMetadata _mySqlDbMetadata;
        private readonly string _schemaName;
        private readonly PostgreSqlObjectNames _sqlObjectNames;
        private readonly DmTable _tableDescription;
        private readonly ObjectNameParser _tableName;
        private readonly ObjectNameParser _trackingName;
        private readonly NpgsqlTransaction _transaction;

        public PostgreSqlBuilderProcedure(DmTable tableDescription, DbConnection connection,
            DbTransaction transaction = null)
        {
            _connection = connection as NpgsqlConnection;
            _transaction = transaction as NpgsqlTransaction;

            _tableDescription = tableDescription;
            (_tableName, _trackingName) = PostgreSqlBuilder.GetParsers(tableDescription);
            _sqlObjectNames = new PostgreSqlObjectNames(_tableDescription);
            _mySqlDbMetadata = new PostgreSqlDbMetadata();

            _schemaName = new NpgsqlConnectionStringBuilder(connection.ConnectionString).SearchPath ?? "public";
        }

        public FilterClauseCollection Filters { get; set; }

        /// <summary>
        ///     Check if we need to create the stored procedure
        /// </summary>
        public bool NeedToCreateProcedure(DbCommandType commandType)
        {
            if (_connection.State != ConnectionState.Open)
                throw new ArgumentException("Here, we need an opened connection please");

            var commandName = _sqlObjectNames.GetCommandName(commandType);

            return !PostgreSqlManagementUtils.ProcedureExists(_connection, _transaction, commandName);
        }

        /// <summary>
        ///     Check if we need to create the TVP Type
        /// </summary>
        public bool NeedToCreateType(DbCommandType commandType)
        {
            return false;
        }

        public void CreateReset()
        {
            var commandName = _sqlObjectNames.GetCommandName(DbCommandType.Reset);
            CreateProcedureCommand(BuildResetCommand, commandName);
        }

        public string CreateResetScriptText()
        {
            var commandName = _sqlObjectNames.GetCommandName(DbCommandType.Reset);
            return CreateProcedureCommandScriptText(BuildResetCommand, commandName);
        }

        public void CreateDelete()
        {
            var commandName = _sqlObjectNames.GetCommandName(DbCommandType.DeleteRow);
            CreateProcedureCommand(BuildDeleteCommand, commandName);
        }

        public string CreateDeleteScriptText()
        {
            var commandName = _sqlObjectNames.GetCommandName(DbCommandType.DeleteRow);
            return CreateProcedureCommandScriptText(BuildDeleteCommand, commandName);
        }

        public void CreateDeleteMetadata()
        {
            var commandName = _sqlObjectNames.GetCommandName(DbCommandType.DeleteMetadata);
            CreateProcedureCommand(BuildDeleteMetadataCommand, commandName);
        }

        public string CreateDeleteMetadataScriptText()
        {
            var commandName = _sqlObjectNames.GetCommandName(DbCommandType.DeleteMetadata);
            return CreateProcedureCommandScriptText(BuildDeleteMetadataCommand, commandName);
        }

        public void CreateInsert()
        {
            var commandName = _sqlObjectNames.GetCommandName(DbCommandType.InsertRow);
            CreateProcedureCommand(BuildInsertCommand, commandName);
        }

        public string CreateInsertScriptText()
        {
            var commandName = _sqlObjectNames.GetCommandName(DbCommandType.InsertRow);
            return CreateProcedureCommandScriptText(BuildInsertCommand, commandName);
        }

        public void CreateInsertMetadata()
        {
            var commandName = _sqlObjectNames.GetCommandName(DbCommandType.InsertMetadata);
            CreateProcedureCommand(BuildInsertMetadataCommand, commandName);
        }

        public string CreateInsertMetadataScriptText()
        {
            var commandName = _sqlObjectNames.GetCommandName(DbCommandType.InsertMetadata);
            return CreateProcedureCommandScriptText(BuildInsertMetadataCommand, commandName);
        }

        public void CreateSelectRow()
        {
            var commandName = _sqlObjectNames.GetCommandName(DbCommandType.SelectRow);
            CreateProcedureCommand(BuildSelectRowCommand, commandName);
        }

        public string CreateSelectRowScriptText()
        {
            var commandName = _sqlObjectNames.GetCommandName(DbCommandType.SelectRow);
            return CreateProcedureCommandScriptText(BuildSelectRowCommand, commandName);
        }

        public void CreateUpdate()
        {
            var commandName = _sqlObjectNames.GetCommandName(DbCommandType.UpdateRow);
            CreateProcedureCommand(BuildUpdateCommand, commandName);
        }

        public string CreateUpdateScriptText()
        {
            var commandName = _sqlObjectNames.GetCommandName(DbCommandType.UpdateRow);
            return CreateProcedureCommandScriptText(BuildUpdateCommand, commandName);
        }

        public void CreateUpdateMetadata()
        {
            var commandName = _sqlObjectNames.GetCommandName(DbCommandType.UpdateMetadata);
            CreateProcedureCommand(BuildUpdateMetadataCommand, commandName);
        }

        public string CreateUpdateMetadataScriptText()
        {
            var commandName = _sqlObjectNames.GetCommandName(DbCommandType.UpdateMetadata);
            return CreateProcedureCommandScriptText(BuildUpdateMetadataCommand, commandName);
        }

        public void CreateSelectIncrementalChanges()
        {
            var commandName = _sqlObjectNames.GetCommandName(DbCommandType.SelectChanges);
            Func<Tuple<NpgsqlCommand, string>> cmdWithoutFilter = () => BuildSelectIncrementalChangesCommand();
            CreateProcedureCommand(cmdWithoutFilter, commandName);

            if (Filters != null && Filters.Count > 0)
            {
                foreach (var c in Filters)
                {
                    var columnFilter = _tableDescription.Columns[c.ColumnName];

                    if (columnFilter == null)
                        throw new InvalidExpressionException(
                            $"Column {c.ColumnName} does not exist in Table {_tableDescription.TableName}");
                }

                var filtersName = Filters.Select(f => f.ColumnName);
                commandName = _sqlObjectNames.GetCommandName(DbCommandType.SelectChangesWitFilters, filtersName);
                Func<Tuple<NpgsqlCommand, string>> cmdWithFilter = () => BuildSelectIncrementalChangesCommand(true);
                CreateProcedureCommand(cmdWithFilter, commandName);
            }
        }

        public string CreateSelectIncrementalChangesScriptText()
        {
            var sbSelecteChanges = new StringBuilder();

            var commandName = _sqlObjectNames.GetCommandName(DbCommandType.SelectChanges);
            Func<Tuple<NpgsqlCommand, string>> cmdWithoutFilter = () => BuildSelectIncrementalChangesCommand();
            sbSelecteChanges.AppendLine(CreateProcedureCommandScriptText(cmdWithoutFilter, commandName));


            if (Filters != null && Filters.Count > 0)
            {
                commandName = _sqlObjectNames.GetCommandName(DbCommandType.SelectChangesWitFilters);
                var name = "";
                var sep = "";
                foreach (var c in Filters)
                {
                    var columnFilter = _tableDescription.Columns[c.ColumnName];

                    if (columnFilter == null)
                        throw new InvalidExpressionException(
                            $"Column {c.ColumnName} does not exist in Table {_tableDescription.TableName}");

                    var unquotedColumnName = new ObjectNameParser(columnFilter.ColumnName, "\"", "\"").UnquotedString;
                    name += $"{unquotedColumnName}{sep}";
                    sep = "_";
                }

                commandName = string.Format(commandName, name);
                Func<Tuple<NpgsqlCommand, string>> cmdWithFilter = () => BuildSelectIncrementalChangesCommand(true);
                sbSelecteChanges.AppendLine(CreateProcedureCommandScriptText(cmdWithFilter, commandName));
            }

            return sbSelecteChanges.ToString();
        }

        public void CreateTVPType()
        {
            throw new NotImplementedException();
        }

        public void CreateBulkInsert()
        {
            throw new NotImplementedException();
        }

        public void CreateBulkUpdate()
        {
            throw new NotImplementedException();
        }

        public void CreateBulkDelete()
        {
            throw new NotImplementedException();
        }

        public string CreateTVPTypeScriptText()
        {
            throw new NotImplementedException();
        }

        public string CreateBulkInsertScriptText()
        {
            throw new NotImplementedException();
        }

        public string CreateBulkUpdateScriptText()
        {
            throw new NotImplementedException();
        }

        public string CreateBulkDeleteScriptText()
        {
            throw new NotImplementedException();
        }

        public void DropSelectRow()
        {
            DropProcedure(DbCommandType.SelectRow);
        }

        public void DropSelectIncrementalChanges()
        {
            DropProcedure(DbCommandType.SelectChanges);

            // filtered 
            if (Filters != null && Filters.Count > 0)
            {
                var alreadyOpened = _connection.State == ConnectionState.Open;

                using (var command = new NpgsqlCommand())
                {
                    if (!alreadyOpened)
                        _connection.Open();

                    if (_transaction != null)
                        command.Transaction = _transaction;

                    foreach (var c in Filters)
                    {
                        var columnFilter = _tableDescription.Columns[c.ColumnName];

                        if (columnFilter == null)
                            throw new InvalidExpressionException(
                                $"Column {c.ColumnName} does not exist in Table {_tableDescription.TableName}");
                    }

                    var filtersName = Filters.Select(f => f.ColumnName);
                    var commandNameWithFilter =
                        _sqlObjectNames.GetCommandName(DbCommandType.SelectChangesWitFilters, filtersName);

                    command.CommandText = $"DROP PROCEDURE IF EXISTS {commandNameWithFilter};";
                    command.Connection = _connection;
                    command.ExecuteNonQuery();
                }

                if (!alreadyOpened && _connection.State != ConnectionState.Closed)
                    _connection.Close();
            }
        }

        public void DropInsert()
        {
            DropProcedure(DbCommandType.InsertRow);
        }

        public void DropUpdate()
        {
            DropProcedure(DbCommandType.UpdateRow);
        }

        public void DropDelete()
        {
            DropProcedure(DbCommandType.DeleteRow);
        }

        public void DropInsertMetadata()
        {
            DropProcedure(DbCommandType.InsertMetadata);
        }

        public void DropUpdateMetadata()
        {
            DropProcedure(DbCommandType.UpdateMetadata);
        }

        public void DropDeleteMetadata()
        {
            DropProcedure(DbCommandType.DeleteMetadata);
        }

        public void DropTVPType()
        {
            throw new NotImplementedException();
        }

        public void DropBulkInsert()
        {
            throw new NotImplementedException();
        }

        public void DropBulkUpdate()
        {
            throw new NotImplementedException();
        }

        public void DropBulkDelete()
        {
            throw new NotImplementedException();
        }

        public void DropReset()
        {
            DropProcedure(DbCommandType.Reset);
        }

        public string DropSelectRowScriptText()
        {
            return DropProcedureText(DbCommandType.SelectRow);
        }

        public string DropSelectIncrementalChangesScriptText()
        {
            return DropProcedureText(DbCommandType.SelectChanges);
        }

        public string DropInsertScriptText()
        {
            return DropProcedureText(DbCommandType.InsertRow);
        }

        public string DropUpdateScriptText()
        {
            return DropProcedureText(DbCommandType.UpdateRow);
        }

        public string DropDeleteScriptText()
        {
            return DropProcedureText(DbCommandType.DeleteRow);
        }

        public string DropInsertMetadataScriptText()
        {
            return DropProcedureText(DbCommandType.InsertMetadata);
        }

        public string DropUpdateMetadataScriptText()
        {
            return DropProcedureText(DbCommandType.UpdateMetadata);
        }

        public string DropDeleteMetadataScriptText()
        {
            return DropProcedureText(DbCommandType.DeleteMetadata);
        }

        public string DropTVPTypeScriptText()
        {
            throw new NotImplementedException();
        }

        public string DropBulkInsertScriptText()
        {
            throw new NotImplementedException();
        }

        public string DropBulkUpdateScriptText()
        {
            throw new NotImplementedException();
        }

        public string DropBulkDeleteScriptText()
        {
            throw new NotImplementedException();
        }

        public string DropResetScriptText()
        {
            return DropProcedureText(DbCommandType.Reset);
        }

        private void AddPkColumnParametersToCommand(NpgsqlCommand sqlCommand)
        {
            foreach (var pkColumn in _tableDescription.PrimaryKey.Columns)
                sqlCommand.Parameters.Add(pkColumn.GetPostgreSqlParameter());
        }

        private void AddColumnParametersToCommand(NpgsqlCommand sqlCommand)
        {
            foreach (var column in _tableDescription.Columns.Where(c => !c.ReadOnly))
                sqlCommand.Parameters.Add(column.GetPostgreSqlParameter());
        }

        /// <summary>
        ///     From a SqlParameter, create the declaration
        /// </summary>
        internal string CreateParameterDeclaration(NpgsqlParameter param)
        {
            var stringBuilder3 = new StringBuilder();
            
            var stringType = _mySqlDbMetadata.GetStringFromDbType(param.DbType);

            var precision =
                _mySqlDbMetadata.GetPrecisionStringFromDbType(param.DbType, param.Size, param.Precision, param.Scale);


            stringBuilder3.Append($"{param.ParameterName} {stringType}{precision}");

            return stringBuilder3.ToString();
        }

        /// <summary>
        ///     From a SqlCommand, create a stored procedure string
        /// </summary>
        private string CreateProcedureCommandText(NpgsqlCommand cmd, string returnTableDefinition, string procName)
        {
            var stringBuilder = new StringBuilder();
            stringBuilder.Append("CREATE FUNCTION ");
            stringBuilder.Append(procName);
            stringBuilder.Append(" (");
            stringBuilder.AppendLine();
            var str = "\n\t";
            foreach (NpgsqlParameter parameter in cmd.Parameters)
            {
                stringBuilder.Append(string.Concat(str, CreateParameterDeclaration(parameter)));
                str = ",\n\t";
            }

            stringBuilder.Append("\n)");
            stringBuilder.AppendLine($"RETURNS {returnTableDefinition}");

            stringBuilder.Append($"LANGUAGE 'plpgsql'\nCOST 100\nSET search_path='{_schemaName}' AS $BODY$");
            stringBuilder.AppendLine("DECLARE rows INT;ts BIGINT;");
            stringBuilder.Append("\nBEGIN\n");

            stringBuilder.Append(cmd.CommandText);
            stringBuilder.Append("\nEND;");
            stringBuilder.Append("$BODY$;");
            return stringBuilder.ToString();
        }

        /// <summary>
        ///     Create a stored procedure
        /// </summary>
        public void CreateProcedureCommand(Func<Tuple<NpgsqlCommand, string>> buildCommand, string procName)
        {
            var alreadyOpened = _connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    _connection.Open();

                var cmd = buildCommand();
                var str = CreateProcedureCommandText(cmd.Item1, cmd.Item2, procName);
                using (var command = new NpgsqlCommand(str, _connection))
                {
                    if (_transaction != null)
                        command.Transaction = _transaction;

                    command.ExecuteNonQuery();
                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during CreateProcedureCommand : {ex}");
                throw;
            }
            finally
            {
                if (!alreadyOpened && _connection.State != ConnectionState.Closed)
                    _connection.Close();
            }
        }

        private string CreateProcedureCommandScriptText(Func<Tuple<NpgsqlCommand, string>> buildCommand,
            string procName)
        {
            var alreadyOpened = _connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    _connection.Open();

                var str1 = $"Command {procName} for table {_tableName.QuotedString}";
                var cmd = buildCommand();
                var str = CreateProcedureCommandText(cmd.Item1, cmd.Item2, procName);
                return PostgreSqlBuilder.WrapScriptTextWithComments(str, str1);
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during CreateProcedureCommand : {ex}");
                throw;
            }
            finally
            {
                if (!alreadyOpened && _connection.State != ConnectionState.Closed)
                    _connection.Close();
            }
        }

        //------------------------------------------------------------------
        // Reset command
        //------------------------------------------------------------------
        private Tuple<NpgsqlCommand, string> BuildResetCommand()
        {
            var updTriggerName = _sqlObjectNames.GetCommandName(DbCommandType.UpdateTrigger);
            var delTriggerName = _sqlObjectNames.GetCommandName(DbCommandType.DeleteTrigger);
            var insTriggerName = _sqlObjectNames.GetCommandName(DbCommandType.InsertTrigger);

            var sqlCommand = new NpgsqlCommand();

            var stringBuilder = new StringBuilder();

            stringBuilder.AppendLine($"DELETE FROM {_tableName.QuotedString};");
            stringBuilder.AppendLine($"DELETE FROM {_trackingName.QuotedString};");

            stringBuilder.AppendLine();
            sqlCommand.CommandText = stringBuilder.ToString();
            return Tuple.Create(sqlCommand, "void");
        }

        //------------------------------------------------------------------
        // Delete command
        //------------------------------------------------------------------
        private Tuple<NpgsqlCommand, string> BuildDeleteCommand()
        {
            var sqlCommand = new NpgsqlCommand();
            AddPkColumnParametersToCommand(sqlCommand);
            var sqlParameter = new NpgsqlParameter();
            sqlParameter.ParameterName = "sync_force_write";
            sqlParameter.NpgsqlDbType = NpgsqlDbType.Integer;
            sqlCommand.Parameters.Add(sqlParameter);

            var sqlParameter1 = new NpgsqlParameter();
            sqlParameter1.ParameterName = "sync_min_timestamp";
            sqlParameter1.NpgsqlDbType = NpgsqlDbType.Bigint;
            sqlCommand.Parameters.Add(sqlParameter1);

            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("ts := 0;");
            stringBuilder.AppendLine(
                $"SELECT \"timestamp\" FROM {_trackingName.QuotedObjectName} WHERE {PostgreSqlManagementUtils.WhereColumnAndParameters(_tableDescription.PrimaryKey.Columns, _trackingName.QuotedObjectName)} LIMIT 1 INTO ts;");
            stringBuilder.AppendLine($"DELETE FROM {_tableName.QuotedString} WHERE");
            stringBuilder.AppendLine(
                PostgreSqlManagementUtils.WhereColumnAndParameters(_tableDescription.PrimaryKey.Columns, ""));
            stringBuilder.AppendLine("AND (ts <= sync_min_timestamp  OR sync_force_write = 1);");
            stringBuilder.AppendLine("GET DIAGNOSTICS rows = ROW_COUNT;");
            stringBuilder.AppendLine("RETURN rows;");
            sqlCommand.CommandText = stringBuilder.ToString();
            return Tuple.Create(sqlCommand, "integer");
        }


        //------------------------------------------------------------------
        // Delete Metadata command
        //------------------------------------------------------------------
        private Tuple<NpgsqlCommand, string> BuildDeleteMetadataCommand()
        {
            var sqlCommand = new NpgsqlCommand();
            AddPkColumnParametersToCommand(sqlCommand);
            var sqlParameter = new NpgsqlParameter();
            sqlParameter.ParameterName = "sync_check_concurrency";
            sqlParameter.NpgsqlDbType = NpgsqlDbType.Integer;
            sqlCommand.Parameters.Add(sqlParameter);

            var sqlParameter1 = new NpgsqlParameter();
            sqlParameter1.ParameterName = "sync_row_timestamp";
            sqlParameter1.NpgsqlDbType = NpgsqlDbType.Bigint;
            sqlCommand.Parameters.Add(sqlParameter1);

            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"DELETE FROM {_trackingName.QuotedString} ");
            stringBuilder.Append($"WHERE ");
            stringBuilder.AppendLine(
                PostgreSqlManagementUtils.ColumnsAndParameters(_tableDescription.PrimaryKey.Columns, ""));
            stringBuilder.Append(";");
            sqlCommand.CommandText = stringBuilder.ToString();
            return Tuple.Create(sqlCommand, "void");
        }


        //------------------------------------------------------------------
        // Insert command
        //------------------------------------------------------------------
        private Tuple<NpgsqlCommand, string> BuildInsertCommand()
        {
            var sqlCommand = new NpgsqlCommand();
            var stringBuilder = new StringBuilder();
            var stringBuilderArguments = new StringBuilder();
            var stringBuilderParameters = new StringBuilder();

            AddColumnParametersToCommand(sqlCommand);


            stringBuilder.Append(string.Concat("IF ((SELECT COUNT(*) FROM ", _trackingName.QuotedString, " WHERE "));
            stringBuilder.Append(
                PostgreSqlManagementUtils.ColumnsAndParameters(_tableDescription.PrimaryKey.Columns, string.Empty));
            stringBuilder.AppendLine(") <= 0) THEN");

            var empty = string.Empty;
            foreach (var mutableColumn in _tableDescription.Columns.Where(c => !c.ReadOnly))
            {
                var columnName = new ObjectNameParser(mutableColumn.ColumnName, "\"", "\"");
                stringBuilderArguments.Append(string.Concat(empty, columnName.QuotedString));
                stringBuilderParameters.Append(string.Concat(empty,
                    $"{PgsqlPrefixParameter}{columnName.UnquotedString}"));
                empty = ", ";
            }


            stringBuilder.AppendLine($"\tINSERT INTO {_tableName.QuotedString}");
            stringBuilder.AppendLine($"\t({stringBuilderArguments})");
            stringBuilder.AppendLine($"\tVALUES ({stringBuilderParameters});");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("GET DIAGNOSTICS rows = ROW_COUNT;");
            stringBuilder.AppendLine("RETURN rows;");
            stringBuilder.AppendLine("END IF;");
            stringBuilder.AppendLine("RETURN 0;");
            sqlCommand.CommandText = stringBuilder.ToString();
            return Tuple.Create(sqlCommand, "INT");
        }

        //------------------------------------------------------------------
        // Insert Metadata command
        //------------------------------------------------------------------
        private Tuple<NpgsqlCommand, string> BuildInsertMetadataCommand()
        {
            var stringBuilderArguments = new StringBuilder();
            var stringBuilderParameters = new StringBuilder();
            var sqlCommand = new NpgsqlCommand();

            var stringBuilder = new StringBuilder();
            AddPkColumnParametersToCommand(sqlCommand);
            var sqlParameter = new NpgsqlParameter();
            sqlParameter.ParameterName = "sync_scope_id";
            sqlParameter.NpgsqlDbType = NpgsqlDbType.Uuid;
            sqlCommand.Parameters.Add(sqlParameter);

            var sqlParameter1 = new NpgsqlParameter();
            sqlParameter1.ParameterName = "sync_row_is_tombstone";
            sqlParameter1.NpgsqlDbType = NpgsqlDbType.Integer;
            sqlCommand.Parameters.Add(sqlParameter1);

            var sqlParameter3 = new NpgsqlParameter();
            sqlParameter3.ParameterName = "create_timestamp";
            sqlParameter3.NpgsqlDbType = NpgsqlDbType.Bigint;

            sqlCommand.Parameters.Add(sqlParameter3);
            var sqlParameter4 = new NpgsqlParameter();
            sqlParameter4.ParameterName = "update_timestamp";
            sqlParameter4.NpgsqlDbType = NpgsqlDbType.Bigint;
            sqlCommand.Parameters.Add(sqlParameter4);

            stringBuilder.AppendLine($"\tINSERT INTO {_trackingName.QuotedString}");

            var empty = string.Empty;
            var pkColumns = new List<string>();
            foreach (var pkColumn in _tableDescription.PrimaryKey.Columns)
            {
                var columnName = new ObjectNameParser(pkColumn.ColumnName, "\"", "\"");
                stringBuilderArguments.Append(string.Concat(empty, columnName.QuotedString));
                stringBuilderParameters.Append(string.Concat(empty,
                    $"{PgsqlPrefixParameter}{columnName.UnquotedString}"));
                pkColumns.Add(columnName.QuotedString);
                empty = ", ";
            }

            stringBuilder.Append($"\t({stringBuilderArguments}, ");
            stringBuilder.AppendLine(
                $"\"create_scope_id\", \"create_timestamp\", \"update_scope_id\", \"update_timestamp\",");
            stringBuilder.AppendLine($"\t\"sync_row_is_tombstone\", \"timestamp\", \"last_change_datetime\")");
            stringBuilder.Append($"\tVALUES ({stringBuilderParameters}, ");
            stringBuilder.AppendLine($"\tsync_scope_id, create_timestamp, sync_scope_id, update_timestamp, ");
            stringBuilder.AppendLine($"\tsync_row_is_tombstone, {PostgreSqlObjectNames.TimestampValue}, now())");
            stringBuilder.AppendLine($"\tON CONFLICT ({string.Join(",", pkColumns)}) DO UPDATE SET");
            stringBuilder.AppendLine($"\t \"create_scope_id\" = sync_scope_id, ");
            stringBuilder.AppendLine($"\t \"create_timestamp\" = $4, ");
            stringBuilder.AppendLine($"\t \"update_scope_id\" = sync_scope_id, ");
            stringBuilder.AppendLine($"\t \"update_timestamp\" = $5, ");
            stringBuilder.AppendLine($"\t \"sync_row_is_tombstone\" = $3, ");
            stringBuilder.AppendLine($"\t \"timestamp\" = {PostgreSqlObjectNames.TimestampValue}, ");
            stringBuilder.AppendLine($"\t \"last_change_datetime\" = now(); ");

            stringBuilder.AppendLine("GET DIAGNOSTICS rows = ROW_COUNT;");
            stringBuilder.AppendLine("RETURN rows;");

            sqlCommand.CommandText = stringBuilder.ToString();
            return Tuple.Create(sqlCommand, "integer");
        }


        //------------------------------------------------------------------
        // Select Row command
        //------------------------------------------------------------------
        private Tuple<NpgsqlCommand, string> BuildSelectRowCommand()
        {
            var sqlCommand = new NpgsqlCommand();
            AddPkColumnParametersToCommand(sqlCommand);

            var sqlParameter = new NpgsqlParameter();
            sqlParameter.ParameterName = "sync_scope_id";
            sqlParameter.NpgsqlDbType = NpgsqlDbType.Uuid;
            sqlCommand.Parameters.Add(sqlParameter);

            var stringBuilder = new StringBuilder("RETURN QUERY SELECT ");
            var returnColums = new List<string>();
            stringBuilder.AppendLine();
            var stringBuilder1 = new StringBuilder();
            var empty = string.Empty;
            foreach (var pkColumn in _tableDescription.PrimaryKey.Columns)
            {
                var pkColumnName = new ObjectNameParser(pkColumn.ColumnName, "\"", "\"");
                stringBuilder.AppendLine($"\t\"side\".{pkColumnName.QuotedString}, ");
                stringBuilder1.Append(
                    $"{empty}\"side\".{pkColumnName.QuotedString} = {PgsqlPrefixParameter}{pkColumnName.UnquotedString}");
                returnColums.Add($"{pkColumnName} {pkColumn.OriginalTypeName}");
                empty = " AND ";
            }

            foreach (var nonPkMutableColumn in _tableDescription.NonPkColumns.Where(c => !c.ReadOnly))
            {
                var nonPkColumnName = new ObjectNameParser(nonPkMutableColumn.ColumnName, "\"", "\"");
                stringBuilder.AppendLine($"\t\"base\".{nonPkColumnName.QuotedString}, ");
                returnColums.Add($"{nonPkColumnName} {nonPkMutableColumn.OriginalTypeName}");
            }

            stringBuilder.AppendLine("\t\"side\".\"sync_row_is_tombstone\",");
            returnColums.Add("sync_row_is_tombstone SMALLINT");
            stringBuilder.AppendLine("\t\"side\".\"create_scope_id\",");
            returnColums.Add("create_scope_id UUID");
            stringBuilder.AppendLine("\t\"side\".\"create_timestamp\",");
            returnColums.Add("create_timestamp BIGINT");
            stringBuilder.AppendLine("\t\"side\".\"update_scope_id\",");
            returnColums.Add("update_scope_id UUID");
            stringBuilder.AppendLine("\t\"side\".\"update_timestamp\"");
            returnColums.Add("update_timestamp BIGINT");


            stringBuilder.AppendLine($"FROM {_tableName.QuotedString} \"base\"");
            stringBuilder.AppendLine($"RIGHT JOIN {_trackingName.QuotedString} \"side\" ON");

            var str = string.Empty;
            foreach (var pkColumn in _tableDescription.PrimaryKey.Columns)
            {
                var pkColumnName = new ObjectNameParser(pkColumn.ColumnName, "\"", "\"");
                stringBuilder.Append(
                    $"{str}\"base\".{pkColumnName.QuotedString} = \"side\".{pkColumnName.QuotedString}");
                str = " AND ";
            }

            stringBuilder.AppendLine();
            stringBuilder.Append("WHERE ");
            stringBuilder.Append(stringBuilder1);
            stringBuilder.Append(";");
            sqlCommand.CommandText = stringBuilder.ToString();
            return Tuple.Create(sqlCommand, $"TABLE ({string.Join(",", returnColums)})");
        }


        //------------------------------------------------------------------
        // Update command
        //------------------------------------------------------------------
        private Tuple<NpgsqlCommand, string> BuildUpdateCommand()
        {
            var sqlCommand = new NpgsqlCommand();

            var stringBuilder = new StringBuilder();
            AddColumnParametersToCommand(sqlCommand);

            var sqlParameter = new NpgsqlParameter();
            sqlParameter.ParameterName = "sync_force_write";
            sqlParameter.NpgsqlDbType = NpgsqlDbType.Integer;
            sqlCommand.Parameters.Add(sqlParameter);

            var sqlParameter1 = new NpgsqlParameter();
            sqlParameter1.ParameterName = "sync_min_timestamp";
            sqlParameter1.NpgsqlDbType = NpgsqlDbType.Bigint;
            sqlCommand.Parameters.Add(sqlParameter1);

            var whereArgs = new List<string>();
            foreach (var pkColumn in _tableDescription.PrimaryKey.Columns)
            {
                var pk = new ObjectNameParser(pkColumn.ColumnName, "\"", "\"");
                whereArgs.Add(
                    $"{pk.QuotedObjectName} = {PgsqlPrefixParameter}{pk.UnquotedString.ToLowerInvariant()}");
            }

            stringBuilder.AppendLine("ts := 0;");
            stringBuilder.AppendLine(
                $"SELECT \"timestamp\" FROM {_trackingName.QuotedObjectName} WHERE {string.Join(" AND ", whereArgs.Select(a => $"{_trackingName.QuotedObjectName}.{a}"))} LIMIT 1 INTO ts;");

            stringBuilder.AppendLine($"UPDATE {_tableName.QuotedString}");
            stringBuilder.Append(
                $"SET {PostgreSqlManagementUtils.CommaSeparatedUpdateFromParameters(_tableDescription)}");
            stringBuilder.Append($"WHERE {string.Join(" AND ", whereArgs)}");
            stringBuilder.AppendLine($" AND (ts <= sync_min_timestamp OR sync_force_write = 1);");

            stringBuilder.AppendLine("GET DIAGNOSTICS rows = ROW_COUNT;");
            stringBuilder.AppendLine("RETURN rows;");
            stringBuilder.AppendLine();
            // Can't rely on rows count since MySql will return 0 if an update don't update any columns

            //stringBuilder.AppendLine($"/* Since the update 'could' potentially returns 0 as row affected count when we make a double update with the same values, to be sure, make a fake update on metadatas time column */

            //stringBuilder.AppendLine($"UPDATE {trackingName.QuotedObjectName} ");
            //stringBuilder.AppendLine($"SET \"timestamp\" = {MySqlObjectNames.TimestampValue}");
            //stringBuilder.AppendLine($"WHERE {MySqlManagementUtils.WhereColumnAndParameters(this.tableDescription.PrimaryKey.Columns, "")} AND (ts <= sync_min_timestamp OR sync_force_write = 1);");


            sqlCommand.CommandText = stringBuilder.ToString();
            return Tuple.Create(sqlCommand, "integer");
        }

        //------------------------------------------------------------------
        // Update Metadata command
        //------------------------------------------------------------------
        private Tuple<NpgsqlCommand, string> BuildUpdateMetadataCommand()
        {
            var sqlCommand = new NpgsqlCommand();
            var stringBuilder = new StringBuilder();
            AddPkColumnParametersToCommand(sqlCommand);
            var sqlParameter = new NpgsqlParameter();
            sqlParameter.ParameterName = "sync_scope_id";
            sqlParameter.NpgsqlDbType = NpgsqlDbType.Uuid;
            sqlCommand.Parameters.Add(sqlParameter);

            var sqlParameter1 = new NpgsqlParameter();
            sqlParameter1.ParameterName = "sync_row_is_tombstone";
            sqlParameter1.NpgsqlDbType = NpgsqlDbType.Integer;
            sqlCommand.Parameters.Add(sqlParameter1);

            var sqlParameter3 = new NpgsqlParameter();
            sqlParameter3.ParameterName = "create_timestamp";
            sqlParameter3.NpgsqlDbType = NpgsqlDbType.Bigint;
            sqlCommand.Parameters.Add(sqlParameter3);

            var sqlParameter5 = new NpgsqlParameter();
            sqlParameter5.ParameterName = "update_timestamp";
            sqlParameter5.NpgsqlDbType = NpgsqlDbType.Bigint;
            sqlCommand.Parameters.Add(sqlParameter5);

            //stringBuilder.AppendLine("DO $$");
            //stringBuilder.AppendLine($"DECLARE was_tombstone int;");
            //stringBuilder.AppendLine("BEGIN");
            stringBuilder.AppendLine($"ts := 1;"); // was_tombstone
            stringBuilder.AppendLine($"SELECT {_trackingName.QuotedObjectName}.\"sync_row_is_tombstone\" " +
                                     $"FROM {_trackingName.QuotedObjectName} " +
                                     $"WHERE {PostgreSqlManagementUtils.WhereColumnAndParameters(_tableDescription.PrimaryKey.Columns, _trackingName.QuotedObjectName)} " +
                                     $"LIMIT 1 INTO ts;");
            stringBuilder.AppendLine($"IF (ts is not null and ts = 1 and sync_row_is_tombstone = 0) THEN");

            stringBuilder.AppendLine($"UPDATE {_trackingName.QuotedString}");
            stringBuilder.AppendLine($"SET \"create_scope_id\" = sync_scope_id, ");
            stringBuilder.AppendLine($"\t \"update_scope_id\" = sync_scope_id, ");
            stringBuilder.AppendLine($"\t \"create_timestamp\" = $4, ");
            stringBuilder.AppendLine($"\t \"update_timestamp\" = $5, ");
            stringBuilder.AppendLine($"\t \"sync_row_is_tombstone\" = $3, ");
            stringBuilder.AppendLine($"\t \"timestamp\" = {PostgreSqlObjectNames.TimestampValue}, ");
            stringBuilder.AppendLine($"\t \"last_change_datetime\" = now() ");
            stringBuilder.AppendLine(
                $"WHERE {PostgreSqlManagementUtils.WhereColumnAndParameters(_tableDescription.PrimaryKey.Columns, "")};");

            stringBuilder.AppendLine($"ELSE");

            stringBuilder.AppendLine($"UPDATE {_trackingName.QuotedString}");
            stringBuilder.AppendLine($"SET \"update_scope_id\" = sync_scope_id, ");
            stringBuilder.AppendLine($"\t \"update_timestamp\" = $5, ");
            stringBuilder.AppendLine($"\t \"sync_row_is_tombstone\" = $3, ");
            stringBuilder.AppendLine($"\t \"timestamp\" = {PostgreSqlObjectNames.TimestampValue}, ");
            stringBuilder.AppendLine($"\t \"last_change_datetime\" = now() ");
            stringBuilder.AppendLine(
                $"WHERE {PostgreSqlManagementUtils.WhereColumnAndParameters(_tableDescription.PrimaryKey.Columns, "")};");
            stringBuilder.AppendLine("END IF;");

            stringBuilder.AppendLine("GET DIAGNOSTICS rows = ROW_COUNT;");
            stringBuilder.AppendLine("RETURN rows;");

            sqlCommand.CommandText = stringBuilder.ToString();
            return Tuple.Create(sqlCommand, "integer");
        }


        //------------------------------------------------------------------
        // Select changes command
        //------------------------------------------------------------------
        private Tuple<NpgsqlCommand, string> BuildSelectIncrementalChangesCommand(bool withFilter = false)
        {
            var sqlCommand = new NpgsqlCommand();
            var sqlParameter1 = new NpgsqlParameter();
            sqlParameter1.ParameterName = "sync_min_timestamp";
            sqlParameter1.NpgsqlDbType = NpgsqlDbType.Bigint;
            sqlCommand.Parameters.Add(sqlParameter1);

            var sqlParameter3 = new NpgsqlParameter();
            sqlParameter3.ParameterName = "sync_scope_id";
            sqlParameter3.NpgsqlDbType = NpgsqlDbType.Uuid;
            sqlCommand.Parameters.Add(sqlParameter3);

            var sqlParameter4 = new NpgsqlParameter();
            sqlParameter4.ParameterName = "sync_scope_is_new";
            sqlParameter4.NpgsqlDbType = NpgsqlDbType.Smallint;
            sqlCommand.Parameters.Add(sqlParameter4);

            var sqlParameter5 = new NpgsqlParameter();
            sqlParameter5.ParameterName = "sync_scope_is_reinit";
            sqlParameter5.NpgsqlDbType = NpgsqlDbType.Smallint;
            sqlCommand.Parameters.Add(sqlParameter5);


            //if (withFilter && this.Filters != null && this.Filters.Count > 0)
            //{
            //    foreach (var c in this.Filters)
            //    {
            //        var columnFilter = this.tableDescription.Columns[c.ColumnName];

            //        if (columnFilter == null)
            //            throw new InvalidExpressionException($"Column {c.ColumnName} does not exist in Table {this.tableDescription.TableName}");

            //        var columnFilterName = new ObjectNameParser(columnFilter.ColumnName, "\"", "\"");

            //        MySqlParameter sqlParamFilter = new MySqlParameter($"{columnFilterName.UnquotedString}", columnFilter.GetMySqlDbType());
            //        sqlCommand.Parameters.Add(sqlParamFilter);
            //    }
            //}

            var returnColumns = new List<string>();
            var stringBuilder = new StringBuilder("RETURN QUERY SELECT ");
            foreach (var pkColumn in _tableDescription.PrimaryKey.Columns)
            {
                var pkColumnName = new ObjectNameParser(pkColumn.ColumnName, "\"", "\"");
                stringBuilder.AppendLine($"\t\"side\".{pkColumnName.QuotedString}, ");
                returnColumns.Add($"{pkColumnName} {pkColumn.OriginalDbType}");
            }

            foreach (var column in _tableDescription.NonPkColumns.Where(col => !col.ReadOnly))
            {
                var columnName = new ObjectNameParser(column.ColumnName, "\"", "\"");
                stringBuilder.AppendLine($"\t\"base\".{columnName.QuotedString}, ");
                returnColumns.Add($"{columnName} {column.OriginalDbType}");
            }

            stringBuilder.AppendLine($"\t\"side\".\"sync_row_is_tombstone\", ");
            returnColumns.Add("sync_row_is_tombstone SMALLINT");
            stringBuilder.AppendLine($"\t\"side\".\"create_scope_id\", ");
            returnColumns.Add("create_scope_id UUID");
            stringBuilder.AppendLine($"\t\"side\".\"create_timestamp\", ");
            returnColumns.Add("create_timestamp BIGINT");
            stringBuilder.AppendLine($"\t\"side\".\"update_scope_id\", ");
            returnColumns.Add("update_scope_id UUID");
            stringBuilder.AppendLine($"\t\"side\".\"update_timestamp\" ");
            returnColumns.Add("update_timestamp BIGINT");
            stringBuilder.AppendLine($"FROM {_tableName.QuotedString} \"base\"");
            stringBuilder.AppendLine($"RIGHT JOIN {_trackingName.QuotedString} \"side\"");
            stringBuilder.Append($"ON ");

            var empty = "";
            foreach (var pkColumn in _tableDescription.PrimaryKey.Columns)
            {
                var pkColumnName = new ObjectNameParser(pkColumn.ColumnName, "\"", "\"");
                stringBuilder.Append(
                    $"{empty}\"base\".{pkColumnName.QuotedString} = \"side\".{pkColumnName.QuotedString}");
                empty = " AND ";
            }

            stringBuilder.AppendLine();
            stringBuilder.AppendLine("WHERE (");
            

            if (withFilter && Filters != null && Filters.Count > 0)
            {
                var builderFilter = new StringBuilder();
                builderFilter.Append("\t(");
                var filterSeparationString = "";
                foreach (var c in Filters)
                {
                    var columnFilter = _tableDescription.Columns[c.ColumnName];

                    if (columnFilter == null)
                        throw new InvalidExpressionException(
                            $"Column {c.ColumnName} does not exist in Table {_tableDescription.TableName}");

                    var columnFilterName = new ObjectNameParser(columnFilter.ColumnName, "\"", "\"");

                    builderFilter.Append(
                        $"\"side\".{columnFilterName.QuotedObjectName} = {columnFilterName.UnquotedString}{filterSeparationString}");
                    filterSeparationString = " AND ";
                }

                builderFilter.AppendLine(")");
                builderFilter.Append("\tOR (");
                builderFilter.AppendLine(
                    "(\"side\".\"update_scope_id\" = sync_scope_id or \"side\".\"update_scope_id\" IS NULL)");
                builderFilter.Append("\t\tAND (");

                filterSeparationString = "";
                foreach (var c in Filters)
                {
                    var columnFilter = _tableDescription.Columns[c.ColumnName];
                    var columnFilterName = new ObjectNameParser(columnFilter.ColumnName, "\"", "\"");

                    builderFilter.Append(
                        $"\"side\".{columnFilterName.QuotedObjectName} IS NULL{filterSeparationString}");
                    filterSeparationString = " OR ";
                }

                builderFilter.AppendLine("))");
                builderFilter.AppendLine("\t)");
                builderFilter.AppendLine("AND (");
                stringBuilder.Append(builderFilter);
            }

            stringBuilder.AppendLine("\t-- Update made by the local instance");
            stringBuilder.AppendLine("\t\"side\".\"update_scope_id\" IS NULL");
            stringBuilder.AppendLine("\t-- Or Update different from remote");
            stringBuilder.AppendLine("\tOR \"side\".\"update_scope_id\" <> sync_scope_id");
            stringBuilder.AppendLine("\t-- Or we are in reinit mode so we take rows even thoses updated by the scope");
            stringBuilder.AppendLine("\tOR sync_scope_is_reinit = 1");
            stringBuilder.AppendLine("    )");
            stringBuilder.AppendLine("AND (");
            stringBuilder.AppendLine("\t-- And Timestamp is > from remote timestamp");
            stringBuilder.AppendLine("\t\"side\".\"timestamp\" > sync_min_timestamp");
            stringBuilder.AppendLine("\tOR");
            stringBuilder.AppendLine("\t-- remote instance is new, so we don't take the last timestamp");
            stringBuilder.AppendLine("\tsync_scope_is_new = 1");
            stringBuilder.AppendLine("\t)");
            stringBuilder.AppendLine("AND (");
            stringBuilder.AppendLine("\t\"side\".\"sync_row_is_tombstone\" = 1 ");
            stringBuilder.AppendLine("\tOR");
            stringBuilder.Append("\t(\"side\".\"sync_row_is_tombstone\" = 0");

            empty = " AND ";
            foreach (var pkColumn in _tableDescription.PrimaryKey.Columns)
            {
                var pkColumnName = new ObjectNameParser(pkColumn.ColumnName, "\"", "\"");
                stringBuilder.Append($"{empty}\"base\".{pkColumnName.QuotedString} is not null");
            }

            stringBuilder.AppendLine("\t)");
            stringBuilder.AppendLine(");");


            sqlCommand.CommandText = stringBuilder.ToString();

            //if (this._filterParameters != null)
            //{
            //    foreach (MySqlParameter _filterParameter in this._filterParameters)
            //    {
            //        sqlCommand.Parameters.Add(((ICloneable)_filterParameter).Clone());
            //    }
            //}
            return Tuple.Create(sqlCommand, $"TABLE ({string.Join(",", returnColumns)})");
        }


        private string DropProcedureText(DbCommandType procType)
        {
            var commandName = _sqlObjectNames.GetCommandName(procType);
            var commandText = $"drop procedure if exists {commandName}";

            var str1 = $"Drop procedure {commandName} for table {_tableName.QuotedString}";
            return PostgreSqlBuilder.WrapScriptTextWithComments(commandText, str1);
        }

        private void DropProcedure(DbCommandType procType)
        {
            var commandName = _sqlObjectNames.GetCommandName(procType);
            var commandText = $"drop procedure if exists {commandName}";

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
                Debug.WriteLine($"Error during DropProcedureCommand : {ex}");
                throw;
            }
            finally
            {
                if (!alreadyOpened && _connection.State != ConnectionState.Closed)
                    _connection.Close();
            }
        }
    }
}