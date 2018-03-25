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
        private ObjectNameParser tableName;
        private ObjectNameParser trackingName;
        private NpgsqlConnection connection;
        private NpgsqlTransaction transaction;
        private DmTable tableDescription;
        private PostgreSqlObjectNames sqlObjectNames;
        private PostgreSqlDbMetadata mySqlDbMetadata;
        private string _schemaName;
        internal const string PGSQL_PREFIX_PARAMETER = "in_";

        public FilterClauseCollection Filters { get; set; }

        public PostgreSqlBuilderProcedure(DmTable tableDescription, DbConnection connection, DbTransaction transaction = null)
        {
            this.connection = connection as NpgsqlConnection;
            this.transaction = transaction as NpgsqlTransaction;

            this.tableDescription = tableDescription;
            (this.tableName, this.trackingName) = PostgreSqlBuilder.GetParsers(tableDescription);
            this.sqlObjectNames = new PostgreSqlObjectNames(this.tableDescription);
            this.mySqlDbMetadata = new PostgreSqlDbMetadata();

            _schemaName = new NpgsqlConnectionStringBuilder(connection.ConnectionString).SearchPath ?? "public";
        }

        private void AddPkColumnParametersToCommand(NpgsqlCommand sqlCommand)
        {
            foreach (DmColumn pkColumn in this.tableDescription.PrimaryKey.Columns)
                sqlCommand.Parameters.Add(pkColumn.GetPostgreSqlParameter());
        }
        private void AddColumnParametersToCommand(NpgsqlCommand sqlCommand)
        {
            foreach (DmColumn column in this.tableDescription.Columns.Where(c => !c.ReadOnly))
                sqlCommand.Parameters.Add(column.GetPostgreSqlParameter());
        }

        /// <summary>
        /// From a SqlParameter, create the declaration
        /// </summary>
        internal string CreateParameterDeclaration(NpgsqlParameter param)
        {
            StringBuilder stringBuilder3 = new StringBuilder();
            var sqlDbType = param.NpgsqlDbType;

            string empty = string.Empty;

            var stringType = this.mySqlDbMetadata.GetStringFromDbType(param.DbType);

            string precision = this.mySqlDbMetadata.GetPrecisionStringFromDbType(param.DbType, param.Size, param.Precision, param.Scale);


            stringBuilder3.Append($"{param.ParameterName} {stringType}{precision}");

            return stringBuilder3.ToString();

        }

        /// <summary>
        /// From a SqlCommand, create a stored procedure string
        /// </summary>
        private string CreateProcedureCommandText(NpgsqlCommand cmd, string returnTableDefinition, string procName)
        {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.Append("CREATE FUNCTION ");
            stringBuilder.Append(procName);
            stringBuilder.Append(" (");
            stringBuilder.AppendLine();
            string str = "\n\t";
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
        /// Create a stored procedure
        /// </summary>
        public void CreateProcedureCommand(Func<Tuple<NpgsqlCommand, string>> BuildCommand, string procName)
        {

            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    connection.Open();

                var cmd = BuildCommand();
                var str = CreateProcedureCommandText(cmd.Item1, cmd.Item2, procName);
                using (var command = new NpgsqlCommand(str, connection))
                {
                    if (transaction != null)
                        command.Transaction = transaction;

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
                if (!alreadyOpened && connection.State != ConnectionState.Closed)
                    connection.Close();

            }

        }

        private string CreateProcedureCommandScriptText(Func<Tuple<NpgsqlCommand, string>> BuildCommand, string procName)
        {

            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    connection.Open();

                var str1 = $"Command {procName} for table {tableName.QuotedString}";
                var cmd = BuildCommand();
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
                if (!alreadyOpened && connection.State != ConnectionState.Closed)
                    connection.Close();

            }
        }

        /// <summary>
        /// Check if we need to create the stored procedure
        /// </summary>
        public bool NeedToCreateProcedure(DbCommandType commandType)
        {
            if (connection.State != ConnectionState.Open)
                throw new ArgumentException("Here, we need an opened connection please");

            var commandName = this.sqlObjectNames.GetCommandName(commandType);

            return !PostgreSqlManagementUtils.ProcedureExists(connection, transaction, commandName);
        }

        /// <summary>
        /// Check if we need to create the TVP Type
        /// </summary>
        public bool NeedToCreateType(DbCommandType commandType)
        {
            return false;
        }

        //------------------------------------------------------------------
        // Reset command
        //------------------------------------------------------------------
        private Tuple<NpgsqlCommand, string> BuildResetCommand()
        {
            var updTriggerName = this.sqlObjectNames.GetCommandName(DbCommandType.UpdateTrigger);
            var delTriggerName = this.sqlObjectNames.GetCommandName(DbCommandType.DeleteTrigger);
            var insTriggerName = this.sqlObjectNames.GetCommandName(DbCommandType.InsertTrigger);

            var sqlCommand = new NpgsqlCommand();

            StringBuilder stringBuilder = new StringBuilder();

            stringBuilder.AppendLine($"DELETE FROM {tableName.QuotedString};");
            stringBuilder.AppendLine($"DELETE FROM {trackingName.QuotedString};");

            stringBuilder.AppendLine();
            sqlCommand.CommandText = stringBuilder.ToString();
            return Tuple.Create(sqlCommand, "void");
        }
        public void CreateReset()
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.Reset);
            CreateProcedureCommand(BuildResetCommand, commandName);
        }
        public string CreateResetScriptText()
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.Reset);
            return CreateProcedureCommandScriptText(BuildResetCommand, commandName);
        }

       //------------------------------------------------------------------
        // Delete command
        //------------------------------------------------------------------
        private Tuple<NpgsqlCommand, string> BuildDeleteCommand()
        {
            var sqlCommand = new NpgsqlCommand();
            this.AddPkColumnParametersToCommand(sqlCommand);
            var sqlParameter = new NpgsqlParameter();
            sqlParameter.ParameterName = "sync_force_write";
            sqlParameter.NpgsqlDbType = NpgsqlDbType.Integer;
            sqlCommand.Parameters.Add(sqlParameter);

            var sqlParameter1 = new NpgsqlParameter();
            sqlParameter1.ParameterName = "sync_min_timestamp";
            sqlParameter1.NpgsqlDbType = NpgsqlDbType.Bigint;
            sqlCommand.Parameters.Add(sqlParameter1);

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("ts := 0;");
            stringBuilder.AppendLine($"SELECT \"timestamp\" FROM {trackingName.QuotedObjectName} WHERE {PostgreSqlManagementUtils.WhereColumnAndParameters(this.tableDescription.PrimaryKey.Columns, trackingName.QuotedObjectName)} LIMIT 1 INTO ts;");
            stringBuilder.AppendLine($"DELETE FROM {tableName.QuotedString} WHERE");
            stringBuilder.AppendLine(PostgreSqlManagementUtils.WhereColumnAndParameters(this.tableDescription.PrimaryKey.Columns, ""));
            stringBuilder.AppendLine("AND (ts <= sync_min_timestamp  OR sync_force_write = 1);");
            stringBuilder.AppendLine("GET DIAGNOSTICS rows = ROW_COUNT;");
            stringBuilder.AppendLine("RETURN rows;");
            sqlCommand.CommandText = stringBuilder.ToString();
            return Tuple.Create(sqlCommand, "integer");
        }

        public void CreateDelete()
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.DeleteRow);
            CreateProcedureCommand(BuildDeleteCommand, commandName);
        }

        public string CreateDeleteScriptText()
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.DeleteRow);
            return CreateProcedureCommandScriptText(BuildDeleteCommand, commandName);
        }


        //------------------------------------------------------------------
        // Delete Metadata command
        //------------------------------------------------------------------
        private Tuple<NpgsqlCommand, string> BuildDeleteMetadataCommand()
        {
            var sqlCommand = new NpgsqlCommand();
            this.AddPkColumnParametersToCommand(sqlCommand);
            var sqlParameter = new NpgsqlParameter();
            sqlParameter.ParameterName = "sync_check_concurrency";
            sqlParameter.NpgsqlDbType = NpgsqlDbType.Integer;
            sqlCommand.Parameters.Add(sqlParameter);

            var sqlParameter1 = new NpgsqlParameter();
            sqlParameter1.ParameterName = "sync_row_timestamp";
            sqlParameter1.NpgsqlDbType = NpgsqlDbType.Bigint;
            sqlCommand.Parameters.Add(sqlParameter1);

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"DELETE FROM {trackingName.QuotedString} ");
            stringBuilder.Append($"WHERE ");
            stringBuilder.AppendLine(PostgreSqlManagementUtils.ColumnsAndParameters(this.tableDescription.PrimaryKey.Columns, ""));
            stringBuilder.Append(";");
            sqlCommand.CommandText = stringBuilder.ToString();
            return Tuple.Create(sqlCommand, "void");
        }

        public void CreateDeleteMetadata()
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.DeleteMetadata);
            CreateProcedureCommand(BuildDeleteMetadataCommand, commandName);
        }

        public string CreateDeleteMetadataScriptText()
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.DeleteMetadata);
            return CreateProcedureCommandScriptText(BuildDeleteMetadataCommand, commandName);
        }


        //------------------------------------------------------------------
        // Insert command
        //------------------------------------------------------------------
        private Tuple<NpgsqlCommand, string> BuildInsertCommand()
        {
            var sqlCommand = new NpgsqlCommand();
            StringBuilder stringBuilder = new StringBuilder();
            StringBuilder stringBuilderArguments = new StringBuilder();
            StringBuilder stringBuilderParameters = new StringBuilder();

            this.AddColumnParametersToCommand(sqlCommand);


            stringBuilder.Append(string.Concat("IF ((SELECT COUNT(*) FROM ", trackingName.QuotedString, " WHERE "));
            stringBuilder.Append(PostgreSqlManagementUtils.ColumnsAndParameters(this.tableDescription.PrimaryKey.Columns, string.Empty));
            stringBuilder.AppendLine(") <= 0) THEN");

            string empty = string.Empty;
            foreach (var mutableColumn in this.tableDescription.Columns.Where(c => !c.ReadOnly))
            {
                ObjectNameParser columnName = new ObjectNameParser(mutableColumn.ColumnName, "\"", "\"");
                stringBuilderArguments.Append(string.Concat(empty, columnName.QuotedString));
                stringBuilderParameters.Append(string.Concat(empty, $"{PGSQL_PREFIX_PARAMETER}{columnName.UnquotedString}"));
                empty = ", ";
            }


            stringBuilder.AppendLine($"\tINSERT INTO {tableName.QuotedString}");
            stringBuilder.AppendLine($"\t({stringBuilderArguments.ToString()})");
            stringBuilder.AppendLine($"\tVALUES ({stringBuilderParameters.ToString()});");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("GET DIAGNOSTICS rows = ROW_COUNT;");
            stringBuilder.AppendLine("RETURN rows;");
            stringBuilder.AppendLine("END IF;");
            stringBuilder.AppendLine("RETURN 0;");
            sqlCommand.CommandText = stringBuilder.ToString();
            return Tuple.Create(sqlCommand, "INT");
        }

        public void CreateInsert()
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.InsertRow);
            CreateProcedureCommand(BuildInsertCommand, commandName);
        }

        public string CreateInsertScriptText()
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.InsertRow);
            return CreateProcedureCommandScriptText(BuildInsertCommand, commandName);
        }

        //------------------------------------------------------------------
        // Insert Metadata command
        //------------------------------------------------------------------
        private Tuple<NpgsqlCommand, string> BuildInsertMetadataCommand()
        {
            StringBuilder stringBuilderArguments = new StringBuilder();
            StringBuilder stringBuilderParameters = new StringBuilder();
            var sqlCommand = new NpgsqlCommand();

            StringBuilder stringBuilder = new StringBuilder();
            this.AddPkColumnParametersToCommand(sqlCommand);
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

            stringBuilder.AppendLine($"\tINSERT INTO {trackingName.QuotedString}");

            string empty = string.Empty;
            var pkColumns = new List<string>();
            foreach (var pkColumn in this.tableDescription.PrimaryKey.Columns)
            {
                ObjectNameParser columnName = new ObjectNameParser(pkColumn.ColumnName, "\"", "\"");
                stringBuilderArguments.Append(string.Concat(empty, columnName.QuotedString));
                stringBuilderParameters.Append(string.Concat(empty, $"{PGSQL_PREFIX_PARAMETER}{columnName.UnquotedString}"));
                pkColumns.Add(columnName.QuotedString);
                empty = ", ";
            }
            stringBuilder.Append($"\t({stringBuilderArguments.ToString()}, ");
            stringBuilder.AppendLine($"\"create_scope_id\", \"create_timestamp\", \"update_scope_id\", \"update_timestamp\",");
            stringBuilder.AppendLine($"\t\"sync_row_is_tombstone\", \"timestamp\", \"last_change_datetime\")");
            stringBuilder.Append($"\tVALUES ({stringBuilderParameters.ToString()}, ");
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

        public void CreateInsertMetadata()
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.InsertMetadata);
            CreateProcedureCommand(BuildInsertMetadataCommand, commandName);
        }

        public string CreateInsertMetadataScriptText()
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.InsertMetadata);
            return CreateProcedureCommandScriptText(BuildInsertMetadataCommand, commandName);
        }


        //------------------------------------------------------------------
        // Select Row command
        //------------------------------------------------------------------
        private Tuple<NpgsqlCommand, string> BuildSelectRowCommand()
        {
            var sqlCommand = new NpgsqlCommand();
            this.AddPkColumnParametersToCommand(sqlCommand);

            var sqlParameter = new NpgsqlParameter();
            sqlParameter.ParameterName = "sync_scope_id";
            sqlParameter.NpgsqlDbType = NpgsqlDbType.Uuid;
            sqlCommand.Parameters.Add(sqlParameter);

            StringBuilder stringBuilder = new StringBuilder("RETURN QUERY SELECT ");
            var returnColums = new List<string>();
            stringBuilder.AppendLine();
            StringBuilder stringBuilder1 = new StringBuilder();
            string empty = string.Empty;
            foreach (var pkColumn in this.tableDescription.PrimaryKey.Columns)
            {
                ObjectNameParser pkColumnName = new ObjectNameParser(pkColumn.ColumnName, "\"", "\"");
                stringBuilder.AppendLine($"\t\"side\".{pkColumnName.QuotedString}, ");
                stringBuilder1.Append($"{empty}\"side\".{pkColumnName.QuotedString} = {PGSQL_PREFIX_PARAMETER}{pkColumnName.UnquotedString}");
                returnColums.Add($"{pkColumnName} {pkColumn.OriginalTypeName}");
                empty = " AND ";
            }
            foreach (DmColumn nonPkMutableColumn in this.tableDescription.NonPkColumns.Where(c => !c.ReadOnly))
            {
                ObjectNameParser nonPkColumnName = new ObjectNameParser(nonPkMutableColumn.ColumnName, "\"", "\"");
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


            stringBuilder.AppendLine($"FROM {tableName.QuotedString} \"base\"");
            stringBuilder.AppendLine($"RIGHT JOIN {trackingName.QuotedString} \"side\" ON");

            string str = string.Empty;
            foreach (var pkColumn in this.tableDescription.PrimaryKey.Columns)
            {
                ObjectNameParser pkColumnName = new ObjectNameParser(pkColumn.ColumnName, "\"", "\"");
                stringBuilder.Append($"{str}\"base\".{pkColumnName.QuotedString} = \"side\".{pkColumnName.QuotedString}");
                str = " AND ";
            }
            stringBuilder.AppendLine();
            stringBuilder.Append("WHERE ");
            stringBuilder.Append(stringBuilder1.ToString());
            stringBuilder.Append(";");
            sqlCommand.CommandText = stringBuilder.ToString();
            return Tuple.Create(sqlCommand, $"TABLE ({string.Join(",", returnColums)})");
        }

        public void CreateSelectRow()
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.SelectRow);
            CreateProcedureCommand(BuildSelectRowCommand, commandName);
        }

        public string CreateSelectRowScriptText()
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.SelectRow);
            return CreateProcedureCommandScriptText(BuildSelectRowCommand, commandName);
        }


        //------------------------------------------------------------------
        // Update command
        //------------------------------------------------------------------
        private Tuple<NpgsqlCommand, string> BuildUpdateCommand()
        {
            var sqlCommand = new NpgsqlCommand();

            StringBuilder stringBuilder = new StringBuilder();
            this.AddColumnParametersToCommand(sqlCommand);

            var sqlParameter = new NpgsqlParameter();
            sqlParameter.ParameterName = "sync_force_write";
            sqlParameter.NpgsqlDbType = NpgsqlDbType.Integer;
            sqlCommand.Parameters.Add(sqlParameter);

            var sqlParameter1 = new NpgsqlParameter();
            sqlParameter1.ParameterName = "sync_min_timestamp";
            sqlParameter1.NpgsqlDbType = NpgsqlDbType.Bigint;
            sqlCommand.Parameters.Add(sqlParameter1);

            var whereArgs = new List<string>();
            foreach (var pkColumn in tableDescription.PrimaryKey.Columns)
            {
                var pk = new ObjectNameParser(pkColumn.ColumnName, "\"", "\"");
                whereArgs.Add($"{pk.QuotedObjectName} = {PGSQL_PREFIX_PARAMETER}{pk.UnquotedString.ToLowerInvariant()}");
            }

            stringBuilder.AppendLine("ts := 0;");
            stringBuilder.AppendLine($"SELECT \"timestamp\" FROM {trackingName.QuotedObjectName} WHERE {string.Join(" AND ", whereArgs.Select(a=>$"{trackingName.QuotedObjectName}.{a}"))} LIMIT 1 INTO ts;");

            stringBuilder.AppendLine($"UPDATE {tableName.QuotedString}");
            stringBuilder.Append($"SET {PostgreSqlManagementUtils.CommaSeparatedUpdateFromParameters(this.tableDescription)}");
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

        public void CreateUpdate()
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.UpdateRow);
            this.CreateProcedureCommand(BuildUpdateCommand, commandName);
        }

        public string CreateUpdateScriptText()
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.UpdateRow);
            return CreateProcedureCommandScriptText(BuildUpdateCommand, commandName);
        }

        //------------------------------------------------------------------
        // Update Metadata command
        //------------------------------------------------------------------
        private Tuple<NpgsqlCommand, string> BuildUpdateMetadataCommand()
        {
            var sqlCommand = new NpgsqlCommand();
            StringBuilder stringBuilder = new StringBuilder();
            this.AddPkColumnParametersToCommand(sqlCommand);
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
            stringBuilder.AppendLine($"SELECT {trackingName.QuotedObjectName}.\"sync_row_is_tombstone\" " +
                                     $"FROM {trackingName.QuotedObjectName} " +
                                     $"WHERE {PostgreSqlManagementUtils.WhereColumnAndParameters(this.tableDescription.PrimaryKey.Columns, trackingName.QuotedObjectName)} " +
                                     $"LIMIT 1 INTO ts;");
            stringBuilder.AppendLine($"IF (ts is not null and ts = 1 and sync_row_is_tombstone = 0) THEN");

            stringBuilder.AppendLine($"UPDATE {trackingName.QuotedString}");
            stringBuilder.AppendLine($"SET \"create_scope_id\" = sync_scope_id, ");
            stringBuilder.AppendLine($"\t \"update_scope_id\" = sync_scope_id, ");
            stringBuilder.AppendLine($"\t \"create_timestamp\" = $4, ");
            stringBuilder.AppendLine($"\t \"update_timestamp\" = $5, ");
            stringBuilder.AppendLine($"\t \"sync_row_is_tombstone\" = $3, ");
            stringBuilder.AppendLine($"\t \"timestamp\" = {PostgreSqlObjectNames.TimestampValue}, ");
            stringBuilder.AppendLine($"\t \"last_change_datetime\" = now() ");
            stringBuilder.AppendLine($"WHERE {PostgreSqlManagementUtils.WhereColumnAndParameters(this.tableDescription.PrimaryKey.Columns, "")};");

            stringBuilder.AppendLine($"ELSE");

            stringBuilder.AppendLine($"UPDATE {trackingName.QuotedString}");
            stringBuilder.AppendLine($"SET \"update_scope_id\" = sync_scope_id, ");
            stringBuilder.AppendLine($"\t \"update_timestamp\" = $5, ");
            stringBuilder.AppendLine($"\t \"sync_row_is_tombstone\" = $3, ");
            stringBuilder.AppendLine($"\t \"timestamp\" = {PostgreSqlObjectNames.TimestampValue}, ");
            stringBuilder.AppendLine($"\t \"last_change_datetime\" = now() ");
            stringBuilder.AppendLine($"WHERE {PostgreSqlManagementUtils.WhereColumnAndParameters(this.tableDescription.PrimaryKey.Columns, "")};");
            stringBuilder.AppendLine("END IF;");

            stringBuilder.AppendLine("GET DIAGNOSTICS rows = ROW_COUNT;");
            stringBuilder.AppendLine("RETURN rows;");

            sqlCommand.CommandText = stringBuilder.ToString();
            return Tuple.Create(sqlCommand, "integer");
        }

        public void CreateUpdateMetadata()
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.UpdateMetadata);
            CreateProcedureCommand(BuildUpdateMetadataCommand, commandName);
        }

        public string CreateUpdateMetadataScriptText()
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.UpdateMetadata);
            return CreateProcedureCommandScriptText(BuildUpdateMetadataCommand, commandName);
        }


        //------------------------------------------------------------------
        // Select changes command
        //------------------------------------------------------------------
        private Tuple<NpgsqlCommand, string> BuildSelectIncrementalChangesCommand(bool withFilter = false)
        {
            var sqlCommand = new NpgsqlCommand();
            var sqlParameter1 = new NpgsqlParameter();
            sqlParameter1.ParameterName = "sync_min_timestamp";
            sqlParameter1.NpgsqlDbType= NpgsqlDbType.Bigint;
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
            StringBuilder stringBuilder = new StringBuilder("RETURN QUERY SELECT ");
            foreach (var pkColumn in this.tableDescription.PrimaryKey.Columns)
            {
                var pkColumnName = new ObjectNameParser(pkColumn.ColumnName, "\"", "\"");
                stringBuilder.AppendLine($"\t\"side\".{pkColumnName.QuotedString}, ");
                returnColumns.Add($"{pkColumnName} {pkColumn.OriginalDbType}");
            }
            foreach (var column in this.tableDescription.NonPkColumns.Where(col => !col.ReadOnly))
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
            stringBuilder.AppendLine($"FROM {tableName.QuotedString} \"base\"");
            stringBuilder.AppendLine($"RIGHT JOIN {trackingName.QuotedString} \"side\"");
            stringBuilder.Append($"ON ");

            string empty = "";
            foreach (var pkColumn in this.tableDescription.PrimaryKey.Columns)
            {
                var pkColumnName = new ObjectNameParser(pkColumn.ColumnName, "\"", "\"");
                stringBuilder.Append($"{empty}\"base\".{pkColumnName.QuotedString} = \"side\".{pkColumnName.QuotedString}");
                empty = " AND ";
            }
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("WHERE (");
            string str = string.Empty;

            if (withFilter && this.Filters != null && this.Filters.Count > 0)
            {
                StringBuilder builderFilter = new StringBuilder();
                builderFilter.Append("\t(");
                string filterSeparationString = "";
                foreach (var c in this.Filters)
                {
                    var columnFilter = this.tableDescription.Columns[c.ColumnName];

                    if (columnFilter == null)
                        throw new InvalidExpressionException($"Column {c.ColumnName} does not exist in Table {this.tableDescription.TableName}");

                    var columnFilterName = new ObjectNameParser(columnFilter.ColumnName, "\"", "\"");

                    builderFilter.Append($"\"side\".{columnFilterName.QuotedObjectName} = {columnFilterName.UnquotedString}{filterSeparationString}");
                    filterSeparationString = " AND ";
                }
                builderFilter.AppendLine(")");
                builderFilter.Append("\tOR (");
                builderFilter.AppendLine("(\"side\".\"update_scope_id\" = sync_scope_id or \"side\".\"update_scope_id\" IS NULL)");
                builderFilter.Append("\t\tAND (");

                filterSeparationString = "";
                foreach (var c in this.Filters)
                {
                    var columnFilter = this.tableDescription.Columns[c.ColumnName];
                    var columnFilterName = new ObjectNameParser(columnFilter.ColumnName, "\"", "\"");

                    builderFilter.Append($"\"side\".{columnFilterName.QuotedObjectName} IS NULL{filterSeparationString}");
                    filterSeparationString = " OR ";
                }

                builderFilter.AppendLine("))");
                builderFilter.AppendLine("\t)");
                builderFilter.AppendLine("AND (");
                stringBuilder.Append(builderFilter.ToString());
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
            foreach (var pkColumn in this.tableDescription.PrimaryKey.Columns)
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

        public void CreateSelectIncrementalChanges()
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.SelectChanges);
            Func<Tuple<NpgsqlCommand, string>> cmdWithoutFilter = () => BuildSelectIncrementalChangesCommand(false);
            CreateProcedureCommand(cmdWithoutFilter, commandName);

            if (this.Filters != null && this.Filters.Count > 0)
            {
                foreach (var c in this.Filters)
                {
                    var columnFilter = this.tableDescription.Columns[c.ColumnName];

                    if (columnFilter == null)
                        throw new InvalidExpressionException($"Column {c.ColumnName} does not exist in Table {this.tableDescription.TableName}");
                }

                var filtersName = this.Filters.Select(f => f.ColumnName);
                commandName = this.sqlObjectNames.GetCommandName(DbCommandType.SelectChangesWitFilters, filtersName);
                Func<Tuple<NpgsqlCommand, string>> cmdWithFilter = () => BuildSelectIncrementalChangesCommand(true);
                CreateProcedureCommand(cmdWithFilter, commandName);

            }

        }

        public string CreateSelectIncrementalChangesScriptText()
        {
            StringBuilder sbSelecteChanges = new StringBuilder();

            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.SelectChanges);
            Func<Tuple<NpgsqlCommand, string>> cmdWithoutFilter = () => BuildSelectIncrementalChangesCommand(false);
            sbSelecteChanges.AppendLine(CreateProcedureCommandScriptText(cmdWithoutFilter, commandName));


            if (this.Filters != null && this.Filters.Count > 0)
            {
                commandName = this.sqlObjectNames.GetCommandName(DbCommandType.SelectChangesWitFilters);
                string name = "";
                string sep = "";
                foreach (var c in this.Filters)
                {
                    var columnFilter = this.tableDescription.Columns[c.ColumnName];

                    if (columnFilter == null)
                        throw new InvalidExpressionException($"Column {c.ColumnName} does not exist in Table {this.tableDescription.TableName}");

                    var unquotedColumnName = new ObjectNameParser(columnFilter.ColumnName, "\"", "\"").UnquotedString;
                    name += $"{unquotedColumnName}{sep}";
                    sep = "_";
                }

                commandName = String.Format(commandName, name);
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


        private string DropProcedureText(DbCommandType procType)
        {
            var commandName = this.sqlObjectNames.GetCommandName(procType);
            var commandText = $"drop procedure if exists {commandName}";

            var str1 = $"Drop procedure {commandName} for table {tableName.QuotedString}";
            return PostgreSqlBuilder.WrapScriptTextWithComments(commandText, str1);

        }
        private void DropProcedure(DbCommandType procType)
        {
            var commandName = this.sqlObjectNames.GetCommandName(procType);
            var commandText = $"drop procedure if exists {commandName}";

            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    connection.Open();

                using (var command = new NpgsqlCommand(commandText, connection))
                {
                    if (transaction != null)
                        command.Transaction = transaction;

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
                if (!alreadyOpened && connection.State != ConnectionState.Closed)
                    connection.Close();

            }

        }

        public void DropSelectRow()
        {
            DropProcedure(DbCommandType.SelectRow);
        }

        public void DropSelectIncrementalChanges()
        {
            DropProcedure(DbCommandType.SelectChanges);

            // filtered 
            if (this.Filters != null && this.Filters.Count > 0)
            {
                bool alreadyOpened = this.connection.State == ConnectionState.Open;

                using (var command = new NpgsqlCommand())
                {
                    if (!alreadyOpened)
                        this.connection.Open();

                    if (this.transaction != null)
                        command.Transaction = this.transaction;

                    foreach (var c in this.Filters)
                    {
                        var columnFilter = this.tableDescription.Columns[c.ColumnName];

                        if (columnFilter == null)
                            throw new InvalidExpressionException($"Column {c.ColumnName} does not exist in Table {this.tableDescription.TableName}");
                    }

                    var filtersName = this.Filters.Select(f => f.ColumnName);
                    var commandNameWithFilter = this.sqlObjectNames.GetCommandName(DbCommandType.SelectChangesWitFilters, filtersName);

                    command.CommandText = $"DROP PROCEDURE IF EXISTS {commandNameWithFilter};";
                    command.Connection = this.connection;
                    command.ExecuteNonQuery();

                }

                if (!alreadyOpened && this.connection.State != ConnectionState.Closed)
                    this.connection.Close();

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
    }
}
