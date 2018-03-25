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

namespace Dotmim.Sync.PostgreSql.Builders
{
    public class PostgreSqlBuilderTrigger : IDbBuilderTriggerHelper
    {
        private readonly NpgsqlConnection _connection;
        private readonly PostgreSqlObjectNames _postgreSqlObjectNames;
        private readonly string _schemaName;
        private readonly DmTable _tableDescription;
        private readonly ObjectNameParser _tableName;
        private readonly ObjectNameParser _trackingName;
        private readonly NpgsqlTransaction _transaction;


        public PostgreSqlBuilderTrigger(DmTable tableDescription, DbConnection connection,
            DbTransaction transaction = null)
        {
            _connection = connection as NpgsqlConnection;
            _transaction = transaction as NpgsqlTransaction;
            _tableDescription = tableDescription;
            (_tableName, _trackingName) = PostgreSqlBuilder.GetParsers(_tableDescription);
            _postgreSqlObjectNames = new PostgreSqlObjectNames(_tableDescription);
            _schemaName = new NpgsqlConnectionStringBuilder(connection.ConnectionString).SearchPath ?? "public";
        }

        public FilterClauseCollection Filters { get; set; }

        public void CreateDeleteTrigger()
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

                    var delTriggerName = _postgreSqlObjectNames.GetCommandName(DbCommandType.DeleteTrigger);


                    command.CommandText = CreateDeleteProcedureCommand(delTriggerName);
                    command.Connection = _connection;
                    command.ExecuteNonQuery();

                    var createTrigger = new StringBuilder();
                    createTrigger.AppendLine(
                        $"CREATE TRIGGER {delTriggerName} AFTER DELETE ON {_tableName.QuotedString} FOR EACH ROW ");
                    createTrigger.AppendLine($"EXECUTE PROCEDURE {delTriggerName}()");

                    command.CommandText = createTrigger.ToString();
                    command.Connection = _connection;
                    command.ExecuteNonQuery();
                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during CreateDeleteTrigger : {ex}");
                throw;
            }
            finally
            {
                if (!alreadyOpened && _connection.State != ConnectionState.Closed)
                    _connection.Close();
            }
        }

        public string CreateDeleteTriggerScriptText()
        {
            var delTriggerName = string.Format(_postgreSqlObjectNames.GetCommandName(DbCommandType.DeleteTrigger),
                _tableName.UnquotedStringWithUnderScore);
            var createTrigger = new StringBuilder();
            createTrigger.AppendLine(
                $"CREATE TRIGGER {delTriggerName} AFTER DELETE ON {_tableName.QuotedString} FOR EACH ROW ");
            createTrigger.AppendLine();
            createTrigger.AppendLine(DeleteTriggerBodyText());

            var str = $"Delete Trigger for table {_tableName.QuotedString}";
            return PostgreSqlBuilder.WrapScriptTextWithComments(createTrigger.ToString(), str);
        }

        public void AlterDeleteTrigger()
        {
        }

        public string AlterDeleteTriggerScriptText()
        {
            return "";
        }

        public void CreateInsertTrigger()
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

                    var insTriggerName =
                        string.Format(_postgreSqlObjectNames.GetCommandName(DbCommandType.InsertTrigger),
                            _tableName.UnquotedStringWithUnderScore);


                    command.CommandText = CreateInsertProcedureCommand(insTriggerName);
                    command.Connection = _connection;
                    command.ExecuteNonQuery();


                    var createTrigger = new StringBuilder();
                    createTrigger.AppendLine(
                        $"CREATE TRIGGER {insTriggerName} AFTER INSERT ON {_tableName.QuotedString} FOR EACH ROW ");
                    createTrigger.AppendLine($"EXECUTE PROCEDURE {insTriggerName}()");


                    command.CommandText = createTrigger.ToString();
                    command.Connection = _connection;
                    command.ExecuteNonQuery();
                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during CreateDeleteTrigger : {ex}");
                throw;
            }
            finally
            {
                if (!alreadyOpened && _connection.State != ConnectionState.Closed)
                    _connection.Close();
            }
        }

        public string CreateInsertTriggerScriptText()
        {
            var insTriggerName = string.Format(_postgreSqlObjectNames.GetCommandName(DbCommandType.InsertTrigger),
                _tableName.UnquotedStringWithUnderScore);
            var createTrigger = new StringBuilder();
            createTrigger.AppendLine(
                $"CREATE TRIGGER {insTriggerName} AFTER INSERT ON {_tableName.QuotedString} FOR EACH ROW ");
            createTrigger.AppendLine();
            createTrigger.AppendLine(InsertTriggerBodyText());

            var str = $"Insert Trigger for table {_tableName.QuotedString}";
            return PostgreSqlBuilder.WrapScriptTextWithComments(createTrigger.ToString(), str);
        }

        public void AlterInsertTrigger()
        {
        }

        public string AlterInsertTriggerScriptText()
        {
            return "";
        }

        public void CreateUpdateTrigger()
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

                    var updTriggerName =
                        string.Format(_postgreSqlObjectNames.GetCommandName(DbCommandType.UpdateTrigger),
                            _tableName.UnquotedStringWithUnderScore);

                    command.CommandText = CreateUpdateProcedureCommand(updTriggerName);
                    command.Connection = _connection;
                    command.ExecuteNonQuery();

                    var createTrigger = new StringBuilder();
                    createTrigger.AppendLine(
                        $"CREATE TRIGGER {updTriggerName} AFTER UPDATE ON {_tableName.QuotedString} FOR EACH ROW ");
                    createTrigger.AppendLine($"EXECUTE PROCEDURE {updTriggerName}()");

                    command.CommandText = createTrigger.ToString();
                    command.Connection = _connection;
                    command.ExecuteNonQuery();
                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during CreateDeleteTrigger : {ex}");
                throw;
            }
            finally
            {
                if (!alreadyOpened && _connection.State != ConnectionState.Closed)
                    _connection.Close();
            }
        }

        public string CreateUpdateTriggerScriptText()
        {
            var updTriggerName = string.Format(_postgreSqlObjectNames.GetCommandName(DbCommandType.UpdateTrigger),
                _tableName.UnquotedStringWithUnderScore);
            var createTrigger = new StringBuilder();
            createTrigger.AppendLine(
                $"CREATE TRIGGER {updTriggerName} AFTER UPDATE ON {_tableName.QuotedString} FOR EACH ROW ");
            createTrigger.AppendLine();
            createTrigger.AppendLine(UpdateTriggerBodyText());

            var str = $"Update Trigger for table {_tableName.QuotedString}";
            return PostgreSqlBuilder.WrapScriptTextWithComments(createTrigger.ToString(), str);
        }

        public void AlterUpdateTrigger()
        {
        }

        public string AlterUpdateTriggerScriptText()
        {
            return string.Empty;
        }

        public bool NeedToCreateTrigger(DbTriggerType type)
        {
            var updTriggerName = string.Format(_postgreSqlObjectNames.GetCommandName(DbCommandType.UpdateTrigger),
                _tableName.UnquotedStringWithUnderScore);
            var delTriggerName = string.Format(_postgreSqlObjectNames.GetCommandName(DbCommandType.DeleteTrigger),
                _tableName.UnquotedStringWithUnderScore);
            var insTriggerName = string.Format(_postgreSqlObjectNames.GetCommandName(DbCommandType.InsertTrigger),
                _tableName.UnquotedStringWithUnderScore);

            var triggerName = string.Empty;
            switch (type)
            {
                case DbTriggerType.Insert:
                {
                    triggerName = insTriggerName;
                    break;
                }
                case DbTriggerType.Update:
                {
                    triggerName = updTriggerName;
                    break;
                }
                case DbTriggerType.Delete:
                {
                    triggerName = delTriggerName;
                    break;
                }
            }

            return !PostgreSqlManagementUtils.TriggerExists(_connection, _transaction, triggerName);
        }


        public void DropInsertTrigger()
        {
            DropTrigger(DbCommandType.InsertTrigger);
        }

        public void DropUpdateTrigger()
        {
            DropTrigger(DbCommandType.UpdateTrigger);
        }

        public void DropDeleteTrigger()
        {
            DropTrigger(DbCommandType.DeleteTrigger);
        }

        public string DropInsertTriggerScriptText()
        {
            return DropTriggerText(DbCommandType.InsertTrigger);
        }

        public string DropUpdateTriggerScriptText()
        {
            return DropTriggerText(DbCommandType.UpdateTrigger);
        }

        public string DropDeleteTriggerScriptText()
        {
            return DropTriggerText(DbCommandType.DeleteTrigger);
        }

        private string CreateDeleteProcedureCommand(string procedureName)
        {
            return $@"CREATE OR REPLACE FUNCTION {procedureName}()
                        RETURNS trigger 
                        LANGUAGE 'plpgsql'
                        COST 100 
                        SET search_path='{_schemaName}'
                        VOLATILE NOT LEAKPROOF SECURITY DEFINER 
                      AS $BODY$
                        {DeleteTriggerBodyText()}
                      $BODY$;";
        }

        private string DeleteTriggerBodyText()
        {
            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("BEGIN");
            stringBuilder.AppendLine($"UPDATE {_trackingName.QuotedString} ");
            stringBuilder.AppendLine("SET \"sync_row_is_tombstone\" = 1");
            stringBuilder.AppendLine("\t,\"update_scope_id\" = NULL -- since the update if from local, it's a NULL");
            stringBuilder.AppendLine($"\t,\"update_timestamp\" = {PostgreSqlObjectNames.TimestampValue}");
            stringBuilder.AppendLine($"\t,\"timestamp\" = {PostgreSqlObjectNames.TimestampValue}");
            stringBuilder.AppendLine("\t,\"last_change_datetime\" = now()");

            // Filter columns
            if (Filters != null)
            {
                for (var i = 0; i < Filters.Count; i++)
                {
                    var filterColumn = Filters[i];

                    if (_tableDescription.PrimaryKey.Columns.Any(c =>
                        c.ColumnName.ToLowerInvariant() == filterColumn.ColumnName.ToLowerInvariant()))
                        continue;

                    var columnName = new ObjectNameParser(filterColumn.ColumnName.ToLowerInvariant(), "\"", "\"");

                    stringBuilder.AppendLine($"\t,{columnName.QuotedString} = \"d\".{columnName.QuotedString}");
                }

                stringBuilder.AppendLine();
            }

            stringBuilder.Append($"WHERE ");
            stringBuilder.Append(PostgreSqlManagementUtils.JoinTwoTablesOnClause(_tableDescription.PrimaryKey.Columns,
                _trackingName.QuotedString, "old"));
            stringBuilder.AppendLine(";");
            stringBuilder.AppendLine("RETURN OLD;");
            stringBuilder.AppendLine("END;");
            return stringBuilder.ToString();
        }


        /// <summary>
        ///     TODO : Check if row was deleted before, to just make an update !!!!
        /// </summary>
        /// <returns></returns>
        private string InsertTriggerBodyText()
        {
            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("-- If row was deleted before, it already exists, so just make an update");
            stringBuilder.AppendLine("BEGIN");

            stringBuilder.AppendLine($"\tINSERT INTO {_trackingName.QuotedString} (");

            var stringBuilderArguments = new StringBuilder();
            var stringBuilderArguments2 = new StringBuilder();
            var stringPkAreNull = new StringBuilder();

            var argComma = string.Empty;
            var argAnd = string.Empty;
            var pkColumns = new List<string>();
            foreach (var mutableColumn in _tableDescription.PrimaryKey.Columns.Where(c => !c.ReadOnly))
            {
                var columnName = new ObjectNameParser(mutableColumn.ColumnName, "\"", "\"");
                stringBuilderArguments.AppendLine($"\t\t{argComma}{columnName.QuotedString}");
                stringBuilderArguments2.AppendLine($"\t\t{argComma}new.{columnName.QuotedString}");
                stringPkAreNull.Append($"{argAnd}{_trackingName.QuotedString}.{columnName.QuotedString} IS NULL");
                pkColumns.Add(columnName.QuotedString);
                argComma = ",";
                argAnd = " AND ";
            }

            stringBuilder.Append(stringBuilderArguments);
            stringBuilder.AppendLine("\t\t,\"create_scope_id\"");
            stringBuilder.AppendLine("\t\t,\"create_timestamp\"");
            stringBuilder.AppendLine("\t\t,\"update_scope_id\"");
            stringBuilder.AppendLine("\t\t,\"update_timestamp\"");
            stringBuilder.AppendLine("\t\t,\"timestamp\"");
            stringBuilder.AppendLine("\t\t,\"sync_row_is_tombstone\"");
            stringBuilder.AppendLine("\t\t,\"last_change_datetime\"");

            var filterColumnsString = new StringBuilder();

            // Filter columns
            if (Filters != null && Filters.Count > 0)
            {
                for (var i = 0; i < Filters.Count; i++)
                {
                    var filterColumn = Filters[i];
                    if (_tableDescription.PrimaryKey.Columns.Any(c =>
                        c.ColumnName.ToLowerInvariant() == filterColumn.ColumnName.ToLowerInvariant()))
                        continue;

                    var columnName = new ObjectNameParser(filterColumn.ColumnName, "\"", "\"");
                    filterColumnsString.AppendLine($"\t,{columnName.QuotedString}");
                }

                stringBuilder.AppendLine(filterColumnsString.ToString());
            }

            stringBuilder.AppendLine("\t) ");
            stringBuilder.AppendLine("\tVALUES (");
            stringBuilder.Append(stringBuilderArguments2);
            stringBuilder.AppendLine("\t\t,NULL");
            stringBuilder.AppendLine($"\t\t,{PostgreSqlObjectNames.TimestampValue}");
            stringBuilder.AppendLine("\t\t,NULL");
            stringBuilder.AppendLine("\t\t,0");
            stringBuilder.AppendLine($"\t\t,{PostgreSqlObjectNames.TimestampValue}");
            stringBuilder.AppendLine("\t\t,0");
            stringBuilder.AppendLine("\t\t,now()::timestamp");

            if (Filters != null && Filters.Count > 0)
                stringBuilder.AppendLine(filterColumnsString.ToString());

            stringBuilder.AppendLine("\t)");
            stringBuilder.AppendLine($"ON CONFLICT ({string.Join(", ", pkColumns)}) DO UPDATE SET");
            stringBuilder.AppendLine("\t\"sync_row_is_tombstone\" = 0, ");
            stringBuilder.AppendLine("\t\"create_scope_id\" = NULL, ");
            stringBuilder.AppendLine("\t\"update_scope_id\" = NULL, ");
            stringBuilder.AppendLine($"\t\"create_timestamp\" = {PostgreSqlObjectNames.TimestampValue}, ");
            stringBuilder.AppendLine("\t\"update_timestamp\" = NULL, ");
            stringBuilder.AppendLine($"\t\"timestamp\" = {PostgreSqlObjectNames.TimestampValue}, ");
            stringBuilder.AppendLine("\t\"last_change_datetime\" = now()::timestamp");

            if (Filters != null && Filters.Count > 0)
                stringBuilder.AppendLine(filterColumnsString.ToString());

            stringBuilder.Append(";");
            stringBuilder.AppendLine("RETURN NEW;");
            stringBuilder.AppendLine("END");
            return stringBuilder.ToString();
        }

        private string CreateInsertProcedureCommand(string procedureName)
        {
            return $@"CREATE OR REPLACE FUNCTION {procedureName}()
                        RETURNS trigger 
                        LANGUAGE 'plpgsql'
                        COST 100 
                        SET search_path='{_schemaName}'
                        VOLATILE NOT LEAKPROOF SECURITY DEFINER 
                      AS $BODY$
                        {InsertTriggerBodyText()};
                      $BODY$;";
        }

        private string CreateUpdateProcedureCommand(string procedureName)
        {
            return $@"CREATE OR REPLACE FUNCTION {procedureName}()
                        RETURNS trigger 
                        LANGUAGE 'plpgsql'
                        COST 100 
                        SET search_path='{_schemaName}'
                        VOLATILE NOT LEAKPROOF SECURITY DEFINER 
                      AS $BODY$
                        {UpdateTriggerBodyText()}
                      $BODY$;";
        }

        private string UpdateTriggerBodyText()
        {
            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"Begin ");
            stringBuilder.AppendLine($"\tUPDATE {_trackingName.QuotedString} ");
            stringBuilder.AppendLine("\tSET \"update_scope_id\" = NULL -- since the update if from local, it's a NULL");
            stringBuilder.AppendLine($"\t\t,\"update_timestamp\" = {PostgreSqlObjectNames.TimestampValue}");
            stringBuilder.AppendLine($"\t\t,\"timestamp\" = {PostgreSqlObjectNames.TimestampValue}");
            stringBuilder.AppendLine("\t\t,\"last_change_datetime\" = now()");

            if (Filters != null && Filters.Count > 0)
            {
                for (var i = 0; i < Filters.Count; i++)
                {
                    var filterColumn = Filters[i];

                    if (_tableDescription.PrimaryKey.Columns.Any(c =>
                        c.ColumnName.ToLowerInvariant() == filterColumn.ColumnName.ToLowerInvariant()))
                        continue;

                    var columnName = new ObjectNameParser(filterColumn.ColumnName.ToLowerInvariant(), "\"", "\"");
                    stringBuilder.AppendLine($"\t,{columnName.QuotedString} = \"i\".{columnName.QuotedString}");
                }

                stringBuilder.AppendLine();
            }

            stringBuilder.Append($"\tWhere ");
            stringBuilder.Append(PostgreSqlManagementUtils.JoinTwoTablesOnClause(_tableDescription.PrimaryKey.Columns,
                _trackingName.QuotedString, "new"));
            stringBuilder.AppendLine($"; ");
            stringBuilder.AppendLine("RETURN NEW;");
            stringBuilder.AppendLine($"End; ");
            return stringBuilder.ToString();
        }

        public void DropTrigger(DbCommandType triggerType)
        {
            var triggerName = string.Format(_postgreSqlObjectNames.GetCommandName(triggerType),
                _tableName.UnquotedStringWithUnderScore);
            var commandText = $"drop trigger if exists {triggerName}";

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
                Debug.WriteLine($"Error during DropTriggerCommand : {ex}");
                throw;
            }
            finally
            {
                if (!alreadyOpened && _connection.State != ConnectionState.Closed)
                    _connection.Close();
            }
        }

        private string DropTriggerText(DbCommandType triggerType)
        {
            var commandName = _postgreSqlObjectNames.GetCommandName(triggerType);
            var commandText = $"drop trigger if exists {commandName}";

            var str1 = $"Drop trigger {commandName} for table {_tableName.QuotedString}";
            return PostgreSqlBuilder.WrapScriptTextWithComments(commandText, str1);
        }
    }
}