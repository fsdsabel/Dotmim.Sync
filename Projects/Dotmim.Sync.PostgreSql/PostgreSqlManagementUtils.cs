using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Data;
using Dotmim.Sync.PostgreSql.Builders;
using Npgsql;

namespace Dotmim.Sync.PostgreSql
{
    internal static class PostgreSqlManagementUtils
    {

        internal static DmTable ColumnsForTable(NpgsqlConnection connection, NpgsqlTransaction transaction, string tableName)
        {
            string commandColumn = "select * from information_schema.columns WHERE table_schema = current_schema() and table_name = @tableName";

            ObjectNameParser tableNameParser = new ObjectNameParser(tableName, "\"", "\"");
            DmTable dmTable = new DmTable(tableNameParser.UnquotedStringWithUnderScore);
            using (var sqlCommand = new NpgsqlCommand(commandColumn, connection, transaction))
            {
                sqlCommand.Parameters.AddWithValue("@tableName", tableNameParser.ObjectName);

                using (var reader = sqlCommand.ExecuteReader())
                {
                    dmTable.Fill(reader);
                }
            }
            return dmTable;
        }

        internal static DmTable PrimaryKeysForTable(NpgsqlConnection connection, NpgsqlTransaction transaction, string tableName)
        {
            var commandColumn = @"SELECT c.column_name, c.ordinal_position, *
                                  FROM information_schema.key_column_usage AS c
                                  LEFT JOIN information_schema.table_constraints AS t
                                  ON t.constraint_name = c.constraint_name
                                  WHERE t.table_schema = current_schema() AND t.table_name = @tableName AND t.constraint_type = 'PRIMARY KEY';";

            ObjectNameParser tableNameParser = new ObjectNameParser(tableName, "\"", "\"");
            DmTable dmTable = new DmTable(tableNameParser.UnquotedStringWithUnderScore);
            using (var sqlCommand = new NpgsqlCommand(commandColumn, connection, transaction))
            {
                sqlCommand.Parameters.AddWithValue("@tableName", tableNameParser.ObjectName);

                using (var reader = sqlCommand.ExecuteReader())
                {
                    dmTable.Fill(reader);
                }
            }
            return dmTable;
        }

        internal static DmTable RelationsForTable(NpgsqlConnection connection, NpgsqlTransaction transaction, string tableName)
        {
            var commandRelations = @"select 
                    con.constraint_name as ""foreignKey"",
                    att2.attname as ""columnName"", 
                    cl.relname as ""referenceTableName"", 
                    att.attname as ""referenceColumnName"",
                    con.child_table as ""tableName"",
                    con.child_schema
                    from
                    (select
                        unnest(con1.conkey) as ""parent"",
                    unnest(con1.confkey) as ""child"",
                    con1.conname as constraint_name,
                    con1.confrelid,
                    con1.conrelid,
                    cl.relname as child_table,
                    ns.nspname as child_schema
                    from
                        pg_class cl
                        join pg_namespace ns on cl.relnamespace = ns.oid
                    join pg_constraint con1 on con1.conrelid = cl.oid
                    where con1.contype = 'f'
                        ) con
                        join pg_attribute att on
                    att.attrelid = con.confrelid and att.attnum = con.child
                    join pg_class cl on
                    cl.oid = con.confrelid
                    join pg_attribute att2 on
                    att2.attrelid = con.conrelid and att2.attnum = con.parent
                WHERE con.child_table = @tableName AND con.child_schema = current_schema()";

            ObjectNameParser tableNameParser = new ObjectNameParser(tableName, "\"", "\"");
            DmTable dmTable = new DmTable(tableNameParser.UnquotedStringWithUnderScore);
            using (var sqlCommand = new NpgsqlCommand(commandRelations, connection, transaction))
            {
                sqlCommand.Parameters.AddWithValue("@tableName", tableNameParser.ObjectName);

                using (var reader = sqlCommand.ExecuteReader())
                {
                    dmTable.Fill(reader);
                }
            }


            return dmTable;

        }
        /*
        public static void DropTableIfExists(NpgsqlConnection connection, NpgsqlTransaction transaction, string quotedTableName)
        {
            ObjectNameParser objectNameParser = new ObjectNameParser(quotedTableName, "\"", "\"");

            using (var dbCommand = connection.CreateCommand())
            {
                dbCommand.CommandText = $"select * from information_schema.TABLES where table_schema = schema() and table_name = @tableName";

                dbCommand.Parameters.AddWithValue("@tableName", objectNameParser.ObjectName);
               
                if (transaction != null)
                    dbCommand.Transaction = transaction;

                dbCommand.ExecuteNonQuery();
            }
        }

        public static string DropTableIfExistsScriptText(string quotedTableName)
        {
            ObjectNameParser objectNameParser = new ObjectNameParser(quotedTableName, "\"", "\"");

            return $"drop table if exist {objectNameParser.ObjectName}";
        }*/

        internal static bool IsStringNullOrWhitespace(string value)
        {
            return Regex.Match(value ?? string.Empty, "^\\s*$").Success;
        }
        public static string DropTableScriptText(string quotedTableName)
        {
            ObjectNameParser objectNameParser = new ObjectNameParser(quotedTableName, "\"", "\"");

            return $"drop table {objectNameParser.ObjectName}";
        }

        public static void DropTriggerIfExists(NpgsqlConnection connection, NpgsqlTransaction transaction, string quotedTriggerName)
        {
            ObjectNameParser objectNameParser = new ObjectNameParser(quotedTriggerName, "\"", "\"");

            using (DbCommand dbCommand = connection.CreateCommand())
            {
                dbCommand.CommandText = $"drop trigger {objectNameParser.ObjectName}";
                if (transaction != null)
                    dbCommand.Transaction = transaction;
             
                dbCommand.ExecuteNonQuery();
            }
        }

        public static string DropTriggerScriptText(string quotedTriggerName)
        {
            ObjectNameParser objectNameParser = new ObjectNameParser(quotedTriggerName, "\"", "\"");
            return $"drop trigger {objectNameParser.ObjectName}";
        }

        public static bool TableExists(NpgsqlConnection connection, NpgsqlTransaction transaction, string quotedTableName)
        {
            bool tableExist;
            ObjectNameParser tableNameParser = new ObjectNameParser(quotedTableName, "\"", "\"");
            using (DbCommand dbCommand = connection.CreateCommand())
            {
                dbCommand.CommandText =
                    "select COUNT(*) from pg_catalog.pg_tables WHERE tablename = @tableName AND schemaname = current_schema()";

                if (transaction != null)
                    dbCommand.Transaction = transaction;

                var sqlParameter = new NpgsqlParameter()
                {
                    ParameterName = "@tableName",
                    Value = tableNameParser.UnquotedString
                };

                dbCommand.Parameters.Add(sqlParameter);

                tableExist = (Int64)dbCommand.ExecuteScalar() != 0;

            }
            return tableExist;
        }

        public static bool TriggerExists(NpgsqlConnection connection, NpgsqlTransaction transaction, string quotedTriggerName)
        {
            bool triggerExist;
            ObjectNameParser objectNameParser = new ObjectNameParser(quotedTriggerName, "\"", "\"");


            using (var dbCommand = connection.CreateCommand())
            {
                dbCommand.CommandText = "SELECT COUNT(*) FROM \"information_schema\".\"triggers\" WHERE trigger_schema = current_schema() AND trigger_name = @triggerName";

                dbCommand.Parameters.AddWithValue("@triggerName", objectNameParser.ObjectName);

                if (transaction != null)
                    dbCommand.Transaction = transaction;

                triggerExist = (long)dbCommand.ExecuteScalar() != 0L;
            }
            return triggerExist;
        }

        internal static string JoinTwoTablesOnClause(IEnumerable<DmColumn> columns, string leftName, string rightName)
        {
            StringBuilder stringBuilder = new StringBuilder();
            string strRightName = (string.IsNullOrEmpty(rightName) ? string.Empty : string.Concat(rightName, "."));
            string strLeftName = (string.IsNullOrEmpty(leftName) ? string.Empty : string.Concat(leftName, "."));

            string str = "";
            foreach (DmColumn column in columns)
            {
                ObjectNameParser quotedColumn = new ObjectNameParser(column.ColumnName, "\"", "\"");

                stringBuilder.Append(str);
                stringBuilder.Append(strLeftName);
                stringBuilder.Append(quotedColumn.QuotedString);
                stringBuilder.Append(" = ");
                stringBuilder.Append(strRightName);
                stringBuilder.Append(quotedColumn.QuotedString);

                str = " AND ";
            }
            return stringBuilder.ToString();
        }

        internal static string WhereColumnAndParameters(IEnumerable<DmColumn> columns, string fromPrefix)
        {
            StringBuilder stringBuilder = new StringBuilder();
            string strFromPrefix = (string.IsNullOrEmpty(fromPrefix) ? string.Empty : string.Concat(fromPrefix, "."));
            string str1 = "";
            foreach (DmColumn column in columns)
            {
                ObjectNameParser quotedColumn = new ObjectNameParser(column.ColumnName,"\"", "\"");

                stringBuilder.Append(str1);
                stringBuilder.Append(strFromPrefix);
                stringBuilder.Append(quotedColumn.QuotedString);
                stringBuilder.Append(" = ");
                stringBuilder.Append($"in_{column.ColumnName.ToLowerInvariant()}");
                str1 = " AND ";
            }
            return stringBuilder.ToString();
        }

        internal static string CommaSeparatedUpdateFromParameters(DmTable table, string fromPrefix = "")
        {
            StringBuilder stringBuilder = new StringBuilder();
            string strFromPrefix = (string.IsNullOrEmpty(fromPrefix) ? string.Empty : string.Concat(fromPrefix, "."));
            string strSeparator = "";
            foreach (DmColumn column in table.NonPkColumns.Where(c => !c.ReadOnly))
            {
                ObjectNameParser quotedColumn = new ObjectNameParser(column.ColumnName, "\"", "\"");
                stringBuilder.AppendLine($"{strSeparator} {strFromPrefix}{quotedColumn.QuotedString} = in_{quotedColumn.UnquotedString.ToLowerInvariant()}");
                strSeparator = ", ";
            }
            return stringBuilder.ToString();

        }

        internal static bool ProcedureExists(NpgsqlConnection connection, NpgsqlTransaction transaction, string commandName)
        {
            bool procExist;
            ObjectNameParser objectNameParser = new ObjectNameParser(commandName, "\"", "\"");


            using (var dbCommand = connection.CreateCommand())
            {
                dbCommand.CommandText = @"select count(*) from information_schema.ROUTINES
                                        where ROUTINE_TYPE = 'FUNCTION'
                                        and ROUTINE_SCHEMA = current_schema()
                                        and ROUTINE_NAME = @procName";

                dbCommand.Parameters.AddWithValue("@procName", objectNameParser.ObjectName);

                if (transaction != null)
                    dbCommand.Transaction = transaction;

                procExist = (long)dbCommand.ExecuteScalar() != 0L;
            }
            return procExist;
        }

        internal static string ColumnsAndParameters(IEnumerable<DmColumn> columns, string fromPrefix)
        {
            StringBuilder stringBuilder = new StringBuilder();
            string strFromPrefix = (string.IsNullOrEmpty(fromPrefix) ? string.Empty : string.Concat(fromPrefix, "."));
            string str1 = "";
            foreach (DmColumn column in columns)
            {
                ObjectNameParser quotedColumn = new ObjectNameParser(column.ColumnName, "\"", "\"");

                stringBuilder.Append(str1);
                stringBuilder.Append(strFromPrefix);
                stringBuilder.Append(quotedColumn.QuotedString);
                stringBuilder.Append(" = ");
                stringBuilder.Append($"{PostgreSqlBuilderProcedure.PGSQL_PREFIX_PARAMETER}{column.ColumnName}");
                str1 = " AND ";
            }
            return stringBuilder.ToString();
        }
    }
}
