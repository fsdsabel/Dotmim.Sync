using System;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Data;
using Npgsql;

namespace Dotmim.Sync.PostgreSql.Builders
{
    public class PostgreSqlBuilderTable : IDbBuilderTableHelper
    {
        private ObjectNameParser tableName;
        private ObjectNameParser trackingName;
        private DmTable tableDescription;
        private NpgsqlConnection connection;
        private NpgsqlTransaction transaction;
        private PostgreSqlDbMetadata mySqlDbMetadata;

        public PostgreSqlBuilderTable(DmTable tableDescription, DbConnection connection, DbTransaction transaction = null)
        {

            this.connection = connection as NpgsqlConnection;
            this.transaction = transaction as NpgsqlTransaction;
            this.tableDescription = tableDescription;
            (this.tableName, this.trackingName) = PostgreSqlBuilder.GetParsers(this.tableDescription);
            this.mySqlDbMetadata = new PostgreSqlDbMetadata();
        }


        private NpgsqlCommand BuildForeignKeyConstraintsCommand(DmRelation foreignKey)
        {
            NpgsqlCommand sqlCommand = new NpgsqlCommand();

            var childTable = foreignKey.ChildTable;
            var childTableName = new ObjectNameParser(childTable.TableName.ToLowerInvariant(), "\"", "\"");
            var parentTable = foreignKey.ParentTable;
            var parentTableName = new ObjectNameParser(parentTable.TableName.ToLowerInvariant(), "\"", "\""); ;

            var relationName = foreignKey.RelationName.Length > 50 ? foreignKey.RelationName.Substring(0, 50) : foreignKey.RelationName;

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.Append("ALTER TABLE ");
            stringBuilder.AppendLine(parentTableName.QuotedString);
            stringBuilder.Append("ADD CONSTRAINT ");
            stringBuilder.AppendLine(relationName);
            stringBuilder.Append("FOREIGN KEY (");
            string empty = string.Empty;
            foreach (var parentdColumn in foreignKey.ParentColumns)
            {
                var parentColumnName = new ObjectNameParser(parentdColumn.ColumnName.ToLowerInvariant(), "\"", "\"");

                stringBuilder.Append($"{empty} {parentColumnName.QuotedString}");
                empty = ", ";
            }
            stringBuilder.AppendLine(" )");
            stringBuilder.Append("REFERENCES ");
            stringBuilder.Append(childTableName.QuotedString).Append(" (");
            empty = string.Empty;
            foreach (var childColumn in foreignKey.ChildColumns)
            {
                var childColumnName = new ObjectNameParser(childColumn.ColumnName.ToLowerInvariant(), "\"", "\"");
                stringBuilder.Append($"{empty} {childColumnName.QuotedString}");
            }
            stringBuilder.Append(" ) ");
            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }

        public bool NeedToCreateForeignKeyConstraints(DmRelation foreignKey)
        {
            string parentTable = foreignKey.ParentTable.TableName;
            string parentSchema = foreignKey.ParentTable.Schema;
            string parentFullName = String.IsNullOrEmpty(parentSchema) ? parentTable : $"{parentSchema}.{parentTable}";

            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    connection.Open();

                var dmTable = PostgreSqlManagementUtils.RelationsForTable(connection, transaction, parentFullName);

                var foreignKeyExist = dmTable.Rows.Any(r =>
                   dmTable.IsEqual(r["ForeignKey"].ToString(), foreignKey.RelationName));

                return !foreignKeyExist;
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during checking foreign keys: {ex}");
                throw;
            }
            finally
            {
                if (!alreadyOpened && connection.State != ConnectionState.Closed)
                    connection.Close();
            }
        }


        public void CreateForeignKeyConstraints(DmRelation constraint)
        {
            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    connection.Open();

                using (var command = BuildForeignKeyConstraintsCommand(constraint))
                {
                    command.Connection = connection;

                    if (transaction != null)
                        command.Transaction = transaction;

                    command.ExecuteNonQuery();
                }

            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during CreateForeignKeyConstraints : {ex}");
                throw;

            }
            finally
            {
                if (!alreadyOpened && connection.State != ConnectionState.Closed)
                    connection.Close();

            }

        }

        public string CreateForeignKeyConstraintsScriptText(DmRelation constraint)
        {
            StringBuilder stringBuilder = new StringBuilder();

            var constraintName = $"Create Constraint {constraint.RelationName} between parent {constraint.ParentTable.TableName.ToLowerInvariant()} and child {constraint.ChildTable.TableName.ToLowerInvariant()}";
            var constraintScript = BuildForeignKeyConstraintsCommand(constraint).CommandText;
            stringBuilder.Append(PostgreSqlBuilder.WrapScriptTextWithComments(constraintScript, constraintName));
            stringBuilder.AppendLine();

            return stringBuilder.ToString();
        }


        public void CreatePrimaryKey()
        {
            return;

        }
        public string CreatePrimaryKeyScriptText()
        {
            return string.Empty;
        }


        private NpgsqlCommand BuildTableCommand()
        {
            NpgsqlCommand command = new NpgsqlCommand();

            StringBuilder stringBuilder = new StringBuilder($"CREATE TABLE IF NOT EXISTS {tableName.QuotedString} (");
            string empty = string.Empty;
            stringBuilder.AppendLine();
            foreach (var column in this.tableDescription.Columns)
            {
                var columnName = new ObjectNameParser(column.ColumnName.ToLowerInvariant(), "\"", "\"");
                var stringType = this.mySqlDbMetadata.TryGetOwnerDbTypeString(column.OriginalDbType, column.DbType, false, false, this.tableDescription.OriginalProvider, PostgreSqlSyncProvider.ProviderType);
                var stringPrecision = this.mySqlDbMetadata.TryGetOwnerDbTypePrecision(column.OriginalDbType, column.DbType, false, false, column.MaxLength, column.Precision, column.Scale, this.tableDescription.OriginalProvider, PostgreSqlSyncProvider.ProviderType);
                var columnType = $"{stringType} {stringPrecision}";

                var identity = string.Empty;

                if (column.AutoIncrement)
                {
                    var s = column.GetAutoIncrementSeedAndStep();
                    if (s.Seed > 1 || s.Step > 1)
                        throw new NotSupportedException("can't establish a seed / step in MySql autoinc value");

                    identity = $"AUTO_INCREMENT";
                }
                var nullString = column.AllowDBNull ? "NULL" : "NOT NULL";

                // if we have a readonly column, we may have a computed one, so we need to allow null
                if (column.ReadOnly)
                    nullString = "NULL";

                stringBuilder.AppendLine($"\t{empty}{columnName.QuotedString} {columnType} {identity} {nullString}");
                empty = ",";
            }
            stringBuilder.Append("\t,PRIMARY KEY (");

            int i = 0;
            // It seems we need to specify the increment column in first place
            foreach (var pkColumn in this.tableDescription.PrimaryKey.Columns.OrderByDescending(pk => pk.AutoIncrement))
            {
                var quotedColumnName = new ObjectNameParser(pkColumn.ColumnName.ToLowerInvariant(), "\"", "\"").QuotedObjectName;

                stringBuilder.Append(quotedColumnName);

                if (i < this.tableDescription.PrimaryKey.Columns.Length - 1)
                    stringBuilder.Append(", ");
                i++;
            }

            //for (int i = 0; i < this.tableDescription.PrimaryKey.Columns.Length; i++)
            //{
            //    DmColumn pkColumn = this.tableDescription.PrimaryKey.Columns[i];
            //    var quotedColumnName = new ObjectNameParser(pkColumn.ColumnName.ToLowerInvariant(), "\"", "\"").QuotedObjectName;

            //    stringBuilder.Append(quotedColumnName);

            //    if (i < this.tableDescription.PrimaryKey.Columns.Length - 1)
            //        stringBuilder.Append(", ");
            //}
            stringBuilder.Append(")");
            stringBuilder.Append(")");
            return new NpgsqlCommand(stringBuilder.ToString());
        }

        public void CreateTable()
        {
            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                using (var command = BuildTableCommand())
                {
                    if (!alreadyOpened)
                        connection.Open();

                    if (transaction != null)
                        command.Transaction = transaction;

                    command.Connection = connection;
                    command.ExecuteNonQuery();

                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during CreateTable : {ex}");
                throw;

            }
            finally
            {
                if (!alreadyOpened && connection.State != ConnectionState.Closed)
                    connection.Close();

            }

        }
        public string CreateTableScriptText()
        {
            StringBuilder stringBuilder = new StringBuilder();
            var tableNameScript = $"Create Table {tableName.QuotedString}";
            var tableScript = BuildTableCommand().CommandText;
            stringBuilder.Append(PostgreSqlBuilder.WrapScriptTextWithComments(tableScript, tableNameScript));
            stringBuilder.AppendLine();
            return stringBuilder.ToString();
        }


        /// <summary>
        /// For a foreign key, check if the Parent table exists
        /// </summary>
        private bool EnsureForeignKeysTableExist(DmRelation foreignKey)
        {
            var childTable = foreignKey.ChildTable;
            var parentTable = foreignKey.ParentTable;

            // The foreignkey comes from the child table
            var ds = foreignKey.ChildTable.DmSet;

            if (ds == null)
                return false;

            // Check if the parent table is part of the sync configuration
            var exist = ds.Tables.Any(t => ds.IsEqual(t.TableName.ToLowerInvariant(), parentTable.TableName.ToLowerInvariant()));

            if (!exist)
                return false;

            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    connection.Open();

                return PostgreSqlManagementUtils.TableExists(connection, transaction, parentTable.TableName.ToLowerInvariant());

            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during EnsureForeignKeysTableExist : {ex}");
                throw;

            }
            finally
            {
                if (!alreadyOpened && connection.State != ConnectionState.Closed)
                    connection.Close();

            }


        }

        /// <summary>
        /// Check if we need to create the table in the current database
        /// </summary>
        public bool NeedToCreateTable()
        {
            return !PostgreSqlManagementUtils.TableExists(connection, transaction, tableName.UnquotedString);

        }

        public bool NeedToCreateSchema()
        {
            return false;
        }

        public void CreateSchema()
        {
            return;
        }

        public string CreateSchemaScriptText()
        {
            return string.Empty;
        }

        public void DropTable()
        {
            var commandText = $"drop table if exists {tableName.QuotedString}";

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
                Debug.WriteLine($"Error during DropTableCommand : {ex}");
                throw;
            }
            finally
            {
                if (!alreadyOpened && connection.State != ConnectionState.Closed)
                    connection.Close();

            }

        }

        public string DropTableScriptText()
        {
            var commandText = $"drop table if exists {tableName.QuotedString}";

            var str1 = $"Drop table {tableName.QuotedString}";
            return PostgreSqlBuilder.WrapScriptTextWithComments(commandText, str1);
        }
    }
}
