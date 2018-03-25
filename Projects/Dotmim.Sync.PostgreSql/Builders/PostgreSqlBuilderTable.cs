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
        private readonly NpgsqlConnection _connection;
        private readonly PostgreSqlDbMetadata _mySqlDbMetadata;
        private readonly DmTable _tableDescription;
        private readonly ObjectNameParser _tableName;
        private readonly NpgsqlTransaction _transaction;

        public PostgreSqlBuilderTable(DmTable tableDescription, DbConnection connection,
            DbTransaction transaction = null)
        {
            _connection = connection as NpgsqlConnection;
            _transaction = transaction as NpgsqlTransaction;
            _tableDescription = tableDescription;
            (_tableName, _) = PostgreSqlBuilder.GetParsers(_tableDescription);
            _mySqlDbMetadata = new PostgreSqlDbMetadata();
        }

        public bool NeedToCreateForeignKeyConstraints(DmRelation foreignKey)
        {
            var parentTable = foreignKey.ParentTable.TableName;
            var parentSchema = foreignKey.ParentTable.Schema;
            var parentFullName = string.IsNullOrEmpty(parentSchema) ? parentTable : $"{parentSchema}.{parentTable}";

            var alreadyOpened = _connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    _connection.Open();

                var dmTable = PostgreSqlManagementUtils.RelationsForTable(_connection, _transaction, parentFullName);

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
                if (!alreadyOpened && _connection.State != ConnectionState.Closed)
                    _connection.Close();
            }
        }


        public void CreateForeignKeyConstraints(DmRelation constraint)
        {
            var alreadyOpened = _connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    _connection.Open();

                using (var command = BuildForeignKeyConstraintsCommand(constraint))
                {
                    command.Connection = _connection;

                    if (_transaction != null)
                        command.Transaction = _transaction;

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
                if (!alreadyOpened && _connection.State != ConnectionState.Closed)
                    _connection.Close();
            }
        }

        public string CreateForeignKeyConstraintsScriptText(DmRelation constraint)
        {
            var stringBuilder = new StringBuilder();

            var constraintName =
                $"Create Constraint {constraint.RelationName} between parent {constraint.ParentTable.TableName.ToLowerInvariant()} and child {constraint.ChildTable.TableName.ToLowerInvariant()}";
            var constraintScript = BuildForeignKeyConstraintsCommand(constraint).CommandText;
            stringBuilder.Append(PostgreSqlBuilder.WrapScriptTextWithComments(constraintScript, constraintName));
            stringBuilder.AppendLine();

            return stringBuilder.ToString();
        }


        public void CreatePrimaryKey()
        {
        }

        public string CreatePrimaryKeyScriptText()
        {
            return string.Empty;
        }

        public void CreateTable()
        {
            var alreadyOpened = _connection.State == ConnectionState.Open;

            try
            {
                using (var command = BuildTableCommand())
                {
                    if (!alreadyOpened)
                        _connection.Open();

                    if (_transaction != null)
                        command.Transaction = _transaction;

                    command.Connection = _connection;
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
                if (!alreadyOpened && _connection.State != ConnectionState.Closed)
                    _connection.Close();
            }
        }

        public string CreateTableScriptText()
        {
            var stringBuilder = new StringBuilder();
            var tableNameScript = $"Create Table {_tableName.QuotedString}";
            var tableScript = BuildTableCommand().CommandText;
            stringBuilder.Append(PostgreSqlBuilder.WrapScriptTextWithComments(tableScript, tableNameScript));
            stringBuilder.AppendLine();
            return stringBuilder.ToString();
        }

        /// <summary>
        ///     Check if we need to create the table in the current database
        /// </summary>
        public bool NeedToCreateTable()
        {
            return !PostgreSqlManagementUtils.TableExists(_connection, _transaction, _tableName.UnquotedString);
        }

        public bool NeedToCreateSchema()
        {
            return false;
        }

        public void CreateSchema()
        {
        }

        public string CreateSchemaScriptText()
        {
            return string.Empty;
        }

        public void DropTable()
        {
            var commandText = $"drop table if exists {_tableName.QuotedString}";

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
            var commandText = $"drop table if exists {_tableName.QuotedString}";

            var str1 = $"Drop table {_tableName.QuotedString}";
            return PostgreSqlBuilder.WrapScriptTextWithComments(commandText, str1);
        }


        private NpgsqlCommand BuildForeignKeyConstraintsCommand(DmRelation foreignKey)
        {
            var sqlCommand = new NpgsqlCommand();

            var childTable = foreignKey.ChildTable;
            var childTableName = new ObjectNameParser(childTable.TableName.ToLowerInvariant(), "\"", "\"");
            var parentTable = foreignKey.ParentTable;
            var parentTableName = new ObjectNameParser(parentTable.TableName.ToLowerInvariant(), "\"", "\"");
            

            var relationName = foreignKey.RelationName.Length > 50
                ? foreignKey.RelationName.Substring(0, 50)
                : foreignKey.RelationName;

            var stringBuilder = new StringBuilder();
            stringBuilder.Append("ALTER TABLE ");
            stringBuilder.AppendLine(parentTableName.QuotedString);
            stringBuilder.Append("ADD CONSTRAINT ");
            stringBuilder.AppendLine(relationName);
            stringBuilder.Append("FOREIGN KEY (");
            var empty = string.Empty;
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


        private NpgsqlCommand BuildTableCommand()
        {
            var stringBuilder = new StringBuilder($"CREATE TABLE IF NOT EXISTS {_tableName.QuotedString} (");
            var empty = string.Empty;
            stringBuilder.AppendLine();
            foreach (var column in _tableDescription.Columns)
            {
                var columnName = new ObjectNameParser(column.ColumnName.ToLowerInvariant(), "\"", "\"");
                var stringType = _mySqlDbMetadata.TryGetOwnerDbTypeString(column.OriginalDbType, column.DbType, false,
                    false, _tableDescription.OriginalProvider, PostgreSqlSyncProvider.ProviderType);
                var stringPrecision = _mySqlDbMetadata.TryGetOwnerDbTypePrecision(column.OriginalDbType, column.DbType,
                    false, false, column.MaxLength, column.Precision, column.Scale, _tableDescription.OriginalProvider,
                    PostgreSqlSyncProvider.ProviderType);
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

            var i = 0;
            // It seems we need to specify the increment column in first place
            foreach (var pkColumn in _tableDescription.PrimaryKey.Columns.OrderByDescending(pk => pk.AutoIncrement))
            {
                var quotedColumnName = new ObjectNameParser(pkColumn.ColumnName.ToLowerInvariant(), "\"", "\"")
                    .QuotedObjectName;

                stringBuilder.Append(quotedColumnName);

                if (i < _tableDescription.PrimaryKey.Columns.Length - 1)
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
    }
}