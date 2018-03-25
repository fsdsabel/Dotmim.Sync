using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Manager;
using Npgsql;

namespace Dotmim.Sync.PostgreSql.Builders
{
    public class PostgreSqlScopeInfoBuilder : IDbScopeInfoBuilder
    {
        private NpgsqlConnection connection;
        private NpgsqlTransaction transaction;

        public PostgreSqlScopeInfoBuilder(DbConnection connection, DbTransaction transaction = null)
        {
            this.connection = connection as NpgsqlConnection;
            this.transaction = transaction as NpgsqlTransaction;
        }



        public void CreateScopeInfoTable()
        {
            var command = connection.CreateCommand();
            if (transaction != null)
                command.Transaction = transaction;
            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    connection.Open();

                command.CommandText =
                    @"CREATE TABLE scope_info(
                        sync_scope_id UUID NOT NULL,
	                    sync_scope_name varchar(100) NOT NULL,
	                    scope_timestamp int8 NULL,
                        scope_is_local boolean NOT NULL DEFAULT FALSE, 
                        scope_last_sync timestamp NULL,
                        CONSTRAINT ""PK_scope_info""  PRIMARY KEY (sync_scope_id)
                        )";
                command.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during CreateTableScope : {ex}");
                throw;
            }
            finally
            {
                if (!alreadyOpened && connection.State != ConnectionState.Closed)
                    connection.Close();

                if (command != null)
                    command.Dispose();
            }
        }

        public void DropScopeInfoTable()
        {
            var command = connection.CreateCommand();

            if (transaction != null)
                command.Transaction = transaction;

            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    connection.Open();

                command.CommandText = "drop table if exists scope_info";

                command.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during DropScopeInfoTable : {ex}");
                throw;
            }
            finally
            {
                if (!alreadyOpened && connection.State != ConnectionState.Closed)
                    connection.Close();

                if (command != null)
                    command.Dispose();
            }
        }


        public List<ScopeInfo> GetAllScopes(string scopeName)
        {
            var command = connection.CreateCommand();
            if (transaction != null)
                command.Transaction = transaction;

            bool alreadyOpened = connection.State == ConnectionState.Open;

            List<ScopeInfo> scopes = new List<ScopeInfo>();
            try
            {
                if (!alreadyOpened)
                    connection.Open();

                command.CommandText =
                    @"SELECT sync_scope_id
                           , sync_scope_name
                           , scope_timestamp
                           , scope_is_local
                           , scope_last_sync
                    FROM  scope_info
                    WHERE sync_scope_name = @sync_scope_name";

                var p = command.CreateParameter();
                p.ParameterName = "@sync_scope_name";
                p.Value = scopeName;
                p.DbType = DbType.String;
                command.Parameters.Add(p);

                using (DbDataReader reader = command.ExecuteReader())
                {
                    // read only the first one
                    while (reader.Read())
                    {
                        ScopeInfo scopeInfo = new ScopeInfo();
                        scopeInfo.Name = reader["sync_scope_name"] as String;
                        scopeInfo.Id = (Guid)reader["sync_scope_id"];
                        scopeInfo.LastTimestamp = DbManager.ParseTimestamp(reader["scope_timestamp"]);
                        scopeInfo.LastSync = reader["scope_last_sync"] != DBNull.Value ? (DateTime?)reader["scope_last_sync"] : null;
                        scopeInfo.IsLocal = reader.GetBoolean(reader.GetOrdinal("scope_is_local"));
                        scopes.Add(scopeInfo);
                    }
                }

                return scopes;
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during GetAllScopes : {ex}");
                throw;
            }
            finally
            {
                if (!alreadyOpened && connection.State != ConnectionState.Closed)
                    connection.Close();

                if (command != null)
                    command.Dispose();
            }
        }

        public long GetLocalTimestamp()
        {
            var command = connection.CreateCommand();
            if (transaction != null)
                command.Transaction = transaction;

            bool alreadyOpened = connection.State == ConnectionState.Open;
            try
            {
                command.CommandText = $"Select {PostgreSqlObjectNames.TimestampValue}";

                if (!alreadyOpened)
                    connection.Open();

                long result = Convert.ToInt64(command.ExecuteScalar());

                return result;
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during GetLocalTimestamp : {ex}");
                throw;

            }
            finally
            {
                if (!alreadyOpened && connection.State != ConnectionState.Closed)
                    connection.Close();

                if (command != null)
                    command.Dispose();
            }
        }

        public ScopeInfo InsertOrUpdateScopeInfo(ScopeInfo scopeInfo)
        {
            bool alreadyOpened = connection.State == ConnectionState.Open;
            bool exist;
            try
            {
                using (var command = connection.CreateCommand())
                {
                    if (transaction != null)
                        command.Transaction = transaction;


                    if (!alreadyOpened)
                        connection.Open();

                    command.CommandText = @"Select count(*) from scope_info where sync_scope_id = @sync_scope_id";

                    var p = command.CreateParameter();
                    p.ParameterName = "@sync_scope_id";
                    p.Value = scopeInfo.Id;
                    p.DbType = DbType.Guid;
                    command.Parameters.Add(p);

                    exist = (long)command.ExecuteScalar() > 0;

                }

                string stmtText = exist
                    ? $"Update scope_info set sync_scope_name=@sync_scope_name, scope_timestamp={PostgreSqlObjectNames.TimestampValue}, scope_is_local=@scope_is_local, scope_last_sync=@scope_last_sync where sync_scope_id=@sync_scope_id"
                    : $"Insert into scope_info (sync_scope_name, scope_timestamp, scope_is_local, scope_last_sync, sync_scope_id) values (@sync_scope_name, {PostgreSqlObjectNames.TimestampValue}, @scope_is_local, @scope_last_sync, @sync_scope_id)";

                using (var command = connection.CreateCommand())
                {
                    if (transaction != null)
                        command.Transaction = transaction;

                    command.CommandText = stmtText;

                    var p = command.CreateParameter();
                    p.ParameterName = "@sync_scope_name";
                    p.Value = scopeInfo.Name;
                    p.DbType = DbType.String;
                    command.Parameters.Add(p);

                    p = command.CreateParameter();
                    p.ParameterName = "@scope_is_local";
                    p.Value = scopeInfo.IsLocal;
                    p.DbType = DbType.Boolean;
                    command.Parameters.Add(p);

                    p = command.CreateParameter();
                    p.ParameterName = "@scope_last_sync";
                    p.Value = scopeInfo.LastSync.HasValue ? (object)scopeInfo.LastSync.Value : DBNull.Value;
                    p.DbType = DbType.DateTime;
                    command.Parameters.Add(p);

                    p = command.CreateParameter();
                    p.ParameterName = "@sync_scope_id";
                    p.Value = scopeInfo.Id;
                    p.DbType = DbType.Guid;
                    command.Parameters.Add(p);

                    using (DbDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            scopeInfo.Name = reader["sync_scope_name"] as String;
                            scopeInfo.Id = (Guid)reader["sync_scope_id"];
                            scopeInfo.LastTimestamp = DbManager.ParseTimestamp(reader["scope_timestamp"]);
                            scopeInfo.IsLocal = (bool)reader["scope_is_local"];
                            scopeInfo.LastSync = reader["scope_last_sync"] != DBNull.Value ? (DateTime?)reader["scope_last_sync"] : null;
                        }
                    }

                    return scopeInfo;
                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during CreateTableScope : {ex}");
                throw;
            }
            finally
            {
                if (!alreadyOpened && connection.State != ConnectionState.Closed)
                    connection.Close();
            }
        }


        public bool NeedToCreateScopeInfoTable()
        {
            var command = connection.CreateCommand();
            if (transaction != null)
                command.Transaction = transaction;
            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    connection.Open();
                
                command.CommandText = @"SELECT COUNT(*) FROM ""pg_catalog"".""pg_tables"" WHERE ""tablename""='scope_info' AND ""schemaname""=current_schema()";

                return (long)command.ExecuteScalar() != 1;

            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during NeedToCreateScopeInfoTable command : {ex}");
                throw;
            }
            finally
            {
                if (!alreadyOpened && connection.State != ConnectionState.Closed)
                    connection.Close();

                if (command != null)
                    command.Dispose();
            }
        }



    }
}
