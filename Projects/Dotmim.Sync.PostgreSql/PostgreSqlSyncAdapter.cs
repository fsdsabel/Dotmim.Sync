﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Data;
using Dotmim.Sync.PostgreSql.Builders;
using Npgsql;

namespace Dotmim.Sync.PostgreSql
{
    public class PostgreSqlSyncAdapter : DbSyncAdapter
    {
        private NpgsqlConnection connection;
        private NpgsqlTransaction transaction;
        private PostgreSqlObjectNames postgreSqlObjectNames;
        // Derive Parameters cache
        private static Dictionary<string, List<NpgsqlParameter>> derivingParameters = new Dictionary<string, List<NpgsqlParameter>>();

       

        public override DbConnection Connection
        {
            get
            {
                return this.connection;
            }
        }
        public override DbTransaction Transaction
        {
            get
            {
                return this.transaction;
            }

        }

        public PostgreSqlSyncAdapter(DmTable tableDescription, DbConnection connection, DbTransaction transaction) : base(tableDescription)
        {
            var sqlc = connection as NpgsqlConnection;
            this.connection = sqlc ?? throw new InvalidCastException("Connection should be a NpgsqlConnection");

            this.transaction = transaction as NpgsqlTransaction;

            this.postgreSqlObjectNames = new PostgreSqlObjectNames(TableDescription);
        }

        public override bool IsPrimaryKeyViolation(Exception Error)
        {
            return false;
        }

        public override DbCommand GetCommand(DbCommandType commandType, IEnumerable<string> additionals = null)
        {
            var command = this.Connection.CreateCommand();
            string text;

            if (additionals != null)
                text = this.postgreSqlObjectNames.GetCommandName(commandType, additionals);
            else
                text = this.postgreSqlObjectNames.GetCommandName(commandType);

            
            command.CommandType = CommandType.StoredProcedure;
            command.CommandText = text;
            command.Connection = Connection;

            if (Transaction != null)
                command.Transaction = Transaction;

            return command;
        }


        public override DbParameter SetCommandParameters(DbCommandType commandType, DbCommand command)
        {
           

            switch (commandType)
            {
                case DbCommandType.SelectChanges:
                    this.SetSelecteChangesParameters(command);
                    break;
                case DbCommandType.SelectRow:
                    this.SetSelectRowParameters(command);
                    break;
                case DbCommandType.DeleteMetadata:
                    this.SetDeleteMetadataParameters(command);
                    break;
                case DbCommandType.DeleteRow:
                    this.SetDeleteRowParameters(command);
                    break;
                case DbCommandType.InsertMetadata:
                    this.SetInsertMetadataParameters(command);
                    break;
                case DbCommandType.InsertRow:
                    this.SetInsertRowParameters(command);
                    break;
                case DbCommandType.UpdateMetadata:
                    this.SetUpdateMetadataParameters(command);
                    break;
                case DbCommandType.UpdateRow:
                    this.SetUpdateRowParameters(command);
                    break;
                default:
                    break;
            }
            DbParameter returnValue = command.CreateParameter();
            returnValue.Direction = ParameterDirection.Output;
            returnValue.DbType = DbType.Int32;
            command.Parameters.Add(returnValue);
            return returnValue;
        }

        private void SetUpdateRowParameters(DbCommand command)
        {
            DbParameter p;

            foreach (DmColumn column in this.TableDescription.Columns.Where(c => !c.ReadOnly))
            {
                ObjectNameParser quotedColumn = new ObjectNameParser(column.ColumnName);
                p = command.CreateParameter();
                p.ParameterName = $"@in_{quotedColumn.UnquotedStringWithUnderScore.ToLowerInvariant()}";
                p.DbType = column.DbType;
                p.SourceColumn = column.ColumnName;
                command.Parameters.Add(p);
            }

            p = command.CreateParameter();
            p.ParameterName = "@sync_force_write";
            p.DbType = DbType.Int32;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_min_timestamp";
            p.DbType = DbType.Int64;
            command.Parameters.Add(p);

        }

        private void SetUpdateMetadataParameters(DbCommand command)
        {
            DbParameter p;

            foreach (DmColumn column in this.TableDescription.PrimaryKey.Columns.Where(c => !c.ReadOnly))
            {
                ObjectNameParser quotedColumn = new ObjectNameParser(column.ColumnName);
                p = command.CreateParameter();
                p.ParameterName = $"@in_{quotedColumn.UnquotedStringWithUnderScore.ToLowerInvariant()}";
                p.DbType = column.DbType;
                p.SourceColumn = column.ColumnName;
                command.Parameters.Add(p);
            }

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_id";
            p.DbType = DbType.Guid;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_row_is_tombstone";
            p.DbType = DbType.Int32;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@create_timestamp";
            p.DbType = DbType.Int64;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@update_timestamp";
            p.DbType = DbType.Int64;
            command.Parameters.Add(p);
        }

        private void SetInsertRowParameters(DbCommand command)
        {
            DbParameter p;

            foreach (DmColumn column in this.TableDescription.Columns.Where(c => !c.ReadOnly))
            {
                ObjectNameParser quotedColumn = new ObjectNameParser(column.ColumnName);
                p = command.CreateParameter();
                p.ParameterName = $"@in_{quotedColumn.UnquotedStringWithUnderScore.ToLowerInvariant()}";
                p.DbType = column.DbType;
                p.SourceColumn = column.ColumnName;
                command.Parameters.Add(p);
            }
        }

        private void SetInsertMetadataParameters(DbCommand command)
        {
            DbParameter p;

            foreach (DmColumn column in this.TableDescription.PrimaryKey.Columns.Where(c => !c.ReadOnly))
            {
                ObjectNameParser quotedColumn = new ObjectNameParser(column.ColumnName);
                p = command.CreateParameter();
                p.ParameterName = $"@in_{quotedColumn.UnquotedStringWithUnderScore.ToLowerInvariant()}";
                p.DbType = column.DbType;
                p.SourceColumn = column.ColumnName;
                command.Parameters.Add(p);
            }

            /*
            p = command.CreateParameter();
            p.ParameterName = "@create_scope_id";
            p.DbType = DbType.Guid;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@update_scope_id";
            p.DbType = DbType.Guid;
            command.Parameters.Add(p);*/

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_id";
            p.DbType = DbType.Guid;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_row_is_tombstone";
            p.DbType = DbType.Int32;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@create_timestamp";
            p.DbType = DbType.Int64;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@update_timestamp";
            p.DbType = DbType.Int64;
            command.Parameters.Add(p);
        }

        private void SetDeleteRowParameters(DbCommand command)
        {
            DbParameter p;

            foreach (DmColumn column in this.TableDescription.PrimaryKey.Columns.Where(c => !c.ReadOnly))
            {
                ObjectNameParser quotedColumn = new ObjectNameParser(column.ColumnName);
                p = command.CreateParameter();
                p.ParameterName = $"@in_{quotedColumn.UnquotedStringWithUnderScore.ToLowerInvariant()}";
                p.DbType = column.DbType;
                p.SourceColumn = column.ColumnName;
                command.Parameters.Add(p);
            }

            p = command.CreateParameter();
            p.ParameterName = "@sync_force_write";
            p.DbType = DbType.Int32;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_min_timestamp";
            p.DbType = DbType.Int64;
            command.Parameters.Add(p);
        }

        private void SetSelectRowParameters(DbCommand command)
        {
            DbParameter p;

            foreach (DmColumn column in this.TableDescription.PrimaryKey.Columns.Where(c => !c.ReadOnly))
            {
                ObjectNameParser quotedColumn = new ObjectNameParser(column.ColumnName);
                p = command.CreateParameter();
                p.ParameterName = $"@in_{quotedColumn.UnquotedStringWithUnderScore.ToLowerInvariant()}";
                p.DbType = column.DbType;
                p.SourceColumn = column.ColumnName;
                command.Parameters.Add(p);
            }

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_id";
            p.DbType = DbType.Guid;
            p.Value = DBNull.Value;
            command.Parameters.Add(p);

        }

        private void SetDeleteMetadataParameters(DbCommand command)
        {
            return;
        }

        private void SetSelecteChangesParameters(DbCommand command)
        {
            var p = command.CreateParameter();
            p.ParameterName = "@sync_min_timestamp";
            p.DbType = DbType.Int64;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_id";
            p.DbType = DbType.Guid;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_is_new";
            p.DbType = DbType.Int16;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_is_reinit";
            p.DbType = DbType.Int16;
            command.Parameters.Add(p);
        }


        public override void ExecuteBatchCommand(DbCommand cmd, DmView applyTable, DmTable failedRows, ScopeInfo scope)
        {
            throw new NotImplementedException();
        }
    }
}
