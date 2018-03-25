using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;
using Dotmim.Sync.Data;
using Dotmim.Sync.Manager;
using Dotmim.Sync.PostgreSql.Builders;
using Npgsql;
using NpgsqlTypes;

namespace Dotmim.Sync.PostgreSql.Manager
{
    public class PostgreSqlManagerTable : IDbManagerTable
    {
        private NpgsqlConnection _sqlConnection;
        private NpgsqlTransaction _sqlTransaction;
        private PostgreSqlDbMetadata _postgreSqlDbMetadata;

        public PostgreSqlManagerTable(DbConnection connection, DbTransaction transaction)
        {
            _sqlConnection = connection as NpgsqlConnection;
            _sqlTransaction = transaction as NpgsqlTransaction;
            _postgreSqlDbMetadata = new PostgreSqlDbMetadata();
        }

        #region Implementation of IDbManagerTable

        public string TableName { get; set; }

        public List<DmColumn> GetTableDefinition()
        {
            List<DmColumn> columns = new List<DmColumn>();

            // Get the columns definition
            var dmColumnsList = PostgreSqlManagementUtils.ColumnsForTable(_sqlConnection, _sqlTransaction, TableName);
            var postgreSqlDbMetadata = new PostgreSqlDbMetadata();

            foreach (var c in dmColumnsList.Rows.OrderBy(r => (int)r["ordinal_position"]))
            {
                var typeName = c["data_type"].ToString();
                var name = c["column_name"].ToString();
                //var isUnsigned = c["column_type"] != DBNull.Value ? ((string)c["column_type"]).Contains("unsigned") : false;
                var isUnsigned = false;

                // Gets the datastore owner dbType 
                var datastoreDbType = (NpgsqlDbType)postgreSqlDbMetadata.ValidateOwnerDbType(typeName, isUnsigned, false);
                // once we have the datastore type, we can have the managed type
                Type columnType = postgreSqlDbMetadata.ValidateType(datastoreDbType);

                var dbColumn = DmColumn.CreateColumn(name, columnType);
                dbColumn.OriginalTypeName = typeName;
                dbColumn.SetOrdinal(Convert.ToInt32(c["ordinal_position"]));

                var maxLengthLong = c["character_octet_length"] != DBNull.Value ? Convert.ToInt64(c["character_octet_length"]) : 0;
                dbColumn.MaxLength = maxLengthLong > Int32.MaxValue ? Int32.MaxValue : (Int32)maxLengthLong;
                dbColumn.Precision = c["numeric_precision"] != DBNull.Value ? Convert.ToByte(c["numeric_precision"]) : (byte)0;
                dbColumn.Scale = c["numeric_scale"] != DBNull.Value ? Convert.ToByte(c["numeric_scale"]) : (byte)0;
                dbColumn.AllowDBNull = (String)c["is_nullable"] == "NO" ? false : true;
                dbColumn.AutoIncrement = typeName.Contains("serial");
                dbColumn.IsUnsigned = isUnsigned;

                columns.Add(dbColumn);

            }

            return columns;

        }

        public DmTable GetTableRelations()
        {
            return PostgreSqlManagementUtils.RelationsForTable(_sqlConnection, _sqlTransaction, TableName);
        }

        List<DbRelationDefinition> IDbManagerTable.GetTableRelations()
        {
            var dmRelations = GetTableRelations();

            if (dmRelations == null || dmRelations.Rows.Count == 0)
                return null;

            List<DbRelationDefinition> relations = new List<DbRelationDefinition>();

            foreach (var dmRow in dmRelations.Rows)
            {
                DbRelationDefinition relationDefinition = new DbRelationDefinition();
                relationDefinition.ForeignKey = (string)dmRow["ForeignKey"];
                relationDefinition.ColumnName = (string)dmRow["ColumnName"];
                relationDefinition.ReferenceColumnName = (string)dmRow["ReferenceColumnName"];
                relationDefinition.ReferenceTableName = (string)dmRow["ReferenceTableName"];
                relationDefinition.TableName = (string)dmRow["TableName"];

                relations.Add(relationDefinition);
            }

            return relations;

        }

        public List<string> GetTablePrimaryKeys()
        {
            // Get PrimaryKey
            var dmTableKeys = PostgreSqlManagementUtils.PrimaryKeysForTable(_sqlConnection, _sqlTransaction, TableName);

            if (dmTableKeys == null || dmTableKeys.Rows.Count == 0)
                throw new Exception("No Primary Keys in this table, it' can't happen :) ");

            var lstKeys = new List<String>();

            foreach (var dmKey in dmTableKeys.Rows)
                lstKeys.Add((string)dmKey["COLUMN_NAME"]);

            return lstKeys;

        }

        #endregion
    }
}
