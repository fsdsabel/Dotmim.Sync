using System;
using System.Data;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Data;
using Dotmim.Sync.PostgreSql.Builders;
using Npgsql;

namespace Dotmim.Sync.PostgreSql
{
    public static class PostgreSqlExtensionsMethods
    {
        internal static NpgsqlParameter[] DeriveParameters(this NpgsqlConnection connection, NpgsqlCommand cmd,
            bool includeReturnValueParameter = false, NpgsqlTransaction transaction = null)
        {
            if (cmd == null) throw new ArgumentNullException("SqlCommand");

            var textParser = new ObjectNameParser(cmd.CommandText);

            // Hack to check for schema name in the spName
            string schemaName = "dbo";
            string spName = textParser.UnquotedString;
            int firstDot = spName.IndexOf('.');
            if (firstDot > 0)
            {
                schemaName = cmd.CommandText.Substring(0, firstDot);
                spName = spName.Substring(firstDot + 1);
            }

            var alreadyOpened = connection.State == ConnectionState.Open;

            if (!alreadyOpened)
                connection.Open();

            try
            {
                NpgsqlCommandBuilder.DeriveParameters(cmd);
            }
            finally
            {
                if (!alreadyOpened)
                    connection.Close();
            }

            if (!includeReturnValueParameter && cmd.Parameters.Count > 0)
                cmd.Parameters.RemoveAt(0);

            var discoveredParameters = new NpgsqlParameter[cmd.Parameters.Count];

            cmd.Parameters.CopyTo(discoveredParameters, 0);

            // Init the parameters with a DBNull value
            foreach (var discoveredParameter in discoveredParameters)
                discoveredParameter.Value = DBNull.Value;

            return discoveredParameters;

        }


        internal static NpgsqlParameter GetPostgreSqlParameter(this DmColumn column)
        {
            var mySqlDbMetadata = new PostgreSqlDbMetadata();

            var sqlParameter = new NpgsqlParameter
            {
                ParameterName = $"{PostgreSqlBuilderProcedure.PGSQL_PREFIX_PARAMETER}{column.ColumnName}",
                DbType = column.DbType,
                IsNullable = column.AllowDBNull
            };

            (byte precision, byte scale) = mySqlDbMetadata.TryGetOwnerPrecisionAndScale(column.OriginalDbType, column.DbType, false, false, column.Precision, column.Scale, column.Table.OriginalProvider, PostgreSqlSyncProvider.ProviderType);

            if ((sqlParameter.DbType == DbType.Decimal || sqlParameter.DbType == DbType.Double
                                                       || sqlParameter.DbType == DbType.Single || sqlParameter.DbType == DbType.VarNumeric) && precision > 0)
            {
                sqlParameter.Precision = precision;
                if (scale > 0)
                    sqlParameter.Scale = scale;
            }
            else if (column.MaxLength > 0)
            {
                sqlParameter.Size = (int)column.MaxLength;
            }
            else if (sqlParameter.DbType == DbType.Guid)
            {
                //sqlParameter.Size = 36;
            }
            else
            {
                sqlParameter.Size = -1;
            }

            return sqlParameter;
        }
    }
}
