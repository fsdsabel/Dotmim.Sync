using System.Data.Common;
using Dotmim.Sync.Builders;

namespace Dotmim.Sync.PostgreSql.Builders
{
    public class PostgreSqlScopeBuilder : DbScopeBuilder
    {
        

        public override IDbScopeInfoBuilder CreateScopeInfoBuilder(DbConnection connection, DbTransaction transaction = null)
        {
            return new PostgreSqlScopeInfoBuilder(connection, transaction);
        }
    }
}
