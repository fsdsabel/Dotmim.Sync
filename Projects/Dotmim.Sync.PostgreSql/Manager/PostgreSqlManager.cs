using System.Data.Common;
using Dotmim.Sync.Manager;

namespace Dotmim.Sync.PostgreSql.Manager
{
    public class PostgreSqlManager : DbManager
    {
        public PostgreSqlManager(string tableName) : base(tableName)
        {
        }

        #region Overrides of DbManager

        public override IDbManagerTable CreateManagerTable(DbConnection connection, DbTransaction transaction = null)
        {
            return new PostgreSqlManagerTable(connection, transaction);
        }

        #endregion
    }
}
