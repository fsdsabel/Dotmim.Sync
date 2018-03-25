using System.Data.Common;
using System.Text;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Data;

namespace Dotmim.Sync.PostgreSql.Builders
{

    /// <summary>
    /// The MySqlBuilder class is the MySql implementation of DbBuilder class.
    /// In charge of creating tracking table, stored proc, triggers and adapters.
    /// </summary>
    public class PostgreSqlBuilder : DbBuilder
    {
        public PostgreSqlBuilder(DmTable tableDescription) : base(tableDescription)
        {
     }

        internal static (ObjectNameParser tableName, ObjectNameParser trackingName) GetParsers(DmTable tableDescription)
        {
            string tableAndPrefixName = tableDescription.TableName;
            var originalTableName = new ObjectNameParser(tableAndPrefixName, "\"", "\"");
            var trackingTableName = new ObjectNameParser($"{tableAndPrefixName}_tracking", "\"", "\"");

            return (originalTableName, trackingTableName);
        }

        public static string WrapScriptTextWithComments(string commandText, string commentText)
        {
            StringBuilder stringBuilder = new StringBuilder();
            StringBuilder stringBuilder1 = new StringBuilder("\n");

            string str = stringBuilder1.ToString();
            stringBuilder.AppendLine("DELIMITER $$ ");
            stringBuilder.Append(string.Concat("-- BEGIN ", commentText, str));
            stringBuilder.Append(commandText);
            stringBuilder.Append(string.Concat("-- END ", commentText, str, "\n"));
            stringBuilder.AppendLine("$$ ");
            stringBuilder.AppendLine("DELIMITER ;");
            return stringBuilder.ToString();
        }

         public override IDbBuilderProcedureHelper CreateProcBuilder(DbConnection connection, DbTransaction transaction = null)
        {
            return new PostgreSqlBuilderProcedure(TableDescription, connection, transaction);
        }

        public override IDbBuilderTriggerHelper CreateTriggerBuilder(DbConnection connection, DbTransaction transaction = null)
        {
            return new PostgreSqlBuilderTrigger(TableDescription, connection, transaction);
        }

        public override IDbBuilderTableHelper CreateTableBuilder(DbConnection connection, DbTransaction transaction = null)
        {
            return new PostgreSqlBuilderTable(TableDescription, connection, transaction);
        }

        public override IDbBuilderTrackingTableHelper CreateTrackingTableBuilder(DbConnection connection, DbTransaction transaction = null)
        {
            return new PostgreSqlBuilderTrackingTable(TableDescription, connection, transaction);
        }

        public override DbSyncAdapter CreateSyncAdapter(DbConnection connection, DbTransaction transaction = null)
        {
            return new PostgreSqlSyncAdapter(TableDescription, connection, transaction);
        }
    }
}
