using System;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Cache;
using Dotmim.Sync.Data;
using Dotmim.Sync.Manager;
using System.Data.Common;
using Dotmim.Sync.PostgreSql.Builders;
using Dotmim.Sync.PostgreSql.Manager;
using Npgsql;


namespace Dotmim.Sync.PostgreSql
{

    public class PostgreSqlSyncProvider : CoreProvider
    {
        ICache cacheManager;
        DbMetadata dbMetadata;
        static string providerType;

        public override string ProviderTypeName
        {
            get
            {
                return ProviderType;
            }
        }

        public static string ProviderType
        {
            get
            {
                if (!string.IsNullOrEmpty(providerType))
                    return providerType;

                Type type = typeof(PostgreSqlSyncProvider);
                providerType = $"{type.Name}, {type.ToString()}";

                return providerType;
            }

        }

        public override ICache CacheManager
        {
            get
            {
                if (cacheManager == null)
                    cacheManager = new InMemoryCache();

                return cacheManager;
            }
            set
            {
                cacheManager = value;

            }
        }

        /// <summary>
        /// MySql does not support Bulk operations
        /// </summary>
        public override bool SupportBulkOperations => false;

        /// <summary>
        /// MySql can be a server side provider
        /// </summary>
        public override bool CanBeServerProvider => true;


        /// <summary>
        /// Gets or Sets the MySql Metadata object, provided to validate the MySql Columns issued from MySql
        /// </summary>
        /// <summary>
        /// Gets or sets the Metadata object which parse Sql server types
        /// </summary>
        public override DbMetadata Metadata
        {
            get
            {
                if (dbMetadata == null)
                    dbMetadata = new PostgreSqlDbMetadata();

                return dbMetadata;
            }
            set
            {
                dbMetadata = value;

            }
        }

        public PostgreSqlSyncProvider() : base()
        {
        }
        public PostgreSqlSyncProvider(string connectionString) : base()
        {

            var builder = new NpgsqlConnectionStringBuilder(connectionString);
            

            this.ConnectionString = builder.ConnectionString;
        }


        public PostgreSqlSyncProvider(NpgsqlConnectionStringBuilder builder) : base()
        {
            if (String.IsNullOrEmpty(builder.ConnectionString))
                throw new Exception("You have to provide parameters to the Npgsql builder to be able to construct a valid connection string.");


            this.ConnectionString = builder.ConnectionString;
        }


        public override DbConnection CreateConnection() => new NpgsqlConnection(this.ConnectionString);

        public override DbBuilder GetDatabaseBuilder(DmTable tableDescription) => new PostgreSqlBuilder(tableDescription);

        public override DbManager GetDbManager(string tableName) => new PostgreSqlManager(tableName);

        public override DbScopeBuilder GetScopeBuilder() => new PostgreSqlScopeBuilder();
    }
}
