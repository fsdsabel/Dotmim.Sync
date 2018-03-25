using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.PostgreSql;
using Dotmim.Sync.Sqlite;
using Dotmim.Sync.Test;
using Dotmim.Sync.Test.SqlUtils;
using Dotmim.Sync.Tests.Misc;
using Microsoft.Data.Sqlite;
using MySql.Data.MySqlClient;
using Npgsql;
using Xunit;


namespace Dotmim.Sync.Tests
{
    public class PostgreSqlSyncSimpleFixture : IDisposable
    {
        /* GUID-Support:

         * CREATE EXTENSION "uuid-ossp"
                SCHEMA public
                VERSION "1.1";
         *
         *
         */


        private string createTableScript =
        $@"CREATE TABLE IF NOT EXISTS ""ServiceTickets"" (
	            ""ServiceTicketID"" uuid NOT NULL,
	            ""Title"" varchar NOT NULL,
	            ""Description"" varchar NULL,
	            ""StatusValue"" int NOT NULL,
	            ""EscalationLevel"" int NOT NULL,
	            ""Opened"" timestamp NULL,
	            ""Closed"" timestamp NULL,
	            ""CustomerID"" int NULL,
                CONSTRAINT ""PK_ServiceTickets"" PRIMARY KEY ( ""ServiceTicketID"" ));";

        private string datas =
        $@"
            INSERT INTO ""ServiceTickets"" (""ServiceTicketID"", ""Title"", ""Description"", ""StatusValue"", ""EscalationLevel"", ""Opened"", ""Closed"", ""CustomerID"") VALUES (public.uuid_generate_v4(), 'Titre 3', 'Description 3', 1, 0, CAST('2016-07-29T16:36:41.733' AS TIMESTAMP), NULL, 1);
            INSERT INTO ""ServiceTickets"" (""ServiceTicketID"", ""Title"", ""Description"", ""StatusValue"", ""EscalationLevel"", ""Opened"", ""Closed"", ""CustomerID"") VALUES (public.uuid_generate_v4(), 'Titre 4', 'Description 4', 1, 0, CAST('2016-07-29T16:36:41.733' AS TIMESTAMP), NULL, 1);
            INSERT INTO ""ServiceTickets"" (""ServiceTicketID"", ""Title"", ""Description"", ""StatusValue"", ""EscalationLevel"", ""Opened"", ""Closed"", ""CustomerID"") VALUES (public.uuid_generate_v4(), 'Titre Client 1', 'Description Client 1', 1, 0, CAST('2016-07-29T17:26:20.720' AS TIMESTAMP), NULL, 1);
            INSERT INTO ""ServiceTickets"" (""ServiceTicketID"", ""Title"", ""Description"", ""StatusValue"", ""EscalationLevel"", ""Opened"", ""Closed"", ""CustomerID"") VALUES (public.uuid_generate_v4(), 'Titre 6', 'Description 6', 1, 0, CAST('2016-07-29T16:36:41.733' AS TIMESTAMP), NULL, 1);
            INSERT INTO ""ServiceTickets"" (""ServiceTicketID"", ""Title"", ""Description"", ""StatusValue"", ""EscalationLevel"", ""Opened"", ""Closed"", ""CustomerID"") VALUES (public.uuid_generate_v4(), 'Titre 7', 'Description 7', 1, 0, CAST('2016-07-29T16:36:41.733' AS TIMESTAMP), NULL, 10);
            INSERT INTO ""ServiceTickets"" (""ServiceTicketID"", ""Title"", ""Description"", ""StatusValue"", ""EscalationLevel"", ""Opened"", ""Closed"", ""CustomerID"") VALUES (public.uuid_generate_v4(), 'Titre 3', 'Description 3', 1, 0, CAST('2016-07-29T16:36:41.733' AS TIMESTAMP), NULL, 1);
            INSERT INTO ""ServiceTickets"" (""ServiceTicketID"", ""Title"", ""Description"", ""StatusValue"", ""EscalationLevel"", ""Opened"", ""Closed"", ""CustomerID"") VALUES (public.uuid_generate_v4(), 'Titre 4', 'Description 4', 1, 0, CAST('2016-07-29T16:36:41.733' AS TIMESTAMP), NULL, 1);
            INSERT INTO ""ServiceTickets"" (""ServiceTicketID"", ""Title"", ""Description"", ""StatusValue"", ""EscalationLevel"", ""Opened"", ""Closed"", ""CustomerID"") VALUES (public.uuid_generate_v4(), 'Titre Client 1', 'Description Client 1', 1, 0, CAST('2016-07-29T17:26:20.720' AS TIMESTAMP), NULL, 1);
            INSERT INTO ""ServiceTickets"" (""ServiceTicketID"", ""Title"", ""Description"", ""StatusValue"", ""EscalationLevel"", ""Opened"", ""Closed"", ""CustomerID"") VALUES (public.uuid_generate_v4(), 'Titre 6', 'Description 6', 1, 0, CAST('2016-07-29T16:36:41.733' AS TIMESTAMP), NULL, 1);
            INSERT INTO ""ServiceTickets"" (""ServiceTicketID"", ""Title"", ""Description"", ""StatusValue"", ""EscalationLevel"", ""Opened"", ""Closed"", ""CustomerID"") VALUES (public.uuid_generate_v4(), 'Titre 7', 'Description 7', 1, 0, CAST('2016-07-29T16:36:41.733' AS TIMESTAMP), NULL, 10);
            INSERT INTO ""ServiceTickets"" (""ServiceTicketID"", ""Title"", ""Description"", ""StatusValue"", ""EscalationLevel"", ""Opened"", ""Closed"", ""CustomerID"") VALUES (public.uuid_generate_v4(), 'Titre 3', 'Description 3', 1, 0, CAST('2016-07-29T16:36:41.733' AS TIMESTAMP), NULL, 1);
            INSERT INTO ""ServiceTickets"" (""ServiceTicketID"", ""Title"", ""Description"", ""StatusValue"", ""EscalationLevel"", ""Opened"", ""Closed"", ""CustomerID"") VALUES (public.uuid_generate_v4(), 'Titre 4', 'Description 4', 1, 0, CAST('2016-07-29T16:36:41.733' AS TIMESTAMP), NULL, 1);
            INSERT INTO ""ServiceTickets"" (""ServiceTicketID"", ""Title"", ""Description"", ""StatusValue"", ""EscalationLevel"", ""Opened"", ""Closed"", ""CustomerID"") VALUES (public.uuid_generate_v4(), 'Titre Client 1', 'Description Client 1', 1, 0, CAST('2016-07-29T17:26:20.720' AS TIMESTAMP), NULL, 1);
            INSERT INTO ""ServiceTickets"" (""ServiceTicketID"", ""Title"", ""Description"", ""StatusValue"", ""EscalationLevel"", ""Opened"", ""Closed"", ""CustomerID"") VALUES (public.uuid_generate_v4(), 'Titre 6', 'Description 6', 1, 0, CAST('2016-07-29T16:36:41.733' AS TIMESTAMP), NULL, 1);
            INSERT INTO ""ServiceTickets"" (""ServiceTicketID"", ""Title"", ""Description"", ""StatusValue"", ""EscalationLevel"", ""Opened"", ""Closed"", ""CustomerID"") VALUES (public.uuid_generate_v4(), 'Titre 7', 'Description 7', 1, 0, CAST('2016-07-29T16:36:41.733' AS TIMESTAMP), NULL, 10);
            INSERT INTO ""ServiceTickets"" (""ServiceTicketID"", ""Title"", ""Description"", ""StatusValue"", ""EscalationLevel"", ""Opened"", ""Closed"", ""CustomerID"") VALUES (public.uuid_generate_v4(), 'Titre 3', 'Description 3', 1, 0, CAST('2016-07-29T16:36:41.733' AS TIMESTAMP), NULL, 1);
            INSERT INTO ""ServiceTickets"" (""ServiceTicketID"", ""Title"", ""Description"", ""StatusValue"", ""EscalationLevel"", ""Opened"", ""Closed"", ""CustomerID"") VALUES (public.uuid_generate_v4(), 'Titre 4', 'Description 4', 1, 0, CAST('2016-07-29T16:36:41.733' AS TIMESTAMP), NULL, 1);
            INSERT INTO ""ServiceTickets"" (""ServiceTicketID"", ""Title"", ""Description"", ""StatusValue"", ""EscalationLevel"", ""Opened"", ""Closed"", ""CustomerID"") VALUES (public.uuid_generate_v4(), 'Titre Client 1', 'Description Client 1', 1, 0, CAST('2016-07-29T17:26:20.720' AS TIMESTAMP), NULL, 1);
            INSERT INTO ""ServiceTickets"" (""ServiceTicketID"", ""Title"", ""Description"", ""StatusValue"", ""EscalationLevel"", ""Opened"", ""Closed"", ""CustomerID"") VALUES (public.uuid_generate_v4(), 'Titre 6', 'Description 6', 1, 0, CAST('2016-07-29T16:36:41.733' AS TIMESTAMP), NULL, 1);
            INSERT INTO ""ServiceTickets"" (""ServiceTicketID"", ""Title"", ""Description"", ""StatusValue"", ""EscalationLevel"", ""Opened"", ""Closed"", ""CustomerID"") VALUES (public.uuid_generate_v4(), 'Titre 7', 'Description 7', 1, 0, CAST('2016-07-29T16:36:41.733' AS TIMESTAMP), NULL, 10);
            INSERT INTO ""ServiceTickets"" (""ServiceTicketID"", ""Title"", ""Description"", ""StatusValue"", ""EscalationLevel"", ""Opened"", ""Closed"", ""CustomerID"") VALUES (public.uuid_generate_v4(), 'Titre 3', 'Description 3', 1, 0, CAST('2016-07-29T16:36:41.733' AS TIMESTAMP), NULL, 1);
            INSERT INTO ""ServiceTickets"" (""ServiceTicketID"", ""Title"", ""Description"", ""StatusValue"", ""EscalationLevel"", ""Opened"", ""Closed"", ""CustomerID"") VALUES (public.uuid_generate_v4(), 'Titre 4', 'Description 4', 1, 0, CAST('2016-07-29T16:36:41.733' AS TIMESTAMP), NULL, 1);
            INSERT INTO ""ServiceTickets"" (""ServiceTicketID"", ""Title"", ""Description"", ""StatusValue"", ""EscalationLevel"", ""Opened"", ""Closed"", ""CustomerID"") VALUES (public.uuid_generate_v4(), 'Titre Client 1', 'Description Client 1', 1, 0, CAST('2016-07-29T17:26:20.720' AS TIMESTAMP), NULL, 1);
            INSERT INTO ""ServiceTickets"" (""ServiceTicketID"", ""Title"", ""Description"", ""StatusValue"", ""EscalationLevel"", ""Opened"", ""Closed"", ""CustomerID"") VALUES (public.uuid_generate_v4(), 'Titre 6', 'Description 6', 1, 0, CAST('2016-07-29T16:36:41.733' AS TIMESTAMP), NULL, 1);
            INSERT INTO ""ServiceTickets"" (""ServiceTicketID"", ""Title"", ""Description"", ""StatusValue"", ""EscalationLevel"", ""Opened"", ""Closed"", ""CustomerID"") VALUES (public.uuid_generate_v4(), 'Titre 7', 'Description 7', 1, 0, CAST('2016-07-29T16:36:41.733' AS TIMESTAMP), NULL, 10);
            INSERT INTO ""ServiceTickets"" (""ServiceTicketID"", ""Title"", ""Description"", ""StatusValue"", ""EscalationLevel"", ""Opened"", ""Closed"", ""CustomerID"") VALUES (public.uuid_generate_v4(), 'Titre 3', 'Description 3', 1, 0, CAST('2016-07-29T16:36:41.733' AS TIMESTAMP), NULL, 1);
            INSERT INTO ""ServiceTickets"" (""ServiceTicketID"", ""Title"", ""Description"", ""StatusValue"", ""EscalationLevel"", ""Opened"", ""Closed"", ""CustomerID"") VALUES (public.uuid_generate_v4(), 'Titre 4', 'Description 4', 1, 0, CAST('2016-07-29T16:36:41.733' AS TIMESTAMP), NULL, 1);
            INSERT INTO ""ServiceTickets"" (""ServiceTicketID"", ""Title"", ""Description"", ""StatusValue"", ""EscalationLevel"", ""Opened"", ""Closed"", ""CustomerID"") VALUES (public.uuid_generate_v4(), 'Titre Client 1', 'Description Client 1', 1, 0, CAST('2016-07-29T17:26:20.720' AS TIMESTAMP), NULL, 1);
            INSERT INTO ""ServiceTickets"" (""ServiceTicketID"", ""Title"", ""Description"", ""StatusValue"", ""EscalationLevel"", ""Opened"", ""Closed"", ""CustomerID"") VALUES (public.uuid_generate_v4(), 'Titre 6', 'Description 6', 1, 0, CAST('2016-07-29T16:36:41.733' AS TIMESTAMP), NULL, 1);
            INSERT INTO ""ServiceTickets"" (""ServiceTicketID"", ""Title"", ""Description"", ""StatusValue"", ""EscalationLevel"", ""Opened"", ""Closed"", ""CustomerID"") VALUES (public.uuid_generate_v4(), 'Titre 7', 'Description 7', 1, 0, CAST('2016-07-29T16:36:41.733' AS TIMESTAMP), NULL, 10);
            INSERT INTO ""ServiceTickets"" (""ServiceTicketID"", ""Title"", ""Description"", ""StatusValue"", ""EscalationLevel"", ""Opened"", ""Closed"", ""CustomerID"") VALUES (public.uuid_generate_v4(), 'Titre 3', 'Description 3', 1, 0, CAST('2016-07-29T16:36:41.733' AS TIMESTAMP), NULL, 1);
            INSERT INTO ""ServiceTickets"" (""ServiceTicketID"", ""Title"", ""Description"", ""StatusValue"", ""EscalationLevel"", ""Opened"", ""Closed"", ""CustomerID"") VALUES (public.uuid_generate_v4(), 'Titre 4', 'Description 4', 1, 0, CAST('2016-07-29T16:36:41.733' AS TIMESTAMP), NULL, 1);
            INSERT INTO ""ServiceTickets"" (""ServiceTicketID"", ""Title"", ""Description"", ""StatusValue"", ""EscalationLevel"", ""Opened"", ""Closed"", ""CustomerID"") VALUES (public.uuid_generate_v4(), 'Titre Client 1', 'Description Client 1', 1, 0, CAST('2016-07-29T17:26:20.720' AS TIMESTAMP), NULL, 1);
            INSERT INTO ""ServiceTickets"" (""ServiceTicketID"", ""Title"", ""Description"", ""StatusValue"", ""EscalationLevel"", ""Opened"", ""Closed"", ""CustomerID"") VALUES (public.uuid_generate_v4(), 'Titre 6', 'Description 6', 1, 0, CAST('2016-07-29T16:36:41.733' AS TIMESTAMP), NULL, 1);
            INSERT INTO ""ServiceTickets"" (""ServiceTicketID"", ""Title"", ""Description"", ""StatusValue"", ""EscalationLevel"", ""Opened"", ""Closed"", ""CustomerID"") VALUES (public.uuid_generate_v4(), 'Titre 7', 'Description 7', 1, 0, CAST('2016-07-29T16:36:41.733' AS TIMESTAMP), NULL, 10);
            INSERT INTO ""ServiceTickets"" (""ServiceTicketID"", ""Title"", ""Description"", ""StatusValue"", ""EscalationLevel"", ""Opened"", ""Closed"", ""CustomerID"") VALUES (public.uuid_generate_v4(), 'Titre 3', 'Description 3', 1, 0, CAST('2016-07-29T16:36:41.733' AS TIMESTAMP), NULL, 1);
            INSERT INTO ""ServiceTickets"" (""ServiceTicketID"", ""Title"", ""Description"", ""StatusValue"", ""EscalationLevel"", ""Opened"", ""Closed"", ""CustomerID"") VALUES (public.uuid_generate_v4(), 'Titre 4', 'Description 4', 1, 0, CAST('2016-07-29T16:36:41.733' AS TIMESTAMP), NULL, 1);
            INSERT INTO ""ServiceTickets"" (""ServiceTicketID"", ""Title"", ""Description"", ""StatusValue"", ""EscalationLevel"", ""Opened"", ""Closed"", ""CustomerID"") VALUES (public.uuid_generate_v4(), 'Titre Client 1', 'Description Client 1', 1, 0, CAST('2016-07-29T17:26:20.720' AS TIMESTAMP), NULL, 1);
            INSERT INTO ""ServiceTickets"" (""ServiceTicketID"", ""Title"", ""Description"", ""StatusValue"", ""EscalationLevel"", ""Opened"", ""Closed"", ""CustomerID"") VALUES (public.uuid_generate_v4(), 'Titre 6', 'Description 6', 1, 0, CAST('2016-07-29T16:36:41.733' AS TIMESTAMP), NULL, 1);
            INSERT INTO ""ServiceTickets"" (""ServiceTicketID"", ""Title"", ""Description"", ""StatusValue"", ""EscalationLevel"", ""Opened"", ""Closed"", ""CustomerID"") VALUES (public.uuid_generate_v4(), 'Titre 7', 'Description 7', 1, 0, CAST('2016-07-29T16:36:41.733' AS TIMESTAMP), NULL, 10);
            INSERT INTO ""ServiceTickets"" (""ServiceTicketID"", ""Title"", ""Description"", ""StatusValue"", ""EscalationLevel"", ""Opened"", ""Closed"", ""CustomerID"") VALUES (public.uuid_generate_v4(), 'Titre 3', 'Description 3', 1, 0, CAST('2016-07-29T16:36:41.733' AS TIMESTAMP), NULL, 1);
            INSERT INTO ""ServiceTickets"" (""ServiceTicketID"", ""Title"", ""Description"", ""StatusValue"", ""EscalationLevel"", ""Opened"", ""Closed"", ""CustomerID"") VALUES (public.uuid_generate_v4(), 'Titre 4', 'Description 4', 1, 0, CAST('2016-07-29T16:36:41.733' AS TIMESTAMP), NULL, 1);
            INSERT INTO ""ServiceTickets"" (""ServiceTicketID"", ""Title"", ""Description"", ""StatusValue"", ""EscalationLevel"", ""Opened"", ""Closed"", ""CustomerID"") VALUES (public.uuid_generate_v4(), 'Titre Client 1', 'Description Client 1', 1, 0, CAST('2016-07-29T17:26:20.720' AS TIMESTAMP), NULL, 1);
            INSERT INTO ""ServiceTickets"" (""ServiceTicketID"", ""Title"", ""Description"", ""StatusValue"", ""EscalationLevel"", ""Opened"", ""Closed"", ""CustomerID"") VALUES (public.uuid_generate_v4(), 'Titre 6', 'Description 6', 1, 0, CAST('2016-07-29T16:36:41.733' AS TIMESTAMP), NULL, 1);
            INSERT INTO ""ServiceTickets"" (""ServiceTicketID"", ""Title"", ""Description"", ""StatusValue"", ""EscalationLevel"", ""Opened"", ""Closed"", ""CustomerID"") VALUES (public.uuid_generate_v4(), 'Titre 7', 'Description 7', 1, 0, CAST('2016-07-29T16:36:41.733' AS TIMESTAMP), NULL, 10);
            INSERT INTO ""ServiceTickets"" (""ServiceTicketID"", ""Title"", ""Description"", ""StatusValue"", ""EscalationLevel"", ""Opened"", ""Closed"", ""CustomerID"") VALUES (public.uuid_generate_v4(), 'Titre 3', 'Description 3', 1, 0, CAST('2016-07-29T16:36:41.733' AS TIMESTAMP), NULL, 1);
            INSERT INTO ""ServiceTickets"" (""ServiceTicketID"", ""Title"", ""Description"", ""StatusValue"", ""EscalationLevel"", ""Opened"", ""Closed"", ""CustomerID"") VALUES (public.uuid_generate_v4(), 'Titre 4', 'Description 4', 1, 0, CAST('2016-07-29T16:36:41.733' AS TIMESTAMP), NULL, 1);
            INSERT INTO ""ServiceTickets"" (""ServiceTicketID"", ""Title"", ""Description"", ""StatusValue"", ""EscalationLevel"", ""Opened"", ""Closed"", ""CustomerID"") VALUES (public.uuid_generate_v4(), 'Titre Client 1', 'Description Client 1', 1, 0, CAST('2016-07-29T17:26:20.720' AS TIMESTAMP), NULL, 1);
            INSERT INTO ""ServiceTickets"" (""ServiceTicketID"", ""Title"", ""Description"", ""StatusValue"", ""EscalationLevel"", ""Opened"", ""Closed"", ""CustomerID"") VALUES (public.uuid_generate_v4(), 'Titre 6', 'Description 6', 1, 0, CAST('2016-07-29T16:36:41.733' AS TIMESTAMP), NULL, 1);
            INSERT INTO ""ServiceTickets"" (""ServiceTicketID"", ""Title"", ""Description"", ""StatusValue"", ""EscalationLevel"", ""Opened"", ""Closed"", ""CustomerID"") VALUES (public.uuid_generate_v4(), 'Titre 7', 'Description 7', 1, 0, CAST('2016-07-29T16:36:41.733' AS TIMESTAMP), NULL, 10);
          ";

        private HelperDB helperDb = new HelperDB();
        private string serverSchemaName = "unittest_dev";
        private string serverDbName = "UnitTests";

        public string[] Tables => new string[] { "ServiceTickets" };

        public string ClientSqliteDb => "client.sqlite";

        public String ServerConnectionString => HelperDB.GetPostgreSqlDatabaseConnectionString(serverDbName, serverSchemaName);
        public SyncAgent Agent { get; set; }


        public PostgreSqlSyncSimpleFixture()
        {
            // create databases
            helperDb.ExecutePostgreSqlScript(serverDbName, "",
                @"drop schema IF EXISTS ""unittest_dev"" cascade;
                  create schema ""unittest_dev"";
                  ALTER DEFAULT PRIVILEGES IN SCHEMA unittest_dev
                  GRANT ALL ON TABLES TO unittester;
                  GRANT ALL ON SCHEMA unittest_dev TO unittester; ");

            // create table
            helperDb.ExecutePostgreSqlScript(serverDbName, serverSchemaName, createTableScript);
            // insert table
            helperDb.ExecutePostgreSqlScript(serverDbName, serverSchemaName, datas);

            if (File.Exists(ClientSqliteDb))
            {
                File.Delete(ClientSqliteDb);
            }
            
            

            var serverProvider = new PostgreSqlSyncProvider(ServerConnectionString);
            var clientProvider = new SqliteSyncProvider(ClientSqliteDb);
            var simpleConfiguration = new SyncConfiguration(Tables);

            Agent = new SyncAgent(clientProvider, serverProvider, simpleConfiguration);

        }
        public void Dispose()
        {
            helperDb.ExecutePostgreSqlScript(serverDbName, "", @"drop schema ""unittest_dev"" cascade;");
            File.Delete(ClientSqliteDb);
        }

    }

    

    [TestCaseOrderer("Dotmim.Sync.Tests.Misc.PriorityOrderer", "Dotmim.Sync.Tests")]
    public class PostgreSqlSyncTests : IClassFixture<PostgreSqlSyncSimpleFixture>
    {
        PostgreSqlSyncSimpleFixture fixture;
        SyncAgent agent;

        public PostgreSqlSyncTests(PostgreSqlSyncSimpleFixture fixture)
        {
            this.fixture = fixture;
            this.agent = fixture.Agent;
        }

        [Fact, TestPriority(1)]
        public async Task Initialize()
        {
            var session = await agent.SynchronizeAsync();

            Assert.Equal(50, session.TotalChangesDownloaded);
            Assert.Equal(0, session.TotalChangesUploaded);
        }


        [Theory, ClassData(typeof(InlineConfigurations)), TestPriority(2)]
        public async Task SyncNoRows(SyncConfiguration conf)
        {
            agent.Configuration = conf;
            agent.Configuration.Add(fixture.Tables);
            var session = await agent.SynchronizeAsync();
            if (session.TotalChangesDownloaded != 0)
            {

            }
            Assert.Equal(0, session.TotalChangesDownloaded);
            Assert.Equal(0, session.TotalChangesUploaded);
        }

        [Theory, ClassData(typeof(InlineConfigurations)), TestPriority(3)]
        public async Task InsertFromServer(SyncConfiguration conf)
        {
            var insertRowScript =
                $@"INSERT INTO ""ServiceTickets"" (""ServiceTicketID"", ""Title"", ""Description"", ""StatusValue"", ""EscalationLevel"", ""Opened"", ""Closed"", ""CustomerID"") 
                VALUES (public.uuid_generate_v4(), 'Insert One Row', 'Description Insert One Row', 1, 0, now(), NULL, 1)";

            using (var sqlConnection = new NpgsqlConnection(fixture.ServerConnectionString))
            {
                using (var sqlCmd = new NpgsqlCommand(insertRowScript, sqlConnection))
                {
                    sqlConnection.Open();
                    sqlCmd.ExecuteNonQuery();
                    sqlConnection.Close();
                }
            }
            agent.Configuration = conf;
            agent.Configuration.Add(fixture.Tables);
            var session = await agent.SynchronizeAsync();

            Assert.Equal(1, session.TotalChangesDownloaded);
            Assert.Equal(0, session.TotalChangesUploaded);
        }

        [Theory, ClassData(typeof(InlineConfigurations)), TestPriority(4)]
        public async Task InsertFromClient(SyncConfiguration conf)
        {
            Guid newId = Guid.NewGuid();

            var insertRowScript =
                $@"INSERT INTO ServiceTickets (ServiceTicketID, Title, Description, StatusValue, EscalationLevel, Opened, Closed, CustomerID) 
                VALUES ('{newId.ToString()}', 'Insert One Row in SQLite client', 'Description Insert One Row', 1, 0, datetime('now'), NULL, 1)";

            int nbRowsInserted = 0;

            using (var sqlConnection = new SqliteConnection($"Data Source={fixture.ClientSqliteDb}"))
            {
                using (var sqlCmd = new SqliteCommand(insertRowScript, sqlConnection))
                {
                    sqlConnection.Open();
                    nbRowsInserted = sqlCmd.ExecuteNonQuery();
                    sqlConnection.Close();
                }
            }
            if (nbRowsInserted < 0)
                throw new Exception("Row not inserted");

            agent.Configuration = conf;
            agent.Configuration.Add(fixture.Tables);
            var session = await agent.SynchronizeAsync();

            Assert.Equal(0, session.TotalChangesDownloaded);
            Assert.Equal(1, session.TotalChangesUploaded);
        }

        [Theory, ClassData(typeof(InlineConfigurations)), TestPriority(5)]
        public async Task UpdateFromClient(SyncConfiguration conf)
        {
            Guid newId = Guid.NewGuid();

            var insertRowScript =
            $@"INSERT INTO ServiceTickets (ServiceTicketID, Title, Description, StatusValue, EscalationLevel, Opened, Closed, CustomerID) 
                VALUES ('{newId.ToString()}', 'Insert One Row in SQLite client', 'Description Insert One Row', 1, 0, datetime('now'), NULL, 1)";

            using (var sqlConnection = new SqliteConnection($"Data Source={fixture.ClientSqliteDb}"))
            {
                using (var sqlCmd = new SqliteCommand(insertRowScript, sqlConnection))
                {
                    sqlConnection.Open();
                    sqlCmd.ExecuteNonQuery();
                    sqlConnection.Close();
                }
            }
            agent.Configuration = conf;
            agent.Configuration.Add(fixture.Tables);
            var session = await agent.SynchronizeAsync();

            Assert.Equal(0, session.TotalChangesDownloaded);
            Assert.Equal(1, session.TotalChangesUploaded);

            var updateRowScript =
            $@" Update ServiceTickets Set Title = 'Updated from SQLite Client side !' Where ServiceTicketId = '{newId.ToString()}'";

            using (var sqlConnection = new SqliteConnection($"Data Source={fixture.ClientSqliteDb}"))
            {
                using (var sqlCmd = new SqliteCommand(updateRowScript, sqlConnection))
                {
                    sqlConnection.Open();
                    sqlCmd.ExecuteNonQuery();
                    sqlConnection.Close();
                }
            }
            agent.Configuration = conf;
            agent.Configuration.Add(fixture.Tables);
            session = await agent.SynchronizeAsync();

            Assert.Equal(0, session.TotalChangesDownloaded);
            Assert.Equal(1, session.TotalChangesUploaded);
        }


        [Theory, ClassData(typeof(InlineConfigurations)), TestPriority(6)]
        public async Task UpdateFromServer(SyncConfiguration conf)
        {
            Guid guid = Guid.NewGuid();
            var updateRowScript =
                $@"Update ""ServiceTickets"" Set ""Title"" = 'Updated from server {guid.ToString()}' Where ""ServiceTicketID"" = (SELECT ""ServiceTicketID"" FROM ""ServiceTickets"" LIMIT 1)";

            using (var sqlConnection = new NpgsqlConnection(fixture.ServerConnectionString))
            {
                using (var sqlCmd = new NpgsqlCommand(updateRowScript, sqlConnection))
                {
                    sqlConnection.Open();
                    sqlCmd.ExecuteNonQuery();
                    sqlConnection.Close();
                }
            }

            var serverProvider = new PostgreSqlSyncProvider(fixture.ServerConnectionString);
            var clientProvider = new SqliteSyncProvider(fixture.ClientSqliteDb);
            //var simpleConfiguration = new SyncConfiguration(Tables);

            var agent = new SyncAgent(clientProvider, serverProvider);

            agent.Configuration = conf;
            agent.Configuration.Add(fixture.Tables);
            var session = await agent.SynchronizeAsync();

            Assert.Equal(1, session.TotalChangesDownloaded);
            Assert.Equal(0, session.TotalChangesUploaded);
        }

        [Theory, ClassData(typeof(InlineConfigurations)), TestPriority(7)]
        public async Task DeleteFromServer(SyncConfiguration conf)
        {
            var updateRowScript =
                $@"Delete From ""ServiceTickets"" Where ""ServiceTicketID"" = (SELECT ""ServiceTicketID"" FROM ""ServiceTickets"" LIMIT 1)";

            using (var sqlConnection = new NpgsqlConnection(fixture.ServerConnectionString))
            {
                using (var sqlCmd = new NpgsqlCommand(updateRowScript, sqlConnection))
                {
                    sqlConnection.Open();
                    sqlCmd.ExecuteNonQuery();
                    sqlConnection.Close();
                }
            }
            agent.Configuration = conf;
            agent.Configuration.Add(fixture.Tables);
            var session = await agent.SynchronizeAsync();

            Assert.Equal(1, session.TotalChangesDownloaded);
            Assert.Equal(0, session.TotalChangesUploaded);
        }

        [Theory, ClassData(typeof(InlineConfigurations)), TestPriority(8)]
        public async Task DeleteFromClient(SyncConfiguration conf)
        {
            long count;
            var selectcount = $@"Select count(*) From ""ServiceTickets""";
            var updateRowScript = $@"Delete From ServiceTickets";

            using (var sqlConnection = new SqliteConnection($"Data Source={fixture.ClientSqliteDb}"))
            {
                sqlConnection.Open();
                using (var sqlCmd = new SqliteCommand(selectcount, sqlConnection))
                    count = (long)sqlCmd.ExecuteScalar();
                using (var sqlCmd = new SqliteCommand(updateRowScript, sqlConnection))
                    sqlCmd.ExecuteNonQuery();
                sqlConnection.Close();
            }

            agent.Configuration = conf;
            agent.Configuration.Add(fixture.Tables);
            var session = await agent.SynchronizeAsync();

            Assert.Equal(0, session.TotalChangesDownloaded);
            Assert.Equal(count, session.TotalChangesUploaded);

            // check all rows deleted on server side
            using (var sqlConnection = new NpgsqlConnection(fixture.ServerConnectionString))
            {
                sqlConnection.Open();
                using (var sqlCmd = new NpgsqlCommand(selectcount, sqlConnection))
                    count = (long)sqlCmd.ExecuteScalar();
            }
            Assert.Equal(0, count);
        }

        [Theory, ClassData(typeof(InlineConfigurations)), TestPriority(9)]
        public async Task ConflictInsertInsertServerWins(SyncConfiguration conf)
        {
            Guid insertConflictId = Guid.NewGuid();

            using (var sqlConnection = new SqliteConnection($"Data Source={fixture.ClientSqliteDb}"))
            {
                var script = $@"INSERT INTO ServiceTickets
                            (ServiceTicketID, Title, Description, StatusValue, EscalationLevel, Opened, Closed, CustomerID) 
                            VALUES 
                            (@pk, 'Conflict Line Client', 'Description client', 1, 0, datetime('now'), NULL, 1)";

                using (var sqlCmd = new SqliteCommand(script, sqlConnection))
                {
                    sqlCmd.Parameters.Add(new SqliteParameter("pk", insertConflictId));
                    sqlConnection.Open();
                    sqlCmd.ExecuteNonQuery();
                    sqlConnection.Close();
                }
            }

            using (var sqlConnection = new NpgsqlConnection(fixture.ServerConnectionString))
            {
                var script = $@"INSERT INTO ""ServiceTickets""
                            (""ServiceTicketID"", ""Title"", ""Description"", ""StatusValue"", ""EscalationLevel"", ""Opened"", ""Closed"", ""CustomerID"") 
                            VALUES 
                            ('{insertConflictId.ToString()}', 'Conflict Line Server', 'Description client', 1, 0, now(), NULL, 1)";

                using (var sqlCmd = new NpgsqlCommand(script, sqlConnection))
                {
                    sqlConnection.Open();
                    sqlCmd.ExecuteNonQuery();
                    sqlConnection.Close();
                }
            }

            agent.Configuration = conf;
            agent.Configuration.Add(fixture.Tables);
            var session = await agent.SynchronizeAsync();

            // check statistics
            Assert.Equal(1, session.TotalChangesDownloaded);
            Assert.Equal(1, session.TotalChangesUploaded);
            Assert.Equal(1, session.TotalSyncConflicts);

            string expectedRes = string.Empty;
            using (var sqlConnection = new SqliteConnection($"Data Source={fixture.ClientSqliteDb}"))
            {
                var script = $@"Select Title from ServiceTickets Where ServiceTicketID=@pk";

                using (var sqlCmd = new SqliteCommand(script, sqlConnection))
                {
                    sqlCmd.Parameters.Add(new SqliteParameter("pk", insertConflictId));
                    sqlConnection.Open();
                    expectedRes = sqlCmd.ExecuteScalar() as string;
                    sqlConnection.Close();
                }
            }

            // check good title on client
            Assert.Equal("Conflict Line Server", expectedRes);
        }

        [Theory, ClassData(typeof(InlineConfigurations)), TestPriority(10)]
        public async Task ConflictUpdateUpdateServerWins(SyncConfiguration conf)
        {
            Guid updateConflictId = Guid.NewGuid();
            using (var sqlConnection = new SqliteConnection($"Data Source={fixture.ClientSqliteDb}"))
            {
                var script = $@"INSERT INTO ServiceTickets 
                            (ServiceTicketID, Title, Description, StatusValue, EscalationLevel, Opened, Closed, CustomerID) 
                            VALUES 
                            (@pk, 'Line Client', 'Description client', 1, 0, datetime('now'), NULL, 1)";

                using (var sqlCmd = new SqliteCommand(script, sqlConnection))
                {
                    sqlCmd.Parameters.Add(new SqliteParameter("pk", updateConflictId));
                    sqlConnection.Open();
                    sqlCmd.ExecuteNonQuery();
                    sqlConnection.Close();
                }
            }

            agent.Configuration = conf;
            agent.Configuration.Add(fixture.Tables);
            var session = await agent.SynchronizeAsync();

            //just check, even if it's not the real test :)
            // check statistics
            Assert.Equal(0, session.TotalChangesDownloaded);
            Assert.Equal(1, session.TotalChangesUploaded);
            Assert.Equal(0, session.TotalSyncConflicts);


            using (var sqlConnection = new SqliteConnection($"Data Source={fixture.ClientSqliteDb}"))
            {
                var script = $@"Update ServiceTickets 
                                Set Title = 'Updated from Client'
                                Where ServiceTicketId = @pk";

                using (var sqlCmd = new SqliteCommand(script, sqlConnection))
                {
                    sqlCmd.Parameters.Add(new SqliteParameter("pk", updateConflictId));
                    sqlConnection.Open();
                    sqlCmd.ExecuteNonQuery();
                    sqlConnection.Close();
                }
            }

            using (var sqlConnection = new NpgsqlConnection(fixture.ServerConnectionString))
            {
                var script = $@"Update ""ServiceTickets"" 
                                Set ""Title"" = 'Updated from Server'
                                Where ""ServiceTicketID"" = '{updateConflictId.ToString()}'";

                using (var sqlCmd = new NpgsqlCommand(script, sqlConnection))
                {
                    sqlConnection.Open();
                    sqlCmd.ExecuteNonQuery();
                    sqlConnection.Close();
                }
            }

            session = await agent.SynchronizeAsync();

            // check statistics
            Assert.Equal(1, session.TotalChangesDownloaded);
            Assert.Equal(1, session.TotalChangesUploaded);
            Assert.Equal(1, session.TotalSyncConflicts);

            string expectedRes = string.Empty;
            using (var sqlConnection = new SqliteConnection($"Data Source={fixture.ClientSqliteDb}"))
            {
                var script = $@"Select Title from ServiceTickets Where ServiceTicketID=@pk";

                using (var sqlCmd = new SqliteCommand(script, sqlConnection))
                {
                    sqlCmd.Parameters.Add(new SqliteParameter("pk", updateConflictId));
                    sqlConnection.Open();
                    expectedRes = sqlCmd.ExecuteScalar() as string;
                    sqlConnection.Close();
                }
            }

            // check good title on client
            Assert.Equal("Updated from Server", expectedRes);
        }

        [Theory, ClassData(typeof(InlineConfigurations)), TestPriority(11)]
        public async Task ConflictUpdateUpdateClientWins(SyncConfiguration conf)
        {
            var id = Guid.NewGuid();

            using (var sqlConnection = new SqliteConnection($"Data Source={fixture.ClientSqliteDb}"))
            {
                var script = $@"INSERT INTO ServiceTickets 
                            (ServiceTicketID, Title, Description, StatusValue, EscalationLevel, Opened, Closed, CustomerID) 
                            VALUES 
                            (@pk, 'Line for conflict', 'Description client', 1, 0, datetime('now'), NULL, 1)";

                using (var sqlCmd = new SqliteCommand(script, sqlConnection))
                {
                    sqlCmd.Parameters.Add(new SqliteParameter("pk", id));
                    sqlConnection.Open();
                    sqlCmd.ExecuteNonQuery();
                    sqlConnection.Close();
                }
            }

            agent.Configuration = conf;
            agent.Configuration.Add(fixture.Tables);
            var session = await agent.SynchronizeAsync();

            //just check, even if it's not the real test :)
            // check statistics
            Assert.Equal(0, session.TotalChangesDownloaded);
            Assert.Equal(1, session.TotalChangesUploaded);
            Assert.Equal(0, session.TotalSyncConflicts);


            using (var sqlConnection = new SqliteConnection($"Data Source={fixture.ClientSqliteDb}"))
            {
                var script = $@"Update ServiceTickets 
                                Set Title = 'Updated from Client'
                                Where ServiceTicketId = @pk";

                using (var sqlCmd = new SqliteCommand(script, sqlConnection))
                {
                    sqlCmd.Parameters.Add(new SqliteParameter("pk", id));
                    sqlConnection.Open();
                    sqlCmd.ExecuteNonQuery();
                    sqlConnection.Close();
                }
            }

            using (var sqlConnection = new NpgsqlConnection(fixture.ServerConnectionString))
            {
                var script = $@"Update ""ServiceTickets"" 
                                Set ""Title"" = 'Updated from Server'
                                Where ""ServiceTicketID"" = '{id}'";

                using (var sqlCmd = new NpgsqlCommand(script, sqlConnection))
                {
                    sqlConnection.Open();
                    sqlCmd.ExecuteNonQuery();
                    sqlConnection.Close();
                }
            }

            agent.ApplyChangedFailed += (s, args) => args.Action = ConflictAction.ClientWins;

            await Assert.RaisesAsync<ApplyChangeFailedEventArgs>(
                h => agent.ApplyChangedFailed += h,
                h => agent.ApplyChangedFailed -= h, async () =>
                {
                    session = await agent.SynchronizeAsync();
                });

            // check statistics
            Assert.Equal(0, session.TotalChangesDownloaded);
            Assert.Equal(1, session.TotalChangesUploaded);
            Assert.Equal(1, session.TotalSyncConflicts);

            string expectedRes = string.Empty;
            using (var sqlConnection = new NpgsqlConnection(fixture.ServerConnectionString))
            {
                var script = $@"Select ""Title"" from ""ServiceTickets"" Where ""ServiceTicketID""='{id}'";

                using (var sqlCmd = new NpgsqlCommand(script, sqlConnection))
                {
                    sqlConnection.Open();
                    expectedRes = sqlCmd.ExecuteScalar() as string;
                    sqlConnection.Close();
                }
            }

            // check good title on client
            Assert.Equal("Updated from Client", expectedRes);
        }


        [Theory, ClassData(typeof(InlineConfigurations)), TestPriority(12)]
        public async Task ConflictInsertInsertConfigurationClientWins(SyncConfiguration conf)
        {

            Guid id = Guid.NewGuid();

            using (var sqlConnection = new SqliteConnection($"Data Source={fixture.ClientSqliteDb}"))
            {
                var script = $@"INSERT INTO ServiceTickets 
                            (ServiceTicketID, Title, Description, StatusValue, EscalationLevel, Opened, Closed, CustomerID) 
                            VALUES 
                            (@pk, 'Conflict Line Client', 'Description client', 1, 0, datetime('now'), NULL, 1)";

                using (var sqlCmd = new SqliteCommand(script, sqlConnection))
                {
                    sqlCmd.Parameters.Add(new SqliteParameter("pk", id));
                    sqlConnection.Open();
                    sqlCmd.ExecuteNonQuery();
                    sqlConnection.Close();
                }
            }

            using (var sqlConnection = new NpgsqlConnection(fixture.ServerConnectionString))
            {
                var script = $@"INSERT INTO ""ServiceTickets"" 
                            (""ServiceTicketID"", ""Title"", ""Description"", ""StatusValue"", ""EscalationLevel"", ""Opened"", ""Closed"", ""CustomerID"") 
                            VALUES 
                            ('{id.ToString()}', 'Conflict Line Server', 'Description client', 1, 0, now(), NULL, 1)";

                using (var sqlCmd = new NpgsqlCommand(script, sqlConnection))
                {
                    sqlConnection.Open();
                    sqlCmd.ExecuteNonQuery();
                    sqlConnection.Close();
                }
            }

            agent.Configuration = conf;
            agent.Configuration.Add(fixture.Tables);
            agent.Configuration.ConflictResolutionPolicy = ConflictResolutionPolicy.ClientWins;
            var session = await agent.SynchronizeAsync();

            // check statistics
            Assert.Equal(0, session.TotalChangesDownloaded);
            Assert.Equal(1, session.TotalChangesUploaded);
            Assert.Equal(1, session.TotalSyncConflicts);

            string expectedRes = string.Empty;
            using (var sqlConnection = new NpgsqlConnection(fixture.ServerConnectionString))
            {
                var script = $@"Select ""Title"" from ""ServiceTickets"" Where ""ServiceTicketID""='{id.ToString()}'";

                using (var sqlCmd = new NpgsqlCommand(script, sqlConnection))
                {
                    sqlConnection.Open();
                    expectedRes = sqlCmd.ExecuteScalar() as string;
                    sqlConnection.Close();
                }
            }

            // check good title on client
            Assert.Equal("Conflict Line Client", expectedRes);
        }

        [Theory, ClassData(typeof(InlineConfigurations)), TestPriority(12)]
        public async Task InsertUpdateDeleteFromServer(SyncConfiguration conf)
        {
            Guid insertedId = Guid.NewGuid();
            Guid updatedId = Guid.NewGuid();
            Guid deletedId = Guid.NewGuid();


            var script =
            $@"INSERT INTO ""ServiceTickets"" (""ServiceTicketID"", ""Title"", ""Description"", ""StatusValue"", ""EscalationLevel"", ""Opened"", ""Closed"", ""CustomerID"") 
               VALUES ('{updatedId.ToString()}', 'Updated', 'Description', 1, 0, now(), NULL, 1);
               INSERT INTO ""ServiceTickets""(""ServiceTicketID"", ""Title"", ""Description"", ""StatusValue"", ""EscalationLevel"", ""Opened"", ""Closed"", ""CustomerID"") 
               VALUES('{deletedId.ToString()}', 'Deleted', 'Description', 1, 0, now(), NULL, 1)";

            using (var sqlConnection = new NpgsqlConnection(fixture.ServerConnectionString))
            {
                using (var sqlCmd = new NpgsqlCommand(script, sqlConnection))
                {
                    sqlConnection.Open();
                    sqlCmd.ExecuteNonQuery();
                    sqlConnection.Close();
                }
            }
            agent.Configuration = conf;
            agent.Configuration.Add(fixture.Tables);
            var session = await agent.SynchronizeAsync();

            Assert.Equal(2, session.TotalChangesDownloaded);
            Assert.Equal(0, session.TotalChangesUploaded);

            script =
               $@"INSERT INTO ""ServiceTickets"" (""ServiceTicketID"", ""Title"", ""Description"", ""StatusValue"", ""EscalationLevel"", ""Opened"", ""Closed"", ""CustomerID"") 
               VALUES ('{insertedId.ToString()}', 'Inserted', 'Description', 1, 0, now(), NULL, 1);
               DELETE FROM ""ServiceTickets"" WHERE ""ServiceTicketID"" = '{deletedId.ToString()}';
               UPDATE ""ServiceTickets"" set ""Description"" = 'Updated again' WHERE  ""ServiceTicketID"" = '{updatedId.ToString()}';";

            using (var sqlConnection = new NpgsqlConnection(fixture.ServerConnectionString))
            {
                using (var sqlCmd = new NpgsqlCommand(script, sqlConnection))
                {
                    sqlConnection.Open();
                    sqlCmd.ExecuteNonQuery();
                    sqlConnection.Close();
                }
            }

            int insertApplied = 0;
            int updateApplied = 0;
            int deleteApplied = 0;
            agent.TableChangesApplied += (sender, args) =>
            {
                switch (args.TableChangesApplied.State)
                {
                    case Data.DmRowState.Added:
                        insertApplied += args.TableChangesApplied.Applied;
                        break;
                    case Data.DmRowState.Modified:
                        updateApplied += args.TableChangesApplied.Applied;
                        break;
                    case Data.DmRowState.Deleted:
                        deleteApplied += args.TableChangesApplied.Applied;
                        break;
                }
            };

            session = await agent.SynchronizeAsync();

            Assert.Equal(3, session.TotalChangesDownloaded);
            Assert.Equal(1, insertApplied);
            Assert.Equal(1, updateApplied);
            Assert.Equal(1, deleteApplied);
            Assert.Equal(0, session.TotalChangesUploaded);

        }
    }
}
