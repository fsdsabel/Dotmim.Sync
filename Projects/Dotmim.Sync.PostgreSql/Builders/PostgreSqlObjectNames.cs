using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Data;

namespace Dotmim.Sync.PostgreSql.Builders
{
    public class PostgreSqlObjectNames
    {
        public const string TimestampValue = "ROUND(EXTRACT(epoch FROM now()) * 100000)";

        internal const string insertTriggerName = "\"{0}_insert_trigger\"";
        internal const string updateTriggerName = "\"{0}_update_trigger\"";
        internal const string deleteTriggerName = "\"{0}_delete_trigger\"";

        internal const string selectChangesProcName = "\"{0}_selectchanges\"";
        internal const string selectChangesProcNameWithFilters = "\"{0}_{1}_selectchanges\"";
        internal const string selectRowProcName = "\"{0}_selectrow\"";

        internal const string insertProcName = "\"{0}_insert\"";
        internal const string updateProcName = "\"{0}_update\"";
        internal const string deleteProcName = "\"{0}_delete\"";

        internal const string resetProcName = "\"{0}_reset\"";

        internal const string insertMetadataProcName = "\"{0}_insertmetadata\"";
        internal const string updateMetadataProcName = "\"{0}_updatemetadata\"";
        internal const string deleteMetadataProcName = "\"{0}_deletemetadata\"";


        private Dictionary<DbCommandType, String> names = new Dictionary<DbCommandType, string>();
        private ObjectNameParser tableName, trackingName;

        public DmTable TableDescription { get; }


        public void AddName(DbCommandType objectType, string name)
        {
            if (names.ContainsKey(objectType))
                throw new Exception("Yous can't add an objectType multiple times");

            names.Add(objectType, name);
        }
        public string GetCommandName(DbCommandType objectType, IEnumerable<string> adds = null)
        {
            if (!names.ContainsKey(objectType))
                throw new NotSupportedException($"MySql provider does not support the command type {objectType.ToString()}");

            return names[objectType];
        }

        public PostgreSqlObjectNames(DmTable tableDescription)
        {
            this.TableDescription = tableDescription;
            (tableName, trackingName) = PostgreSqlBuilder.GetParsers(this.TableDescription);

            SetDefaultNames();
        }

        /// <summary>
        /// Set the default stored procedures names
        /// </summary>
        private void SetDefaultNames()
        {
            this.AddName(DbCommandType.InsertTrigger, string.Format(insertTriggerName, tableName.UnquotedStringWithUnderScore));
            this.AddName(DbCommandType.UpdateTrigger, string.Format(updateTriggerName, tableName.UnquotedStringWithUnderScore));
            this.AddName(DbCommandType.DeleteTrigger, string.Format(deleteTriggerName, tableName.UnquotedStringWithUnderScore));

            this.AddName(DbCommandType.SelectChanges, string.Format(selectChangesProcName, tableName.UnquotedStringWithUnderScore));
            this.AddName(DbCommandType.SelectChangesWitFilters, string.Format(selectChangesProcNameWithFilters, tableName.UnquotedStringWithUnderScore, "{0}"));
            this.AddName(DbCommandType.SelectRow, string.Format(selectRowProcName, tableName.UnquotedStringWithUnderScore));
            this.AddName(DbCommandType.InsertRow, string.Format(insertProcName, tableName.UnquotedStringWithUnderScore));
            this.AddName(DbCommandType.UpdateRow, string.Format(updateProcName, tableName.UnquotedStringWithUnderScore));
            this.AddName(DbCommandType.DeleteRow, string.Format(deleteProcName, tableName.UnquotedStringWithUnderScore));
            this.AddName(DbCommandType.InsertMetadata, string.Format(insertMetadataProcName, tableName.UnquotedStringWithUnderScore));
            this.AddName(DbCommandType.UpdateMetadata, string.Format(updateMetadataProcName, tableName.UnquotedStringWithUnderScore));
            this.AddName(DbCommandType.DeleteMetadata, string.Format(deleteMetadataProcName, tableName.UnquotedStringWithUnderScore));
            this.AddName(DbCommandType.Reset, string.Format(resetProcName, tableName.UnquotedStringWithUnderScore));

            //// Select changes
            //this.CreateSelectChangesCommandText();
            //this.CreateSelectRowCommandText();
            //this.CreateDeleteCommandText();
            //this.CreateDeleteMetadataCommandText();
            //this.CreateInsertCommandText();
            //this.CreateInsertMetadataCommandText();
            //this.CreateUpdateCommandText();
            //this.CreateUpdatedMetadataCommandText();

        }


    }
}
