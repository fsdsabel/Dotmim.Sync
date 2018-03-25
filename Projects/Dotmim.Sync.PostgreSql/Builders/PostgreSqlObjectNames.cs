using System;
using System.Collections.Generic;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Data;

namespace Dotmim.Sync.PostgreSql.Builders
{
    public class PostgreSqlObjectNames
    {
        public const string TimestampValue = "ROUND(EXTRACT(epoch FROM now()) * 100000)";

        internal const string InsertTriggerName = "\"{0}_insert_trigger\"";
        internal const string UpdateTriggerName = "\"{0}_update_trigger\"";
        internal const string DeleteTriggerName = "\"{0}_delete_trigger\"";

        internal const string SelectChangesProcName = "\"{0}_selectchanges\"";
        internal const string SelectChangesProcNameWithFilters = "\"{0}_{1}_selectchanges\"";
        internal const string SelectRowProcName = "\"{0}_selectrow\"";

        internal const string InsertProcName = "\"{0}_insert\"";
        internal const string UpdateProcName = "\"{0}_update\"";
        internal const string DeleteProcName = "\"{0}_delete\"";

        internal const string ResetProcName = "\"{0}_reset\"";

        internal const string InsertMetadataProcName = "\"{0}_insertmetadata\"";
        internal const string UpdateMetadataProcName = "\"{0}_updatemetadata\"";
        internal const string DeleteMetadataProcName = "\"{0}_deletemetadata\"";


        private readonly Dictionary<DbCommandType, string> _names = new Dictionary<DbCommandType, string>();
        private readonly ObjectNameParser _tableName;
        
        public PostgreSqlObjectNames(DmTable tableDescription)
        {
            TableDescription = tableDescription;
            (_tableName, _) = PostgreSqlBuilder.GetParsers(TableDescription);

            SetDefaultNames();
        }

        public DmTable TableDescription { get; }


        public void AddName(DbCommandType objectType, string name)
        {
            if (_names.ContainsKey(objectType))
                throw new Exception("Yous can't add an objectType multiple times");

            _names.Add(objectType, name);
        }

        public string GetCommandName(DbCommandType objectType, IEnumerable<string> adds = null)
        {
            if (!_names.ContainsKey(objectType))
                throw new NotSupportedException(
                    $"MySql provider does not support the command type {objectType.ToString()}");

            return _names[objectType];
        }

        /// <summary>
        ///     Set the default stored procedures names
        /// </summary>
        private void SetDefaultNames()
        {
            AddName(DbCommandType.InsertTrigger,
                string.Format(InsertTriggerName, _tableName.UnquotedStringWithUnderScore));
            AddName(DbCommandType.UpdateTrigger,
                string.Format(UpdateTriggerName, _tableName.UnquotedStringWithUnderScore));
            AddName(DbCommandType.DeleteTrigger,
                string.Format(DeleteTriggerName, _tableName.UnquotedStringWithUnderScore));

            AddName(DbCommandType.SelectChanges,
                string.Format(SelectChangesProcName, _tableName.UnquotedStringWithUnderScore));
            AddName(DbCommandType.SelectChangesWitFilters,
                string.Format(SelectChangesProcNameWithFilters, _tableName.UnquotedStringWithUnderScore, "{0}"));
            AddName(DbCommandType.SelectRow, string.Format(SelectRowProcName, _tableName.UnquotedStringWithUnderScore));
            AddName(DbCommandType.InsertRow, string.Format(InsertProcName, _tableName.UnquotedStringWithUnderScore));
            AddName(DbCommandType.UpdateRow, string.Format(UpdateProcName, _tableName.UnquotedStringWithUnderScore));
            AddName(DbCommandType.DeleteRow, string.Format(DeleteProcName, _tableName.UnquotedStringWithUnderScore));
            AddName(DbCommandType.InsertMetadata,
                string.Format(InsertMetadataProcName, _tableName.UnquotedStringWithUnderScore));
            AddName(DbCommandType.UpdateMetadata,
                string.Format(UpdateMetadataProcName, _tableName.UnquotedStringWithUnderScore));
            AddName(DbCommandType.DeleteMetadata,
                string.Format(DeleteMetadataProcName, _tableName.UnquotedStringWithUnderScore));
            AddName(DbCommandType.Reset, string.Format(ResetProcName, _tableName.UnquotedStringWithUnderScore));

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