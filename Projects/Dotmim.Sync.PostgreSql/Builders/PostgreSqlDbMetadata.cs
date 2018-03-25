using System;
using System.Data;
using Dotmim.Sync.Data;
using Dotmim.Sync.Manager;
using NpgsqlTypes;

namespace Dotmim.Sync.PostgreSql.Builders
{
    public class PostgreSqlDbMetadata : DbMetadata
    {
        public override int GetMaxLengthFromDbType(DbType dbType, int maxLength)
        {
            var typeName = GetStringFromDbType(dbType);
            if (IsTextType(typeName))
            {
                string lowerType = typeName.ToLowerInvariant();
                switch (lowerType)
                {
                    case "varchar":
                    case "char":
                    case "text":
                    case "nchar":
                    case "nvarchar":
                    case "enum":
                    case "set":
                        if (maxLength > 0)
                            return maxLength;
                        else
                            return 0;
                }
                return 0;
            }
            return 0;
        }

        public override int GetMaxLengthFromOwnerDbType(object dbType, int maxLength)
        {
            var typeName = GetStringFromOwnerDbType(dbType);
            if (IsTextType(typeName))
            {
                string lowerType = typeName.ToLowerInvariant();
                switch (lowerType)
                {
                    case "varchar":
                    case "char":
                    case "text":
                    case "nchar":
                    case "nvarchar":
                    case "enum":
                    case "set":
                        if (maxLength > 0)
                            return maxLength;
                        else
                            return 0;
                }
                return 0;
            }
            return 0;
        }

        public override object GetOwnerDbTypeFromDbType(DbType dbType)
        {
            
            switch (dbType)
            {

                case DbType.AnsiString:
                    return NpgsqlDbType.Text;
                case DbType.StringFixedLength:
                case DbType.AnsiStringFixedLength:
                    return NpgsqlDbType.Varchar;
                case DbType.Binary:
                    return NpgsqlDbType.Bytea;
                case DbType.Boolean:
                    return NpgsqlDbType.Boolean;
                case DbType.Byte:
                    return NpgsqlDbType.Smallint;
                case DbType.Currency:
                    return NpgsqlDbType.Money;
                case DbType.Date:
                    return NpgsqlDbType.Date;
                case DbType.DateTime:
                case DbType.DateTime2:
                case DbType.DateTimeOffset:
                    return NpgsqlDbType.Timestamp;
                case DbType.Decimal:
                    return NpgsqlDbType.Numeric;
                case DbType.Double:
                    return NpgsqlDbType.Double;
                case DbType.Guid:
                    return NpgsqlDbType.Uuid;
                case DbType.Int16:
                    return NpgsqlDbType.Smallint;
                case DbType.Int32:
                    return NpgsqlDbType.Integer;
                case DbType.Int64:
                    return NpgsqlDbType.Bigint;
                case DbType.Object:
                    return NpgsqlDbType.Bytea;
                case DbType.SByte:
                    return NpgsqlDbType.Smallint;
                case DbType.Single:
                    return NpgsqlDbType.Double;
                case DbType.String:
                    return NpgsqlDbType.Text;
                case DbType.Time:
                    return NpgsqlDbType.Time;
                case DbType.UInt16:
                    return NpgsqlDbType.Integer;
                case DbType.UInt32:
                    return NpgsqlDbType.Bigint;
                case DbType.UInt64:
                    return NpgsqlDbType.Bigint;
                case DbType.VarNumeric:
                    return NpgsqlDbType.Numeric;
                case DbType.Xml:
                    return NpgsqlDbType.Xml;
            }
            throw new Exception($"this type {dbType} is not supported");
        }

        public override (byte precision, byte scale) GetPrecisionFromDbType(DbType dbType, byte precision, byte scale)
        {
            var typeName = GetStringFromDbType(dbType);
            if (IsNumericType(typeName) && precision == 0)
            {
                precision = 10;
                scale = 0;
            }
            if (!SupportScale(typeName) || scale == 0)
                return (0, 0);

            return (precision, scale);
        }

        public override (byte precision, byte scale) GetPrecisionFromOwnerDbType(object dbType, byte precision, byte scale)
        {
            var typeName = GetStringFromOwnerDbType(dbType);
            if (IsNumericType(typeName) && precision == 0)
            {
                precision = 10;
                scale = 0;
            }
            if (!SupportScale(typeName) || scale == 0)
                return (0, 0);

            return (precision, scale);
        }

        public override string GetPrecisionStringFromDbType(DbType dbType, int maxLength, byte precision, byte scale)
        {
            if (dbType == DbType.Guid)
                return string.Empty;

            var typeName = GetStringFromDbType(dbType);
            if (IsTextType(typeName))
            {
                string lowerType = typeName.ToLowerInvariant();
                switch (lowerType)
                {
                    case "varchar":
                        if (maxLength > 0)
                            return $"({maxLength})";
                        else
                            return string.Empty;
                }
                return string.Empty;
            }

            if (IsNumericType(typeName) && precision == 0)
            {
                precision = 10;
                scale = 0;
            }
            if (SupportScale(typeName) && scale == 0)
                return String.Format("({0})", precision);

            if (!SupportScale(typeName))
                return string.Empty;

            return String.Format("({0},{1})", precision, scale);
        }

        public override string GetPrecisionStringFromOwnerDbType(object dbType, int maxLength, byte precision, byte scale)
        {
            NpgsqlDbType npgsqlDbType = (NpgsqlDbType)dbType;

            if (npgsqlDbType == NpgsqlDbType.Uuid)
                return string.Empty;

            var typeName = GetStringFromOwnerDbType(dbType);

            if (IsTextType(typeName))
            {
                string lowerType = typeName.ToLowerInvariant();
                switch (lowerType)
                {
                    case "varchar":
                    case "char":
                    case "text":
                    case "nchar":
                    case "nvarchar":
                    case "enum":
                    case "set":
                        if (maxLength > 0)
                            return $"({maxLength})";
                        else
                            return string.Empty;
                }
                return string.Empty;
            }

            if (IsNumericType(typeName) && precision == 0)
            {
                precision = 10;
                scale = 0;
            }
            if (SupportScale(typeName) && scale == 0)
                return String.Format("({0})", precision);

            if (!SupportScale(typeName))
                return string.Empty;

            return String.Format("({0},{1})", precision, scale);
        }

        public override string GetStringFromDbType(DbType dbType)
        {
            string postgreSqlType = string.Empty;
            switch (dbType)
            {
                case DbType.Binary:
                    postgreSqlType = "BYTEA";
                    break;
                case DbType.Boolean:
                    postgreSqlType = "BOOL";
                    break;
                case DbType.Byte:
                case DbType.SByte:
                    postgreSqlType = "SMALLINT";
                    break;
                case DbType.Time:
                    postgreSqlType = "TIME";
                    break;
                case DbType.Date:
                    postgreSqlType = "DATE";
                    break;
                case DbType.DateTime:
                case DbType.DateTime2:
                case DbType.DateTimeOffset:
                    postgreSqlType = "TIMESTAMP";
                    break;
                case DbType.Currency:
                    postgreSqlType = "MONEY";
                    break;
                case DbType.Double:
                    postgreSqlType = "FLOAT8";
                    break;
                case DbType.Single:
                    postgreSqlType = "FLOAT4";
                    break;
                case DbType.Decimal:
                case DbType.VarNumeric:
                    postgreSqlType = "NUMERIC";
                    break;
                case DbType.Int16:
                case DbType.UInt16:
                    postgreSqlType = "SMALLINT";
                    break;
                case DbType.Int32:
                case DbType.UInt32:
                    postgreSqlType = "INT";
                    break;
                case DbType.Int64:
                case DbType.UInt64:
                    postgreSqlType = "BIGINT";
                    break;
                case DbType.String:
                case DbType.AnsiString:
                    postgreSqlType = "TEXT";
                    break;
                case DbType.Xml:
                    postgreSqlType = "XML";
                    break;
                case DbType.StringFixedLength:
                case DbType.AnsiStringFixedLength:
                    postgreSqlType = "VARCHAR";
                    break;
                case DbType.Guid:
                    postgreSqlType = "UUID";
                    break;
                case DbType.Object:
                    postgreSqlType = "BYTEA";
                    break;
            }

            if (string.IsNullOrEmpty(postgreSqlType))
                throw new Exception($"sqltype not valid");

            return postgreSqlType;
        }

        public override string GetStringFromOwnerDbType(object ownerType)
        {
            NpgsqlDbType sqlDbType = (NpgsqlDbType)ownerType;

            switch (sqlDbType)
            {
                case NpgsqlDbType.Bigint:
                    return "BIGINT";
                case NpgsqlDbType.Bit:
                    return "BIT";
                case NpgsqlDbType.Boolean:
                    return "BOOL";
                case NpgsqlDbType.Bytea:
                    return "BYTEA";
                case NpgsqlDbType.Char:
                    return "CHAR";
                case NpgsqlDbType.Date:
                    return "DATE";
                case NpgsqlDbType.Double:
                    return "FLOAT8";
                case NpgsqlDbType.Geometry:
                    return "GEOMETRY";
                case NpgsqlDbType.Integer:
                    return "INT";
                case NpgsqlDbType.Money:
                    return "MONEY";
                case NpgsqlDbType.Numeric:
                    return "NUMERIC";
                case NpgsqlDbType.Real:
                    return "FLOAT4";
                case NpgsqlDbType.Time:
                    return "TIME";
                case NpgsqlDbType.Timestamp:
                    return "TIMESTAMP";
                case NpgsqlDbType.Text:
                    return "TEXT";
                case NpgsqlDbType.Varchar:
                    return "VARCHAR";
                case NpgsqlDbType.Uuid:
                    return "UUID";
                case NpgsqlDbType.Xml:
                    return "XML";
            }
            throw new Exception("Unhandled type encountered");
        }

        public override bool IsNumericType(string typeName)
        {
            string lowerType = typeName.ToLowerInvariant();
            switch (lowerType)
            {
                case "int":
                case "int4":
                case "integer":
                case "int8":
                case "int2":
                case "bigint":
                case "smallint":
                case "serial":
                case "bigserial":
                case "float8":
                case "float4":
                case "real":
                case "smallserial":
                case "serial2":
                case "serial4":
                    return true;
            }
            return false;
        }

        public override bool IsTextType(string typeName)
        {
            string lowerType = typeName.ToLowerInvariant();
            switch (lowerType)
            {
                case "varchar":
                case "char":
                case "text":
                    return true;
            }
            return false;
        }

        public override bool IsValid(DmColumn columnDefinition)
        {
            switch (columnDefinition.OriginalTypeName.ToLowerInvariant())
            {
                case "int":
                case "int4":
                case "integer":
                case "int8":
                case "int2":
                case "bigint":
                case "smallint":
                case "serial":
                case "bigserial":
                case "float8":
                case "float4":
                case "real":
                case "smallserial":
                case "serial2":
                case "serial4":
                case "varchar":
                case "char":
                case "text":
                case "bit":
                case "bytea":
                case "xml":
                case "timestamp":
                case "date":
                case "time":
                case "uuid":
                case "character varying":
                case "timestamp without time zone":
                case "time without time zone":
                    return true;
            }
            return false;
        }

        public override bool SupportScale(string typeName)
        {
            string lowerType = typeName.ToLowerInvariant();
            switch (lowerType)
            {
                case "numeric":
                case "decimal":
                    return true;
            }
            return false;
        }

        public override DbType ValidateDbType(string typeName, bool isUnsigned, bool isUnicode)
        {
            switch (typeName.ToLowerInvariant())
            {
                case "int":
                case "integer":
                case "int4":
                    return isUnsigned ? DbType.UInt32 : DbType.Int32;
                case "int2":
                case "smallint":
                    return isUnsigned ? DbType.UInt16 : DbType.Int16;
                case "int8":
                case "bigint":
                    return isUnsigned ? DbType.UInt64 : DbType.Int64;
                case "bool":
                case "boolean":
                    return DbType.Boolean;
                case "numeric":
                case "decimal":
                    return DbType.VarNumeric;
                case "real":
                case "float4":
                case "double precision":
                case "float8":
                    return DbType.Decimal;
                case "serial":
                case "serial4":
                    return DbType.Int32;
                case "smallserial":
                case "serial2":
                    return DbType.Int16;
                case "bigserial":
                case "serial8":
                    return DbType.Int64;
                case "varchar":
                case "character varying":
                case "text":
                    return isUnicode ? DbType.String : DbType.AnsiString;
                case "date":
                    return DbType.Date;
                case "timestamp":
                case "timestamp without time zone":
                    return DbType.DateTime;
                case "bytea":
                    return DbType.Binary;
                case "time":
                case "time without time zone":
                    return DbType.Time;
                case "xml":
                    return DbType.Xml;
                case "uuid":
                    return DbType.Guid;
            }
            throw new Exception($"this type name {typeName} is not supported");
        }

        public override bool ValidateIsReadonly(DmColumn columnDefinition)
        {
            return columnDefinition.OriginalTypeName.ToLowerInvariant() == "timestamp";
        }

        public override int ValidateMaxLength(string typeName, bool isUnsigned, bool isUnicode, long maxLength)
        {
            Int32 iMaxLength = maxLength > 8000 ? 8000 : Convert.ToInt32(maxLength);
            return iMaxLength;
        }

        public override object ValidateOwnerDbType(string typeName, bool isUnsigned, bool isUnicode)
        {
            switch (typeName.ToUpperInvariant())
            {
                case "TEXT":
                    return NpgsqlDbType.Text;
                case "VARCHAR":
                case "CHARACTER VARYING":
                    return NpgsqlDbType.Varchar;
                case "DATE":
                    return NpgsqlDbType.Date;
                case "TIMESTAMP":
                case "TIMESTAMP WITHOUT TIME ZONE":
                    return NpgsqlDbType.Timestamp;
                case "NUMERIC":
                case "DECIMAL":
                    return NpgsqlDbType.Numeric;
                case "TIME":
                case "TIME WITHOUT TIME ZONE":
                    return NpgsqlDbType.Time;
                case "ENUM":
                    return NpgsqlDbType.Enum;
                case "BIT":
                    return NpgsqlDbType.Bit;
                case "BOOL":
                case "BOOLEAN":
                    return NpgsqlDbType.Boolean;
                case "INT2":
                case "SMALLINT":
                    return NpgsqlDbType.Smallint;
                case "INT":
                case "INTEGER":
                case "INT4":
                    return NpgsqlDbType.Integer;
                case "SERIAL":
                case "SERIAL4":
                    return NpgsqlDbType.Integer;
                case "BIGINT":
                case "INT8":
                    return NpgsqlDbType.Bigint;
                case "FLOAT4":
                case "REAL":
                    return NpgsqlDbType.Real;
                case "DOUBLE PRECISION":
                case "FLOAT8":
                    return NpgsqlDbType.Double;
                case "BYTEA":
                    return NpgsqlDbType.Bytea;
                case "XML":
                    return NpgsqlDbType.Xml;
                case "UUID":
                    return NpgsqlDbType.Uuid;
            }
            throw new Exception("Unhandled type encountered");
        }

        public override byte ValidatePrecision(DmColumn columnDefinition)
        {
            if (IsNumericType(columnDefinition.OriginalTypeName) && columnDefinition.Precision == 0)
                return 10;

            return columnDefinition.Precision;
        }

        public override (byte precision, byte scale) ValidatePrecisionAndScale(DmColumn columnDefinition)
        {
            var precision = columnDefinition.Precision;
            var scale = columnDefinition.Scale;
            if (IsNumericType(columnDefinition.OriginalTypeName) && precision == 0)
            {
                precision = 10;
                scale = 0;
            }

            return (precision, scale);
        }

        public override Type ValidateType(object ownerType)
        {
            NpgsqlDbType sqlDbType = (NpgsqlDbType)ownerType;

            switch (sqlDbType)
            {
                case NpgsqlDbType.Numeric:
                    return typeof(decimal);
                case NpgsqlDbType.Smallint:
                    return typeof(short);
                case NpgsqlDbType.Integer:
                    return typeof(Int32);
                case NpgsqlDbType.Bigint:
                    return typeof(long);
                case NpgsqlDbType.Bit:
                    return typeof(ulong);
                case NpgsqlDbType.Real:
                    return typeof(float);
                case NpgsqlDbType.Double:
                    return typeof(double);
                case NpgsqlDbType.Time:
                    return typeof(TimeSpan);
                case NpgsqlDbType.Date:
                case NpgsqlDbType.Timestamp:
                    return typeof(DateTime);
                case NpgsqlDbType.Enum:
                case NpgsqlDbType.Varchar:
                case NpgsqlDbType.Json:
                case NpgsqlDbType.Xml:
                case NpgsqlDbType.Text:
                    return typeof(string);
                case NpgsqlDbType.Uuid:
                    return typeof(Guid);
                case NpgsqlDbType.Geometry:
                case NpgsqlDbType.Bytea:
                    return typeof(byte[]);
            }
            throw new Exception("Unhandled type encountered");
        }
    }
}
