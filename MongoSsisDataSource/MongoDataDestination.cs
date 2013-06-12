using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.Dts.Pipeline;
using Microsoft.SqlServer.Dts.Pipeline.Wrapper;
using Microsoft.SqlServer.Dts.Runtime;
using Microsoft.SqlServer.Dts.Runtime.Wrapper;
using MongoDB.Driver;
using MongoDB.Bson;

namespace MongoDataSource
{
    [DtsPipelineComponent(DisplayName = "MongoDB Destination",
        Description = "Mongosis - Loads data into a MongoDB data source",
        ComponentType = ComponentType.DestinationAdapter,
        IconResource = "MongoDataSource.Resources.mongosis.ico"
        )]
    public class MongoDataDestination : PipelineComponent
    {
        #region Constants
        private const string MONGO_CONN = "MongoDB connection string";
        private const string MONGO_DB = "MongoDB database name";
        private const string MONGO_COLL = "MongoDB collection name";
        private const string MONGO_BATCH_SIZE = "MongoDB insert batch size";
        #endregion

        #region private variables
        /// <summary>
        /// Connection manager for the mongo database
        /// </summary>
        private IDTSConnectionManager100 _connMgr;

        /// <summary>
        /// Connection to the mongo database
        /// </summary>
        private MongoDatabase _db;

        /// <summary>
        /// Input columns
        /// </summary>
        private List<DestinationColumnInfo> _colInfos = new List<DestinationColumnInfo>();


        #endregion

        public override void ProvideComponentProperties()
        {
            // Reset the component.
            base.RemoveAllInputsOutputsAndCustomProperties();
            ComponentMetaData.RuntimeConnectionCollection.RemoveAll();

            IDTSInput100 input = ComponentMetaData.InputCollection.New();
            input.Name = "Input";

            // MongoDBConnectionManager
            IDTSRuntimeConnection100 connection = ComponentMetaData.RuntimeConnectionCollection.New();
            connection.Name = MONGO_CONN;

            // Database name
            IDTSCustomProperty100 dbname = ComponentMetaData.CustomPropertyCollection.New();
            dbname.Name = MONGO_DB;

            // Collection name in MongoDB
            IDTSCustomProperty100 collname = ComponentMetaData.CustomPropertyCollection.New();
            collname.Name = MONGO_COLL;

            // How many rows per InsertBatch
            IDTSCustomProperty100 batchSize = ComponentMetaData.CustomPropertyCollection.New();
            batchSize.Name = MONGO_BATCH_SIZE;
        }

        /// <summary>
        /// Establishes a connection to a connection manager.
        /// <param name="transaction">The transaction the connection is participating in. (optional)</param>
        public override void AcquireConnections(object transaction)
        {
            if (ComponentMetaData.RuntimeConnectionCollection.Count > 0)
            {
                IDTSRuntimeConnection100 conn = ComponentMetaData.RuntimeConnectionCollection[0];
                _connMgr = conn.ConnectionManager;
                if (_connMgr != null)
                    _db = (MongoDatabase)_connMgr.AcquireConnection(null);
            }
        }

        /// <summary>
        /// Frees the connections established during Microsoft.SqlServer.Dts.Pipeline.PipelineComponent.AcquireConnections(System.Object).
        ///     Called at design time and run time.
        /// </summary>
        public override void ReleaseConnections()
        {
            if (_connMgr != null)
                _connMgr.ReleaseConnection(_db);
        }

        public override void PreExecute()
        {
            IDTSInput100 input = ComponentMetaData.InputCollection[0];

            // Read input column information to memory
            foreach (IDTSInputColumn100 inCol in input.InputColumnCollection)
            {
                DestinationColumnInfo ci = new DestinationColumnInfo();
                ci.BufferColumnIndex = BufferManager.FindColumnByLineageID(input.Buffer, inCol.LineageID);
                ci.ColumnName = inCol.Name;
                ci.DataType = inCol.DataType;
                _colInfos.Add(ci);
            }
            _colInfos.Sort();
        }

        public override void ProcessInput(int inputID, PipelineBuffer buffer)
        {
            bool pbCancel = false;
            if (String.IsNullOrEmpty(ComponentMetaData.CustomPropertyCollection[MONGO_COLL].Value))
            {
                ComponentMetaData.FireError(0, "ProcessInput", "Missing MongoDB collection name", "", 0, out pbCancel);
                return;
            }
            int iBatchSize;
            if (!Int32.TryParse(ComponentMetaData.CustomPropertyCollection[MONGO_BATCH_SIZE].Value, out iBatchSize))
            {
                ComponentMetaData.FireError(0, "ProcessInput", "Failed to parse MongoDB insert batch size", "", 0, out pbCancel);
                return;
            }
            MongoCollection collection = _db.GetCollection(ComponentMetaData.CustomPropertyCollection[MONGO_COLL].Value);
            List<BsonDocument> batch = new List<BsonDocument>();
            int j = 1;

            // Create an empty template document using input column specification
            BsonInsertDocument template = new BsonInsertDocument(_colInfos);
            BsonDocument doc = template.Create();

            while (buffer.NextRow())
            {
                doc = template.DeepClone();
                for (int i = 0; i < _colInfos.Count; i++)
                {
                    doc.InsertDotNotated(_colInfos[i], CreateBsonValue(buffer[_colInfos[i].BufferColumnIndex], _colInfos[i]));
                }
                batch.Add(doc);
                j++;
                if (j > iBatchSize)
                {
                    collection.InsertBatch(batch);
                    ComponentMetaData.IncrementPipelinePerfCounter(103, (uint)batch.Count);
                    j = 1;
                    batch = new List<BsonDocument>();
                }
            }

            if (batch.Count > 0)
            {
                collection.InsertBatch(batch);
                ComponentMetaData.IncrementPipelinePerfCounter(103, (uint)batch.Count);
            }
        }

        /// <summary>
        /// Convert SSIS value to BsonValue
        /// </summary>
        /// <param name="val">SSIS value</param>
        /// <param name="ci">Column properties</param>
        /// <returns></returns>
        private BsonValue CreateBsonValue(object val, DestinationColumnInfo ci)
        {
            BsonValue bsonVal = null;
            if (val == null)
                return BsonNull.Value;
            switch (ci.DataType)
            {
                case DataType.DT_BOOL:
                    bsonVal = (bool)val;
                    break;
                case DataType.DT_DATE:
                case DataType.DT_DBDATE:
                case DataType.DT_DBTIME:
                case DataType.DT_DBTIME2:
                case DataType.DT_DBTIMESTAMP:
                case DataType.DT_DBTIMESTAMP2:
                case DataType.DT_DBTIMESTAMPOFFSET:
                case DataType.DT_FILETIME:
                    DateTime dtVal = (DateTime)val;
                    bsonVal = DateTime.SpecifyKind(dtVal, DateTimeKind.Utc);
                    break;
                case DataType.DT_GUID:
                    bsonVal = (Guid)val;
                    break;
                case DataType.DT_IMAGE:
                case DataType.DT_BYTES:
                    bsonVal = (byte[])val;
                    break;
                case DataType.DT_NULL:
                case DataType.DT_EMPTY:
                    bsonVal = BsonNull.Value;
                    break;
                case DataType.DT_NUMERIC:
                case DataType.DT_R4:
                case DataType.DT_R8:
                case DataType.DT_DECIMAL:
                    bsonVal = Convert.ToDouble(val);
                    break;
                case DataType.DT_I1:
                case DataType.DT_I2:
                case DataType.DT_I4:
                case DataType.DT_UI1:
                case DataType.DT_UI2:
                case DataType.DT_UI4:
                    bsonVal = (int)val;
                    break;
                case DataType.DT_UI8:
                case DataType.DT_I8:
                    bsonVal = (long)val;
                    break;
                case DataType.DT_TEXT:
                case DataType.DT_STR:
                case DataType.DT_WSTR:
                case DataType.DT_NTEXT:
                    bsonVal = (string)val;
                    break;
                default:
                    bool pbCancel = true;
                    ComponentMetaData.FireError(0, "MongoDB Destination", "Data type " + ci.DataType.ToString() + " is not supported.", "", 0, out pbCancel);
                    break;
            }
            return bsonVal;
        }

    }

    /// <summary>
    /// Input column specification
    /// </summary>
    public class DestinationColumnInfo : IComparable
    {
        private string[] _docNames;
        private string _colName;

        public int BufferColumnIndex { get; set; }

        /// <summary>
        /// Dot notated variable as an array. a.b.c --> [a,b,c]
        /// </summary>
        public string[] DocNames
        {
            get { return _docNames; }
        }
        public DataType DataType { get; set; }

        public string ColumnName
        {
            get { return _colName; }
            set
            {
                _colName = value;
                _docNames = value.Split(".".ToCharArray());
            }
        }

        public override string ToString()
        {
            return this.BufferColumnIndex + ", " + this.ColumnName + ", " + this.DataType.ToString();
        }

        /// <summary>
        /// Higher document level first.
        /// a
        /// b
        /// c.c1
        /// c.c2
        /// d.d1.d11
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public int CompareTo(object obj)
        {
            DestinationColumnInfo other = (DestinationColumnInfo)obj;
            int otherCount = (other.ColumnName.Length - other.ColumnName.Replace(".", "").Length);
            int thisCount = (this.ColumnName.Length - this.ColumnName.Replace(".", "").Length);
            if (otherCount > thisCount)
                return -1;
            else if (thisCount > otherCount)
                return 1;
            else
                return this.ColumnName.CompareTo(other.ColumnName);
        }
    }

    public static class Ext
    {
        public static int Depth(this IEnumerable<DestinationColumnInfo> cis)
        {
            return cis.Max(x => x.DocNames.Length);
        }

        // Applies a BsonValue to Bson document template.
        public static void InsertDotNotated(this BsonDocument root, DestinationColumnInfo ci, BsonValue val)
        {
            BsonDocument doc = root;
            for (int i = 0; i < ci.DocNames.Length - 1; i++)
            {
                doc = doc[ci.DocNames[i]].AsBsonDocument;
            }
            doc[ci.DocNames[ci.DocNames.Length - 1]] = val;
        }
    }

    /// <summary>
    /// Class for creating a template BsonDocument using SSIS input column specification. 
    /// Supports nested documents, when using dot notation in SSIS parameters. Arrays are not supported.
    /// </summary>
    public class BsonInsertDocument
    {
        private BsonDocument _root;
        List<DestinationColumnInfo> _cols;

        public BsonDocument DeepClone()
        {
            return _root.DeepClone().AsBsonDocument;
        }

        public BsonInsertDocument(List<DestinationColumnInfo> cols)
        {
            _root = new BsonDocument();
            cols.Sort();
            _cols = cols;

        }

        public BsonDocument Create()
        {
            string prevName = "";
            for (int i = 0; i < _cols.Depth(); i++)
            {
                foreach (DestinationColumnInfo col in _cols)
                {
                    if (col.DocNames.Length >= i + 1)
                    {
                        BsonDocument parent = GetParentDoc(col, i);
                        if (prevName != col.DocNames[i])
                        {
                            object elem = null;
                            if (col.DocNames.Length > i + 1)
                                elem = new Dictionary<string, object>();
                            parent.Add(new Dictionary<string, object> { { col.DocNames[i], elem } });
                        }
                        prevName = col.DocNames[i];
                    }
                }
            }
            return _root;
        }

        private BsonDocument GetParentDoc(DestinationColumnInfo col, int iLvl)
        {
            if (col.DocNames.Length == 1)
                return _root;
            else
            {
                BsonDocument doc = _root;
                for (int i = 0; i < iLvl; i++)
                {
                    doc = doc[col.DocNames[i]].AsBsonDocument;
                }
                return doc;
            }
        }
    }
}
