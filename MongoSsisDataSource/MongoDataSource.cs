/*
 * Copyright (c) 2012 Xbridge Ltd
 * See the file license.txt for copying permission.
 */

using System;
using System.Collections;
using Microsoft.SqlServer.Dts.Pipeline;
using Microsoft.SqlServer.Dts.Pipeline.Wrapper;
using Microsoft.SqlServer.Dts.Runtime.Wrapper;
using MongoDB.Driver;
using MongoDB.Driver.Builders;
using MongoDB.Bson;
using System.Drawing.Design;
using System.Windows.Forms;
using System.Windows.Forms.Design;
using System.Reflection;

namespace MongoDataSource {

    [DtsPipelineComponent(DisplayName = "MongoDB Source", 
        Description = "Mongosis - Loads data from a MongoDB data source", 
        ComponentType = ComponentType.SourceAdapter,
        IconResource = "MongoDataSource.Resources.mongosis.ico")]
    public class MongoDataSource : PipelineComponent {

        private static string COLLECTION_NAME_PROP_NAME = "CollectionName";
        private static string CONDITIONAL_FIELD_PROP_NAME = "ConditionalFieldName";
        private static string CONDITION_FROM_PROP_NAME = "ConditionFromValue";
        private static string CONDITION_TO_PROP_NAME = "ConditionToValue";
        private static string CONDITION_DOC_PROP_NAME = "ConditionQuery";

        private IDTSConnectionManager100 m_ConnMgr;
        private ArrayList columnInformation;
        internal MongoDatabase database;
        public static string MONGODB_CONNECTION_MANAGER_NAME = "MongoDB";

        public override void ProvideComponentProperties() {
            // Allow for resetting the component.
            RemoveAllInputsOutputsAndCustomProperties();
            ComponentMetaData.RuntimeConnectionCollection.RemoveAll();

            AddCustomProperties(ComponentMetaData.CustomPropertyCollection);
            
            IDTSOutput100 output = ComponentMetaData.OutputCollection.New();
            output.Name = "Output";

            IDTSRuntimeConnection100 conn = ComponentMetaData.RuntimeConnectionCollection.New();
            conn.Name = MONGODB_CONNECTION_MANAGER_NAME;
        }

        private void AddCustomProperties(IDTSCustomPropertyCollection100 customPropertyCollection) {
            IDTSCustomProperty100 customProperty = createCustomProperty(customPropertyCollection, COLLECTION_NAME_PROP_NAME, "Name of collection to import data from");
            customProperty.UITypeEditor = typeof(CollectionNameEditor).AssemblyQualifiedName;
            
            customProperty = createCustomProperty(customPropertyCollection, CONDITIONAL_FIELD_PROP_NAME, "Field name for conditional query");
            customProperty.UITypeEditor = typeof(ColumnNameEditor).AssemblyQualifiedName;

            createCustomProperty(customPropertyCollection, CONDITION_FROM_PROP_NAME, "'From' value for conditional query");
            createCustomProperty(customPropertyCollection, CONDITION_TO_PROP_NAME, "'To' value for conditional query");
            createCustomProperty(customPropertyCollection, CONDITION_DOC_PROP_NAME, "Mongo query document for conditional query");
        }

        private IDTSCustomProperty100 createCustomProperty(IDTSCustomPropertyCollection100 customPropertyCollection, string name, string description) {
            IDTSCustomProperty100 customProperty = customPropertyCollection.New();
            customProperty.Description = description;
            customProperty.Name = name;

            return customProperty;
        }

        private IDTSCustomPropertyCollection100 GetCustomPropertyCollection() {
            return ComponentMetaData.CustomPropertyCollection;
        }

        public override Microsoft.SqlServer.Dts.Pipeline.Wrapper.IDTSCustomProperty100 SetComponentProperty(string propertyName, object propertyValue) {
            if (propertyName.Equals("CollectionName")) {
                string collectionName = Convert.ToString(propertyValue);
                CreateColumnsFromMongoDb(collectionName);
            }
            return base.SetComponentProperty(propertyName, propertyValue);
        }

        public override void AcquireConnections(object transaction) {
            if (ComponentMetaData.RuntimeConnectionCollection.Count > 0) {
                IDTSRuntimeConnection100 conn = ComponentMetaData.RuntimeConnectionCollection[0];
                m_ConnMgr = conn.ConnectionManager;

                database = (MongoDatabase)m_ConnMgr.AcquireConnection(null);
            }
        }

        public override void ReleaseConnections() {
            if (m_ConnMgr != null) {
                m_ConnMgr.ReleaseConnection(database);
            }
        }

        public override void SetOutputColumnDataTypeProperties(int iOutputID, int iOutputColumnID, Microsoft.SqlServer.Dts.Runtime.Wrapper.DataType eDataType, int iLength, int iPrecision, int iScale, int iCodePage) {
            IDTSOutputColumn100 outColumn = ComponentMetaData.OutputCollection[0].OutputColumnCollection.GetObjectByID(iOutputColumnID);

            outColumn.SetDataTypeProperties(eDataType, iLength, iPrecision, iScale, iCodePage);
        }

        private void CreateColumnsFromMongoDb(string collectionName) {
            // Get the output.
            IDTSOutput100 output = ComponentMetaData.OutputCollection[0];

            // Start clean, and remove the columns from both collections.
            output.OutputColumnCollection.RemoveAll();
            output.ExternalMetadataColumnCollection.RemoveAll();

            if (database == null) {
                AcquireConnections(null);
            }

            MongoCollection<BsonDocument> collection = database.GetCollection(collectionName);

            if (collection.Count() == 0) {
                throw new Exception(collectionName + " collection has no records");
            }

            BsonDocument document = collection.FindOne();

            // Walk the columns in the schema, 
            // and for each data column create an output column and an external metadata column.
            foreach (BsonElement bsonElement in document) {

                if (!bsonElement.Name.Equals("_id")) {
                    IDTSOutputColumn100 outColumn = BuildOutputColumn(output.OutputColumnCollection, bsonElement);

                    IDTSExternalMetadataColumn100 externalColumn = output.ExternalMetadataColumnCollection.New();
                    PopulateExternalMetadataColumn(externalColumn, outColumn);

                    // Map the external column to the output column.
                    outColumn.ExternalMetadataColumnID = externalColumn.ID;
                }
            }
        }

        private IDTSOutputColumn100 BuildOutputColumn(IDTSOutputColumnCollection100 outputColumnCollection, BsonElement bsonElement) {
            IDTSOutputColumn100 outColumn = outputColumnCollection.New();

            // Set the properties of the output column.
            outColumn.Name = bsonElement.Name;

            DataType dt = GetColumnDataType(bsonElement.Value);
            int length = 0;
            int codepage = 0;

            if (dt == DataType.DT_STR) {
                length = 256;
                codepage = 1252;
            }

            outColumn.SetDataTypeProperties(dt, length, 0, 0, codepage);

            return outColumn;
        }

        private void PopulateExternalMetadataColumn(IDTSExternalMetadataColumn100 externalColumnToPopulate, IDTSOutputColumn100 outputColumn) {
            externalColumnToPopulate.Name = outputColumn.Name;
            externalColumnToPopulate.Precision = outputColumn.Precision;
            externalColumnToPopulate.Length = outputColumn.Length;
            externalColumnToPopulate.DataType = outputColumn.DataType;
            externalColumnToPopulate.Scale = outputColumn.Scale;
        }

        private DataType GetColumnDataType(BsonValue mongoValue) {
            DataType dt = DataType.DT_STR;

            if (mongoValue.IsBsonDateTime) {
                dt = DataType.DT_DATE;
            } else if (mongoValue.IsDouble) {
                dt = DataType.DT_R8;
            } else if (mongoValue.IsBoolean) {
                dt = DataType.DT_BOOL;
            } else if (mongoValue.IsInt32 | mongoValue.IsInt64) {
                dt = DataType.DT_I8;
            }

            return dt;
        }

        public override void PreExecute() {
            this.columnInformation = new ArrayList();
            IDTSOutput100 output = ComponentMetaData.OutputCollection[0];

            foreach (IDTSOutputColumn100 col in output.OutputColumnCollection) {
                ColumnInfo ci = new ColumnInfo();
                ci.BufferColumnIndex = BufferManager.FindColumnByLineageID(output.Buffer, col.LineageID);
                ci.ColumnName = col.Name;
                ci.ColumnDataType = col.DataType;
                this.columnInformation.Add(ci);
            }
        }


        public override void PrimeOutput(int outputs, int[] outputIDs, PipelineBuffer[] buffers) {
            IDTSOutput100 output = ComponentMetaData.OutputCollection[0];
            PipelineBuffer buffer = buffers[0];
            IDTSCustomProperty100 collectionNameProp = ComponentMetaData.CustomPropertyCollection[COLLECTION_NAME_PROP_NAME];

            if (database == null) {
                AcquireConnections(null);
            }

            if (string.IsNullOrEmpty(collectionNameProp.Value)) {
                throw new Exception("The collection name is null or empty!");
            }

            ComponentMetaData.FireInformation(0, "MongoDataSource", "processing collection " + collectionNameProp.Value, String.Empty, 0, false);

            var collection = database.GetCollection(collectionNameProp.Value);

            var cursor = GetCollectionCursor(collection);

            foreach (BsonDocument document in cursor) {

                buffer.AddRow();
                for (int x = 0; x <= columnInformation.Count - 1; x++) {
                    ColumnInfo ci = (ColumnInfo)columnInformation[x];

                    try {
                        if (document[ci.ColumnName] != null) {
                            if (document.GetValue(ci.ColumnName).IsBsonNull) {
                                buffer.SetNull(ci.BufferColumnIndex);
                            } else {
                                buffer[ci.BufferColumnIndex] = GetValue(document, ci);
                            }
                        } else {
                            buffer.SetNull(ci.BufferColumnIndex);
                        }
                    } catch (Exception e) {
                        throw new Exception("There was an issue with column '" + ci.ColumnName + "'", e);
                    }
                }
            }

            buffer.SetEndOfRowset();
        }

        private dynamic GetCollectionCursor(dynamic collection) {
            IDTSCustomProperty100 queryProp = ComponentMetaData.CustomPropertyCollection[CONDITION_DOC_PROP_NAME];
            IDTSCustomProperty100 conditionalFieldProp = ComponentMetaData.CustomPropertyCollection[CONDITIONAL_FIELD_PROP_NAME];
            
            if(!String.IsNullOrEmpty(queryProp.Value)) {
                ComponentMetaData.FireInformation(0, "MongoDataSource", "selecting data with specified query: " + queryProp.Value, String.Empty, 0, false);

                return collection.Find(new QueryDocument(BsonDocument.Parse(queryProp.Value)));
            } else if (!String.IsNullOrEmpty(conditionalFieldProp.Value)) {
                IDTSCustomProperty100 fromValueProp = ComponentMetaData.CustomPropertyCollection[CONDITION_FROM_PROP_NAME];
                IDTSCustomProperty100 toValueProp = ComponentMetaData.CustomPropertyCollection[CONDITION_TO_PROP_NAME];

                IMongoQuery query = BuildQuery(conditionalFieldProp, fromValueProp, toValueProp);

                ComponentMetaData.FireInformation(0, "MongoDataSource", "selecting data with query: " + query, String.Empty, 0, false);

                return collection.Find(query);
            } else {
                ComponentMetaData.FireInformation(0, "MongoDataSource", "selecting all data", String.Empty, 0, false);

                return collection.FindAll();
            }
        }

        private IMongoQuery BuildQuery(IDTSCustomProperty100 condFieldProp,IDTSCustomProperty100 fromProp,IDTSCustomProperty100 toProp) {
            BsonValue fromValue = GetBsonValueForQueryCondition(condFieldProp.Value, fromProp.Value);
            BsonValue toValue = GetBsonValueForQueryCondition(condFieldProp.Value, toProp.Value);

            return BuildQuery(condFieldProp.Value, fromValue, toValue);
        }

        private BsonValue GetBsonValueForQueryCondition(string fieldName, string value) {
            if(String.IsNullOrEmpty(value)) return null;

            ColumnInfo info = GetColumnInfo(fieldName);

            if (info == null) {
                throw new Exception("No information was found for the column '" + fieldName + "', ensure the column name is correct"); 
            }

            return ParseConditionValue(value, info.ColumnDataType);
        }

        private IMongoQuery BuildQuery(string conditionFieldName, BsonValue fromValue, BsonValue toValue) {
            IMongoQuery finalQuery = null;
            IMongoQuery fromQuery = null;
            IMongoQuery toQuery = null;

            if (fromValue != null) {
                fromQuery = Query.GTE(conditionFieldName, fromValue);
            }
            if (toValue != null) {
                toQuery = Query.LTE(conditionFieldName, toValue);
            }

            if (fromQuery != null && toQuery != null) {
                finalQuery = Query.And(fromQuery, toQuery);
            } else if (toQuery != null) {
                finalQuery = toQuery;
            } else {
                finalQuery = fromQuery;
            }

            return finalQuery;
        }

        private BsonValue ParseConditionValue(String value, DataType dt) {
            BsonValue parsedValue = value;

            if (dt == DataType.DT_DATE | dt == DataType.DT_DBTIMESTAMPOFFSET | dt == DataType.DT_DBTIMESTAMP) {
                if ("now".Equals(value, StringComparison.CurrentCultureIgnoreCase) ||
                    "today".Equals(value,StringComparison.CurrentCultureIgnoreCase)) {
                    parsedValue = new BsonDateTime(DateTime.Now);
                } else if("yesterday".Equals(value,StringComparison.CurrentCultureIgnoreCase)) {
                    parsedValue = new BsonDateTime(DateTime.Now.AddDays(-1));
                } else if(value.StartsWith("-")) {
                    int noOfDays = Int16.Parse(value);
                    parsedValue = new BsonDateTime(DateTime.Now.AddDays(noOfDays));
                } else {
                    parsedValue = new BsonDateTime(DateTime.Parse(value));
                }
            } else if (dt == DataType.DT_I8 || dt == DataType.DT_I4) {
                parsedValue = new BsonInt64(Int64.Parse(value));
            } else if (dt == DataType.DT_R8 || dt == DataType.DT_R4) {
                parsedValue = new BsonDouble(Double.Parse(value));
            }
            
            return parsedValue;
        }

        private object GetValue(BsonDocument document, ColumnInfo ci) {
            BsonValue value = document.GetValue(ci.ColumnName);

            return GetDataTypeValueFromBsonValue(value, ci.ColumnDataType);
        }

        private object GetDataTypeValueFromBsonValue(BsonValue value, DataType dt) {

            if (dt == DataType.DT_I8 | dt == DataType.DT_I4) {
                return value.ToInt64();
            } else if (dt == DataType.DT_BOOL) {
                return value.ToBoolean();
            } else if (dt == DataType.DT_R8 | dt == DataType.DT_R4) {
                return value.ToDouble();
            } else if (dt == DataType.DT_DATE | dt == DataType.DT_DBTIMESTAMPOFFSET | dt == DataType.DT_DBTIMESTAMP) {
                return DateTime.Parse(value.ToString());
            } else {
                if (!value.IsString) {
                    ComponentMetaData.FireWarning(0, "MongoDataSource", "Converting " + value.BsonType + " to string, though datatype was " + dt, String.Empty, 0);
                }

                return value.ToString();
            }
        }

		private ColumnInfo GetColumnInfo(String name) {
            foreach(ColumnInfo info in columnInformation) {
                if (info.ColumnName.Equals(name)) {
                    return info;
                }
            }
            return null;
        }

    }

    internal class ColumnInfo {
        internal int BufferColumnIndex;
        internal string ColumnName;
        internal DataType ColumnDataType;
    }

    internal class CollectionNameEditor : UITypeEditor {
        private IWindowsFormsEditorService edSvc = null; 

        public override object EditValue(System.ComponentModel.ITypeDescriptorContext context, IServiceProvider provider, object value) {
            edSvc = (IWindowsFormsEditorService)provider.GetService(typeof(IWindowsFormsEditorService));
            
            if (edSvc != null) {
                MongoDatabase database = GetDatabase(context);
                
                if (database != null) {
                    ListBox lb = BuildListBox(database);

                    edSvc.DropDownControl(lb);

                    if (lb.SelectedItem != null) {
                        return lb.SelectedItem;
                    }
                } else {
                    throw new Exception("No database connection found!");
                }
            }
            return value;
        }

        private ListBox BuildListBox(MongoDatabase database) {
            ListBox lb = new ListBox();

            foreach (String name in database.GetCollectionNames()) {
                if (!name.StartsWith("system")) {
                    lb.Items.Add(name);
                }
            }

            lb.SelectionMode = SelectionMode.One;
            lb.SelectedValueChanged += OnListBoxSelectedValueChanged;

            return lb;
        }

        private MongoDatabase GetDatabase(System.ComponentModel.ITypeDescriptorContext context) {
            MongoDatabase db = null;

            Microsoft.SqlServer.Dts.Runtime.ConnectionManager cm = GetMongoDBConnectionManager(context);
            if (cm != null) {
                db = (MongoDatabase)cm.AcquireConnection(null);
            }

            return db;
        }

        private Microsoft.SqlServer.Dts.Runtime.ConnectionManager GetMongoDBConnectionManager(System.ComponentModel.ITypeDescriptorContext context) {
            Microsoft.SqlServer.Dts.Runtime.Package package = GetPackageFromContext(context);

            return GetMongoDBConnectionManager(package);
        }

        private Microsoft.SqlServer.Dts.Runtime.ConnectionManager GetMongoDBConnectionManager(Microsoft.SqlServer.Dts.Runtime.Package package) {
            if (package != null) {
                foreach (Microsoft.SqlServer.Dts.Runtime.ConnectionManager cm in package.Connections) {
                    if (cm.CreationName.Equals(MongoDataSource.MONGODB_CONNECTION_MANAGER_NAME)) {
                        return cm;
                    }
                }
            }

            return null;
        }

        private Microsoft.SqlServer.Dts.Runtime.Package GetPackageFromContext(System.ComponentModel.ITypeDescriptorContext context) {
            Microsoft.SqlServer.Dts.Runtime.Package package = null;

            PropertyInfo[] props = context.Instance.GetType().GetProperties();

            foreach (PropertyInfo propInfo in props) {
                if (propInfo.Name.Equals("PipelineTask")) {
                    Microsoft.SqlServer.Dts.Runtime.EventsProvider eventsProvider = (Microsoft.SqlServer.Dts.Runtime.EventsProvider)propInfo.GetValue(context.Instance, null);

                    package = (Microsoft.SqlServer.Dts.Runtime.Package)eventsProvider.Parent;
                }
            }

            return package;
        }

        public override UITypeEditorEditStyle GetEditStyle(System.ComponentModel.ITypeDescriptorContext context) {
            return UITypeEditorEditStyle.DropDown;
        }

        private void OnListBoxSelectedValueChanged(object sender, EventArgs e) {
            // close the drop down as soon as something is clicked
            edSvc.CloseDropDown();
        }
    }

    internal class ColumnNameEditor : UITypeEditor {
        private IWindowsFormsEditorService edSvc = null;

        public override object EditValue(System.ComponentModel.ITypeDescriptorContext context, IServiceProvider provider, object value) {
            edSvc = (IWindowsFormsEditorService)provider.GetService(typeof(IWindowsFormsEditorService));

            if (edSvc != null) {
                
                ListBox lb = new ListBox();

                lb.Items.Add("analytica_updated_at");
                lb.Items.Add("analytica_not_updated_at");

                lb.SelectionMode = SelectionMode.One;
                lb.SelectedValueChanged += OnListBoxSelectedValueChanged;
                edSvc.DropDownControl(lb);

                if (lb.SelectedItem != null) {
                    return lb.SelectedItem;
                }
               
            }
            return value;
        }

        public override UITypeEditorEditStyle GetEditStyle(System.ComponentModel.ITypeDescriptorContext context) {
            return UITypeEditorEditStyle.DropDown;
        }

        private void OnListBoxSelectedValueChanged(object sender, EventArgs e) {
            // close the drop down as soon as something is clicked
            edSvc.CloseDropDown();
        }
    }
}
