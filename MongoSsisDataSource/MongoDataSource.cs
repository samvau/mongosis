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
using MongoDB.Bson;
using System.Drawing.Design;
using System.Windows.Forms;
using System.Windows.Forms.Design;
using System.Reflection;

namespace MongoDataSource {

    [DtsPipelineComponent(DisplayName = "MongoDB Data Source", Description = "Loads data from a MongoDB data source", ComponentType = ComponentType.SourceAdapter)]
    public class MongoDataSource : PipelineComponent {

        private IDTSConnectionManager100 m_ConnMgr;
        private ArrayList columnInformation;
        private string _collectionName = string.Empty;
        internal MongoDatabase database;

        public string CollectionName {
            get { return _collectionName; }
            set { _collectionName = value; }
        }

        public override void ProvideComponentProperties() {
            // Allow for resetting the component.
            RemoveAllInputsOutputsAndCustomProperties();
            ComponentMetaData.RuntimeConnectionCollection.RemoveAll();

            dynamic customProperty = ComponentMetaData.CustomPropertyCollection.New();
            customProperty.Name = "CollectionName";
            customProperty.UITypeEditor = typeof(CollectionNameEditor).AssemblyQualifiedName;
            
            IDTSOutput100 output = ComponentMetaData.OutputCollection.New();
            output.Name = "Output";

            IDTSRuntimeConnection100 conn = ComponentMetaData.RuntimeConnectionCollection.New();
            conn.Name = "MongoDB";
        }

        public override Microsoft.SqlServer.Dts.Pipeline.Wrapper.IDTSCustomProperty100 SetComponentProperty(string propertyName, object propertyValue) {
            if (propertyName.Equals("CollectionName")) {
                _collectionName = Convert.ToString(propertyValue);
                CreateColumnsFromMongoDb(_collectionName);
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
            IDTSCustomProperty100 collectionNameProp = ComponentMetaData.CustomPropertyCollection[0];

            if (database == null) {
                AcquireConnections(null);
            }

            if (string.IsNullOrEmpty(collectionNameProp.Value)) {
                throw new Exception("The collection name is null or empty!");
            }

            ComponentMetaData.FireInformation(0, "MongoDataSource", "processing collection " + collectionNameProp.Value, String.Empty, 0, false);

            foreach (BsonDocument document in database.GetCollection(collectionNameProp.Value).FindAll()) {

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


        internal System.Collections.Generic.IEnumerable<string> GetCollectionNames() {
            if (database == null) {
                AcquireConnections(null);
            }

            return database.GetCollectionNames();
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
                ListBox lb = new ListBox();
                MongoDatabase database = GetDatabase(context);

                if (database != null) {
                    foreach (String name in database.GetCollectionNames()) {
                        if (!name.StartsWith("system")) {
                            lb.Items.Add(name);
                        }
                    }
                } else {
                    throw new Exception("No database connection found!");
                }

                lb.SelectionMode = SelectionMode.One;
                lb.SelectedValueChanged += OnListBoxSelectedValueChanged;

                edSvc.DropDownControl(lb);

                if (lb.SelectedItem != null)
                    return lb.SelectedItem;
            }
            return value;
        }

        private MongoDatabase GetDatabase(System.ComponentModel.ITypeDescriptorContext context) {
            MongoDatabase db = null;

            PropertyInfo[] props = context.Instance.GetType().GetProperties();

            foreach (PropertyInfo propInfo in props) {
                if (propInfo.Name.Equals("PipelineTask")) {
                    Microsoft.SqlServer.Dts.Runtime.EventsProvider ep = (Microsoft.SqlServer.Dts.Runtime.EventsProvider)propInfo.GetValue(context.Instance, null);

                    Microsoft.SqlServer.Dts.Runtime.Package p = (Microsoft.SqlServer.Dts.Runtime.Package)ep.Parent;

                    foreach (Microsoft.SqlServer.Dts.Runtime.ConnectionManager cm in p.Connections) {

                        if (cm.CreationName.Equals("MongoDB")) {
                            db = (MongoDatabase)cm.AcquireConnection(null);
                        }
                    }
                }
            }

            return db;
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
