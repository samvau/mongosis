/*
 * Copyright (c) 2012-2013 Xbridge Ltd
 * See the file license.txt for copying permission.
 */

using System;
using System.Collections;
using System.Collections.Generic;
using System.Drawing.Design;
using System.Linq;
using System.Reflection;
using System.Windows.Forms;
using System.Windows.Forms.Design;
using Microsoft.SqlServer.Dts.Pipeline;
using Microsoft.SqlServer.Dts.Pipeline.Wrapper;
using Microsoft.SqlServer.Dts.Runtime.Wrapper;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Builders;

namespace MongoDataSource
{

    [DtsPipelineComponent(DisplayName = "MongoDB Source",
        Description = "Mongosis - Loads data from a MongoDB data source",
        ComponentType = ComponentType.SourceAdapter,
        IconResource = "MongoDataSource.Resources.mongosis.ico")]
    public class MongoDataSource : PipelineComponent
    {
        #region constants
        private const string COLLECTION_NAME_PROP_NAME = "CollectionName";
        private const string CONDITIONAL_FIELD_PROP_NAME = "ConditionalFieldName";
        private const string CONDITION_FROM_PROP_NAME = "ConditionFromValue";
        private const string CONDITION_TO_PROP_NAME = "ConditionToValue";
        private const string CONDITION_DOC_PROP_NAME = "ConditionQuery";
        internal const string MONGODB_CONNECTION_MANAGER_NAME = "MongoDB";
        #endregion

        #region private variables
        /// <summary>
        /// Connection manager for the mongo database
        /// </summary>
        private IDTSConnectionManager100 m_ConnMgr;
        /// <summary>
        /// The ID of the error output
        /// </summary>
        private int errorOutputID = -1;
        /// <summary>
        /// The index of the error output in the outputs collection
        /// </summary>
        private int errorOutputIndex = -1;
        /// <summary>
        /// Connection to the mongo database
        /// </summary>
        private MongoDatabase database;
        /// <summary>
        /// Metadata about each of the output columns
        /// </summary>
        private IEnumerable<ColumnInfo> columnInformata;
        #endregion

        #region PipelineComponent members
        /// <summary>
        /// Called when a component is first added to the data flow task, to initialize
        ///     the Microsoft.SqlServer.Dts.Pipeline.PipelineComponent.ComponentMetaData
        ///     of the component.
        /// </summary>
        public override void ProvideComponentProperties()
        {
            // Allow for resetting the component.
            RemoveAllInputsOutputsAndCustomProperties();
            ComponentMetaData.RuntimeConnectionCollection.RemoveAll();

            AddCustomProperties(ComponentMetaData.CustomPropertyCollection);

            // Specify that the component has an error output.
            ComponentMetaData.UsesDispositions = true;

            var defaultOutput = ComponentMetaData.OutputCollection.New();
            defaultOutput.Name = "Output";
            defaultOutput.ExternalMetadataColumnCollection.IsUsed = true;

            // Adds an additional output category allowing the end-user to send errors to a different output
            var errorOutput = ComponentMetaData.OutputCollection.New();
            errorOutput.Name = "ErrorOutput";
            errorOutput.IsErrorOut = true;

            var conn = ComponentMetaData.RuntimeConnectionCollection.New();
            conn.Name = MONGODB_CONNECTION_MANAGER_NAME;
        }

        /// <summary>
        /// Called after Microsoft.SqlServer.Dts.Pipeline.PipelineComponent.PrepareForExecute(),
        ///     and before Microsoft.SqlServer.Dts.Pipeline.PipelineComponent.PrimeOutput(System.Int32,System.Int32[],Microsoft.SqlServer.Dts.Pipeline.PipelineBuffer[])
        ///     and Microsoft.SqlServer.Dts.Pipeline.PipelineComponent.ProcessInput(System.Int32,Microsoft.SqlServer.Dts.Pipeline.PipelineBuffer).
        /// </summary>
        /// <remarks>
        /// Sets the error output information; the id of the error output and the index of the error output.
        /// Gathers the column metadata.
        /// </remarks>
        public override void PreExecute()
        {
            this.GetErrorOutputInfo(ref errorOutputID, ref errorOutputIndex);
            this.columnInformata = GetColumnInformata().ToArray();
        }

        /// <summary>
        /// Called at run time for source components and transformation components with
        ///     asynchronous outputs to let these components add rows to the output buffers.
        /// </summary>
        /// <param name="outputs">The number of elements in the outputIDs and buffers arrays.</param>
        /// <param name="outputIDs">An array of Microsoft.SqlServer.Dts.Pipeline.Wrapper.IDTSOutput100 ID's.</param>
        /// <param name="buffers">An array of Microsoft.SqlServer.Dts.Pipeline.PipelineBuffer objects.</param>
        public override void PrimeOutput(int outputs, int[] outputIDs, PipelineBuffer[] buffers)
        {
            // Determine which buffer is for regular output, and which is for error output
            PipelineBuffer errorBuffer = null;
            PipelineBuffer defaultBuffer = null;
            for (int outputIndex = 0; outputIndex < outputs; outputIndex++)
                if (outputIDs[outputIndex] == errorOutputID)
                    errorBuffer = buffers[outputIndex];
                else
                    defaultBuffer = buffers[outputIndex];

            var cursor = GetCollectionCursor(ComponentMetaData.CustomPropertyCollection[COLLECTION_NAME_PROP_NAME].Value);
            var defaultOutputColumns = GetDefaultOutputColumns().ToArray();
            var errorOutputColumns = GetErrorOutputColumns().ToArray();
            foreach (BsonDocument document in cursor)
            {
                ColumnInfo failingColumnInfo = null;
                try
                {
                    defaultBuffer.AddRow();
                    foreach (ColumnInfo columnInfo in this.columnInformata)
                    {
                        failingColumnInfo = columnInfo;
                        if (document.Contains(columnInfo.ColumnName) && document[columnInfo.ColumnName] != null)
                        {
                            if (document.GetValue(columnInfo.ColumnName).IsBsonNull)
                                defaultBuffer.SetNull(columnInfo.OuputBufferColumnIndex);
                            else
                                defaultBuffer[columnInfo.OuputBufferColumnIndex] = GetValue(document, columnInfo);
                        }
                        else
                        {
                            defaultBuffer.SetNull(columnInfo.OuputBufferColumnIndex);
                        }
                    }
                }
                catch (Exception ex)
                {
                    switch (failingColumnInfo.OutputColumn.ErrorRowDisposition)
                    {
                        case DTSRowDisposition.RD_RedirectRow:
                            if (errorBuffer == null) throw new InvalidOperationException("There must be an error output defined if redirection was specified");

                            // Add a row to the error buffer.
                            errorBuffer.AddRow();

                            // Get the values from the default buffer
                            // and copy them to the error buffer.
                            foreach (IDTSOutputColumn100 column in errorOutputColumns)
                            {
                                ColumnInfo copiedColumnInfo = GetColumnInfo(column.Name);
                                if (copiedColumnInfo != null)
                                    errorBuffer[copiedColumnInfo.ErrorOuputBufferColumnIndex] = defaultBuffer[copiedColumnInfo.OuputBufferColumnIndex];
                            }

                            // Set the error information.
                            int errorCode = System.Runtime.InteropServices.Marshal.GetHRForException(ex);
                            errorBuffer.SetErrorInfo(errorOutputID, errorCode, failingColumnInfo.OutputColumn.LineageID);

                            // Remove the row that was added to the default buffer.
                            defaultBuffer.RemoveRow();
                            break;
                        case DTSRowDisposition.RD_FailComponent:
                            throw new Exception(String.Format("There was an issue with column: {0}", failingColumnInfo.OutputColumn.Name), ex);
                    }
                }
            }

            if (defaultBuffer != null)
                defaultBuffer.SetEndOfRowset();

            if (errorBuffer != null)
                errorBuffer.SetEndOfRowset();
        }

        /// <summary>
        /// Assigns a value to a Microsoft.SqlServer.Dts.Pipeline.Wrapper.IDTSCustomProperty100
        ///     of the component.
        /// </summary>
        /// <remarks>We override this in order to determine the output columns each time the mongo db collection name is updated</remarks>
        /// <param name="propertyName">The name of the Microsoft.SqlServer.Dts.Pipeline.Wrapper.IDTSCustomProperty100 whose value is set.</param>
        /// <param name="propertyValue">The object stored in the Value property of the Microsoft.SqlServer.Dts.Pipeline.Wrapper.IDTSCustomProperty100 object.</param>
        /// <returns>The Microsoft.SqlServer.Dts.Pipeline.Wrapper.IDTSCustomProperty100 object whose property is set.</returns>
        public override Microsoft.SqlServer.Dts.Pipeline.Wrapper.IDTSCustomProperty100 SetComponentProperty(string propertyName, object propertyValue)
        {
            if (propertyName.Equals("CollectionName"))
            {
                string collectionName = Convert.ToString(propertyValue);
                CreateColumnsFromMongoDb(collectionName);
            }
            return base.SetComponentProperty(propertyName, propertyValue);
        }

        /// <summary>
        /// Establishes a connection to a connection manager.
        /// <param name="transaction">The transaction the connection is participating in. (optional)</param>
        public override void AcquireConnections(object transaction)
        {
            if (ComponentMetaData.RuntimeConnectionCollection.Count > 0)
            {
                IDTSRuntimeConnection100 conn = ComponentMetaData.RuntimeConnectionCollection[0];
                m_ConnMgr = conn.ConnectionManager;
                if(m_ConnMgr != null)
                    database = (MongoDatabase)m_ConnMgr.AcquireConnection(null);
            }
        }

        /// <summary>
        /// Frees the connections established during Microsoft.SqlServer.Dts.Pipeline.PipelineComponent.AcquireConnections(System.Object).
        ///     Called at design time and run time.
        /// </summary>
        public override void ReleaseConnections()
        {
            if (m_ConnMgr != null)
                m_ConnMgr.ReleaseConnection(database);
        }

        /// <summary>
        /// Sets the data type properties of an Microsoft.SqlServer.Dts.Pipeline.Wrapper.IDTSOutputColumn100
        ///     object.
        /// </summary>
        /// <param name="iOutputID">The ID of the Microsoft.SqlServer.Dts.Pipeline.Wrapper.IDTSOutput100 object.</param>
        /// <param name="iOutputColumnID">The ID of the Microsoft.SqlServer.Dts.Pipeline.Wrapper.IDTSOutputColumn100 object.</param>
        /// <param name="eDataType">The Microsoft.SqlServer.Dts.Runtime.Wrapper.DataType of the column.</param>
        /// <param name="iLength">The length of the column.</param>
        /// <param name="iPrecision">The total number of digits in the column.</param>
        /// <param name="iScale">The number of digits of precision in the column.</param>
        /// <param name="iCodePage">The code page of the column.</param>
        public override void SetOutputColumnDataTypeProperties(int iOutputID, int iOutputColumnID, Microsoft.SqlServer.Dts.Runtime.Wrapper.DataType eDataType, int iLength, int iPrecision, int iScale, int iCodePage)
        {
            var output = ComponentMetaData.OutputCollection.FindObjectByID(iOutputID);
            var outColumn = output.OutputColumnCollection.GetObjectByID(iOutputColumnID);
            outColumn.SetDataTypeProperties(eDataType, iLength, iPrecision, iScale, iCodePage);
        }
        #endregion

        #region private methods
        /// <summary>
        /// Adds the custom properties for specifying the mongo db connection and optional query parameters
        /// </summary>
        /// <param name="customPropertyCollection"></param>
        private void AddCustomProperties(IDTSCustomPropertyCollection100 customPropertyCollection)
        {
            var customProperty = createCustomProperty(customPropertyCollection, COLLECTION_NAME_PROP_NAME, "Name of collection to import data from");
            customProperty.UITypeEditor = typeof(CollectionNameEditor).AssemblyQualifiedName;

            createCustomProperty(customPropertyCollection, CONDITIONAL_FIELD_PROP_NAME, "Field name for conditional query");
            createCustomProperty(customPropertyCollection, CONDITION_FROM_PROP_NAME, "'From' value for conditional query");
            createCustomProperty(customPropertyCollection, CONDITION_TO_PROP_NAME, "'To' value for conditional query");
            createCustomProperty(customPropertyCollection, CONDITION_DOC_PROP_NAME, "Mongo query document for conditional query");
        }

        /// <summary>
        /// Helper for adding a custom property to the provided collection
        /// </summary>
        /// <param name="customPropertyCollection"></param>
        /// <param name="name"></param>
        /// <param name="description"></param>
        /// <returns></returns>
        private IDTSCustomProperty100 createCustomProperty(IDTSCustomPropertyCollection100 customPropertyCollection, string name, string description)
        {
            IDTSCustomProperty100 customProperty = customPropertyCollection.New();
            customProperty.Description = description;
            customProperty.Name = name;

            return customProperty;
        }

        /// <summary>
        /// For the provided collection, will attempt to determine the name and type of each column in the collection.
        /// Adds the columns to both the default and error outputs.
        /// </summary>
        /// <param name="collectionName">The name of the mongo db collection</param>
        private void CreateColumnsFromMongoDb(string collectionName)
        {
            if (database == null)
                AcquireConnections(null);

            MongoCollection<BsonDocument> collection = database.GetCollection(collectionName);

            if (collection.Count() == 0)
                throw new Exception(collectionName + " collection has no records");

            // Remove the existing columns from the output in order to prevent us from adding duplicate columns
            foreach (IDTSOutput100 output in ComponentMetaData.OutputCollection)
            {
                if (output.IsErrorOut)
                {
                    List<IDTSOutputColumn100> columns = new List<IDTSOutputColumn100>();
                    for (int i = 0; i < output.OutputColumnCollection.Count; i++)
                        columns.Add(output.OutputColumnCollection[i]);

                    string[] errorColumns = new string[] { "ErrorColumn", "ErrorCode" };
                    IEnumerable<int> columnIdsToRemove = columns.Where(column => !errorColumns.Contains(column.Name)).Select(column => column.ID);
                    foreach (int columnIdToRemove in columnIdsToRemove)
                        output.OutputColumnCollection.RemoveObjectByID(columnIdToRemove);
                }
                else
                {
                    output.OutputColumnCollection.RemoveAll();
                    output.ExternalMetadataColumnCollection.RemoveAll();
                }
            }

            BsonDocument document = collection.FindOne();

            // Walk the columns in the schema,
            // and for each data column create an output column and an external metadata column.
            foreach (BsonElement bsonElement in document)
            {
                // Try to find a document that has a [non null] value for the particular column.
                BsonDocument documentWithNonNullElementValue = collection.FindOne(Query.NE(bsonElement.Name, BsonNull.Value));

                // If a document is found with a value for the element, use the element with the non-null value
                // instead of the original, which may or may not have a value. This will help to ensure that
                // a column will not be treated as a string just because some of its values were null.
                BsonElement bsonElementWithValue = null;
                if (documentWithNonNullElementValue != null)
                    bsonElementWithValue = documentWithNonNullElementValue.GetElement(bsonElement.Name);

                foreach (IDTSOutput100 output in ComponentMetaData.OutputCollection)
                {
                    IDTSOutputColumn100 outColumn = BuildOutputColumn(output, bsonElementWithValue ?? bsonElement);
                    IDTSExternalMetadataColumn100 externalColumn = BuildExternalMetadataColumn(output.ExternalMetadataColumnCollection, outColumn);

                    // Map the external column to the output column.
                    outColumn.ExternalMetadataColumnID = externalColumn.ID;
                }
            }
        }

        /// <summary>
        /// Constructs an output column and adds it to the provided output, using the column metadata from the provided BSON element.
        /// </summary>
        /// <param name="output"></param>
        /// <param name="bsonElement"></param>
        /// <returns></returns>
        private IDTSOutputColumn100 BuildOutputColumn(IDTSOutput100 output, BsonElement bsonElement)
        {
            IDTSOutputColumn100 outColumn = output.OutputColumnCollection.New();

            // Set the properties of the output column.
            outColumn.Name = bsonElement.Name;

            DataType dt = GetColumnDataType(bsonElement.Value);
            int length = 0;
            int codepage = 0;

            if (dt == DataType.DT_STR)
            {
                length = 256;
                codepage = 1252;
            }

            outColumn.SetDataTypeProperties(dt, length, 0, 0, codepage);

            // Setting these values enables the end-user to configure the error behavior on a per-column basis
            if (!output.IsErrorOut)
                outColumn.ErrorRowDisposition = DTSRowDisposition.RD_FailComponent;

            return outColumn;
        }

        /// <summary>
        /// Constructs an external metadata column and adds it to the provided collection, using the column metadata from the provided output column.
        /// </summary>
        /// <param name="externalMetadataColumnCollection"></param>
        /// <param name="outputColumn"></param>
        /// <returns></returns>
        private IDTSExternalMetadataColumn100 BuildExternalMetadataColumn(IDTSExternalMetadataColumnCollection100 externalMetadataColumnCollection, IDTSOutputColumn100 outputColumn)
        {
            IDTSExternalMetadataColumn100 externalColumnToPopulate = externalMetadataColumnCollection.New();
            externalColumnToPopulate.Name = outputColumn.Name;
            externalColumnToPopulate.Precision = outputColumn.Precision;
            externalColumnToPopulate.Length = outputColumn.Length;
            externalColumnToPopulate.DataType = outputColumn.DataType;
            externalColumnToPopulate.Scale = outputColumn.Scale;
            externalColumnToPopulate.CodePage = outputColumn.CodePage;
            return externalColumnToPopulate;
        }

        /// <summary>
        /// Determines the data type corresponding to the provided mongo db value
        /// </summary>
        /// <param name="mongoValue"></param>
        /// <returns></returns>
        private DataType GetColumnDataType(BsonValue mongoValue)
        {
            DataType dt = DataType.DT_STR;

            if (mongoValue.IsBsonDateTime)
            {
                dt = DataType.DT_DATE;
            }
            else if (mongoValue.IsDouble)
            {
                dt = DataType.DT_R8;
            }
            else if (mongoValue.IsBoolean)
            {
                dt = DataType.DT_BOOL;
            }
            else if (mongoValue.IsInt32 | mongoValue.IsInt64)
            {
                dt = DataType.DT_I8;
            }

            return dt;
        }

        /// <summary>
        /// Creates a cursor for accessing the data in the provided collection. Applies a custom query if the parameters specify
        /// </summary>
        /// <param name="collectionName"></param>
        /// <returns></returns>
        private dynamic GetCollectionCursor(string collectionName)
        {
            if (database == null)
                AcquireConnections(null);

            if (string.IsNullOrWhiteSpace(collectionName))
                throw new ArgumentNullException("collectionName");

            ComponentMetaData.FireInformation(0, "MongoDataSource", "processing collection " + collectionName, String.Empty, 0, false);

            var collection = database.GetCollection(collectionName);

            IDTSCustomProperty100 queryProp = ComponentMetaData.CustomPropertyCollection[CONDITION_DOC_PROP_NAME];
            IDTSCustomProperty100 conditionalFieldProp = ComponentMetaData.CustomPropertyCollection[CONDITIONAL_FIELD_PROP_NAME];

            if (!String.IsNullOrEmpty(queryProp.Value))
            {
                ComponentMetaData.FireInformation(0, "MongoDataSource", "selecting data with specified query: " + queryProp.Value, String.Empty, 0, false);

                return collection.Find(new QueryDocument(BsonDocument.Parse(queryProp.Value)));
            }
            else if (!String.IsNullOrEmpty(conditionalFieldProp.Value))
            {
                IDTSCustomProperty100 fromValueProp = ComponentMetaData.CustomPropertyCollection[CONDITION_FROM_PROP_NAME];
                IDTSCustomProperty100 toValueProp = ComponentMetaData.CustomPropertyCollection[CONDITION_TO_PROP_NAME];

                IMongoQuery query = BuildQuery(conditionalFieldProp, fromValueProp, toValueProp);

                ComponentMetaData.FireInformation(0, "MongoDataSource", "selecting data with query: " + query, String.Empty, 0, false);

                return collection.Find(query);
            }
            else
            {
                ComponentMetaData.FireInformation(0, "MongoDataSource", "selecting all data", String.Empty, 0, false);

                return collection.FindAll();
            }
        }

        private IMongoQuery BuildQuery(IDTSCustomProperty100 condFieldProp, IDTSCustomProperty100 fromProp, IDTSCustomProperty100 toProp)
        {
            BsonValue fromValue = GetBsonValueForQueryCondition(condFieldProp.Value, fromProp.Value);
            BsonValue toValue = GetBsonValueForQueryCondition(condFieldProp.Value, toProp.Value);

            return BuildQuery(condFieldProp.Value, fromValue, toValue);
        }

        private BsonValue GetBsonValueForQueryCondition(string fieldName, string value)
        {
            if (String.IsNullOrEmpty(value)) return null;

            ColumnInfo info = GetColumnInfo(fieldName);

            if (info == null)
                throw new Exception("No information was found for the column '" + fieldName + "', ensure the column name is correct");

            return ParseConditionValue(value, info.ColumnDataType);
        }

        private IMongoQuery BuildQuery(string conditionFieldName, BsonValue fromValue, BsonValue toValue)
        {
            IMongoQuery finalQuery = null;
            IMongoQuery fromQuery = null;
            IMongoQuery toQuery = null;

            if (fromValue != null)
            {
                fromQuery = Query.GTE(conditionFieldName, fromValue);
            }
            if (toValue != null)
            {
                toQuery = Query.LTE(conditionFieldName, toValue);
            }

            if (fromQuery != null && toQuery != null)
            {
                finalQuery = Query.And(fromQuery, toQuery);
            }
            else if (toQuery != null)
            {
                finalQuery = toQuery;
            }
            else
            {
                finalQuery = fromQuery;
            }

            return finalQuery;
        }

        private BsonValue ParseConditionValue(String value, DataType dt)
        {
            BsonValue parsedValue = value;

            if (dt == DataType.DT_DATE | dt == DataType.DT_DBTIMESTAMPOFFSET | dt == DataType.DT_DBTIMESTAMP)
            {
                if ("now".Equals(value, StringComparison.CurrentCultureIgnoreCase) ||
                    "today".Equals(value, StringComparison.CurrentCultureIgnoreCase))
                {
                    parsedValue = new BsonDateTime(DateTime.Now);
                }
                else if ("yesterday".Equals(value, StringComparison.CurrentCultureIgnoreCase))
                {
                    parsedValue = new BsonDateTime(DateTime.Now.AddDays(-1));
                }
                else if (value.StartsWith("-"))
                {
                    int noOfDays = Int16.Parse(value);
                    parsedValue = new BsonDateTime(DateTime.Now.AddDays(noOfDays));
                }
                else
                {
                    parsedValue = new BsonDateTime(DateTime.Parse(value));
                }
            }
            else if (dt == DataType.DT_I8 || dt == DataType.DT_I4)
            {
                parsedValue = new BsonInt64(Int64.Parse(value));
            }
            else if (dt == DataType.DT_R8 || dt == DataType.DT_R4)
            {
                parsedValue = new BsonDouble(Double.Parse(value));
            }

            return parsedValue;
        }

        private object GetValue(BsonDocument document, ColumnInfo ci)
        {
            BsonValue value = document.GetValue(ci.ColumnName);

            return GetDataTypeValueFromBsonValue(value, ci.ColumnDataType);
        }

        private object GetDataTypeValueFromBsonValue(BsonValue value, DataType dt)
        {
            if (dt == DataType.DT_I8 | dt == DataType.DT_I4)
            {
                if (value.IsString)
                {
                    Int64 parsedInt = -1;
                    if (!Int64.TryParse(value.ToString(), out parsedInt))
                    {
                        bool pbCancel = true;
                        ComponentMetaData.FireError(0, "MongoDataSource", "Cannot parse string value to integer: " + value.ToString(), "", 0, out pbCancel);
                    }
                    return parsedInt;
                }
                else
                {
                    return value.ToInt64();
                }
            }
            else if (dt == DataType.DT_BOOL)
            {
                return value.ToBoolean();
            }
            else if (dt == DataType.DT_R8 | dt == DataType.DT_R4)
            {
                return value.ToDouble();
            }
            else if (dt == DataType.DT_DATE | dt == DataType.DT_DBTIMESTAMPOFFSET | dt == DataType.DT_DBTIMESTAMP)
            {
                return DateTime.Parse(value.ToString());
            }
            else
            {
                if (!value.IsObjectId && !value.IsString && !value.IsBsonSymbol)
                    ComponentMetaData.FireWarning(0, "MongoDataSource", "Converting " + value.BsonType + " to string, though datatype was " + dt, String.Empty, 0);

                return value.ToString();
            }
        }

        /// <summary>
        /// Returns the column information for the column with the provided name
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        private ColumnInfo GetColumnInfo(String name)
        {
            return this.columnInformata.FirstOrDefault(ci => ci.ColumnName.Equals(name));
        }

        /// <summary>
        /// Collects the column information for the output columns
        /// </summary>
        /// <returns></returns>
        private IEnumerable<ColumnInfo> GetColumnInformata()
        {
            var query = from outputColumn in GetDefaultOutputColumns()
                        join errorOutputColumn in GetErrorOutputColumns() on outputColumn.Name equals errorOutputColumn.Name into joinO
                        from subErrorOutputColum in joinO.DefaultIfEmpty()
                        select new { OutputColumn = outputColumn, ErrorOutputColumn = subErrorOutputColum };

            return query.Select(o => new ColumnInfo()
            {
                ColumnDataType = o.OutputColumn.DataType,
                ColumnName = o.OutputColumn.Name,
                OutputColumn = o.OutputColumn,
                ErrorOutputColumn = o.ErrorOutputColumn,
                OuputBufferColumnIndex = BufferManager.FindColumnByLineageID(GetDefaultOutput().Buffer, o.OutputColumn.LineageID),
                ErrorOuputBufferColumnIndex = o.ErrorOutputColumn != null && GetErrorOutput().IsAttached ?
                    BufferManager.FindColumnByLineageID(GetErrorOutput().Buffer, o.ErrorOutputColumn.LineageID) : 0
            });
        }

        /// <summary>
        /// Locates the default output.
        /// Raises an exception if there is no default output.
        /// </summary>
        /// <returns></returns>
        private IDTSOutput100 GetDefaultOutput()
        {
            foreach (IDTSOutput100 output in ComponentMetaData.OutputCollection)
                if (!output.IsErrorOut)
                    return output;

            throw new InvalidOperationException("There is no default output defined");
        }

        /// <summary>
        /// Locates the error output.
        /// Raises an exception if there is no error output.
        /// </summary>
        /// <returns></returns>
        private IDTSOutput100 GetErrorOutput()
        {
            foreach (IDTSOutput100 output in ComponentMetaData.OutputCollection)
                if (output.IsErrorOut)
                    return output;

            throw new InvalidOperationException("There is no error output defined");
        }

        /// <summary>
        /// Gets the output columns for the default output
        /// </summary>
        /// <returns></returns>
        private IEnumerable<IDTSOutputColumn100> GetDefaultOutputColumns()
        {
            return GetOutputColumns(GetDefaultOutput());
        }

        /// <summary>
        /// Gets the output columns for the error output
        /// </summary>
        /// <returns></returns>
        private IEnumerable<IDTSOutputColumn100> GetErrorOutputColumns()
        {
            return GetOutputColumns(GetErrorOutput());
        }

        /// <summary>
        /// For the provided output, collects the output columns in an IEnumerable
        /// </summary>
        /// <param name="output"></param>
        /// <returns></returns>
        private static IEnumerable<IDTSOutputColumn100> GetOutputColumns(IDTSOutput100 output)
        {
            foreach (IDTSOutputColumn100 column in output.OutputColumnCollection)
                yield return column;
        }
        #endregion
    }

    #region helper classes
    internal class ColumnInfo
    {
        internal int OuputBufferColumnIndex;
        internal int ErrorOuputBufferColumnIndex;
        internal string ColumnName;
        internal DataType ColumnDataType;
        internal IDTSOutputColumn100 OutputColumn;
        internal IDTSOutputColumn100 ErrorOutputColumn;
    }

    internal class CollectionNameEditor : UITypeEditor
    {
        private IWindowsFormsEditorService edSvc = null;

        #region UITypeEditor members
        /// <summary>
        /// Edits the specified object's value using the editor style indicated by the
        ///     System.Drawing.Design.UITypeEditor.GetEditStyle() method.
        /// </summary>
        /// <param name="context">An System.ComponentModel.ITypeDescriptorContext that can be used to gain additional context information.</param>
        /// <param name="provider">An System.IServiceProvider that this editor can use to obtain services.</param>
        /// <param name="value">The object to edit.</param>
        /// <returns>The new value of the object. If the value of the object has not changed, this should return the same object it was passed.</returns>
        public override object EditValue(System.ComponentModel.ITypeDescriptorContext context, IServiceProvider provider, object value)
        {
            edSvc = (IWindowsFormsEditorService)provider.GetService(typeof(IWindowsFormsEditorService));

            if (edSvc != null)
            {
                MongoDatabase database = GetDatabase(context);

                if (database != null)
                {
                    ListBox lb = BuildListBox(database);

                    edSvc.DropDownControl(lb);

                    if (lb.SelectedItem != null)
                        return lb.SelectedItem;
                }
                else
                {
                    throw new Exception("No database connection found!");
                }
            }

            return value;
        }

        /// <summary>
        /// Gets the editor style used by the System.Drawing.Design.UITypeEditor.EditValue(System.IServiceProvider,System.Object)
        ///     method.
        /// </summary>
        /// <param name="context">An System.ComponentModel.ITypeDescriptorContext that can be used to gain additional context information.</param>
        /// <returns>A System.Drawing.Design.UITypeEditorEditStyle value that indicates the style
        ///     of editor used by the System.Drawing.Design.UITypeEditor.EditValue(System.IServiceProvider,System.Object)
        ///     method. If the System.Drawing.Design.UITypeEditor does not support this method,
        ///     then System.Drawing.Design.UITypeEditor.GetEditStyle() will return System.Drawing.Design.UITypeEditorEditStyle.None.</returns>
        public override UITypeEditorEditStyle GetEditStyle(System.ComponentModel.ITypeDescriptorContext context)
        {
            return UITypeEditorEditStyle.DropDown;
        }
        #endregion

        #region private methods
        private ListBox BuildListBox(MongoDatabase database)
        {
            ListBox lb = new ListBox();

            foreach (String name in database.GetCollectionNames())
                if (!name.StartsWith("system"))
                    lb.Items.Add(name);

            lb.SelectionMode = SelectionMode.One;
            lb.SelectedValueChanged += OnListBoxSelectedValueChanged;

            return lb;
        }

        private MongoDatabase GetDatabase(System.ComponentModel.ITypeDescriptorContext context)
        {
            Microsoft.SqlServer.Dts.Runtime.ConnectionManager cm = GetMongoDBConnectionManager(context);
            if (cm != null)
                return (MongoDatabase)cm.AcquireConnection(null);

            return null;
        }

        private Microsoft.SqlServer.Dts.Runtime.ConnectionManager GetMongoDBConnectionManager(System.ComponentModel.ITypeDescriptorContext context)
        {
            Microsoft.SqlServer.Dts.Runtime.Package package = GetPackageFromContext(context);

            return GetMongoDBConnectionManager(package);
        }

        private Microsoft.SqlServer.Dts.Runtime.ConnectionManager GetMongoDBConnectionManager(Microsoft.SqlServer.Dts.Runtime.Package package)
        {
            if (package != null)
                foreach (Microsoft.SqlServer.Dts.Runtime.ConnectionManager cm in package.Connections)
                    if (cm.CreationName.Equals(MongoDataSource.MONGODB_CONNECTION_MANAGER_NAME))
                        return cm;

            return null;
        }

        private Microsoft.SqlServer.Dts.Runtime.Package GetPackageFromContext(System.ComponentModel.ITypeDescriptorContext context)
        {
            PropertyInfo[] props = context.Instance.GetType().GetProperties();

            foreach (PropertyInfo propInfo in props)
            {
                if (propInfo.Name.Equals("PipelineTask"))
                {
                    Microsoft.SqlServer.Dts.Runtime.DtsContainer eventsProvider = (Microsoft.SqlServer.Dts.Runtime.EventsProvider)propInfo.GetValue(context.Instance, null);

                    while (eventsProvider.Parent != null)
                    {
                        if (eventsProvider.Parent.GetType() == typeof(Microsoft.SqlServer.Dts.Runtime.Package))
                            return (Microsoft.SqlServer.Dts.Runtime.Package)eventsProvider.Parent;
                        else
                            eventsProvider = eventsProvider.Parent;
                    }

                    throw new Exception("No package found for task!");
                }
            }

            return null;
        }

        private void OnListBoxSelectedValueChanged(object sender, EventArgs e)
        {
            // close the drop down as soon as something is clicked
            edSvc.CloseDropDown();
        }
        #endregion
    }
    #endregion
}
