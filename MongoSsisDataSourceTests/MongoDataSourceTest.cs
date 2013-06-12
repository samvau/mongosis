/*
 * Copyright (c) 2012-2013 Xbridge Ltd
 * See the file license.txt for copying permission.
 */

using System;
using Microsoft.SqlServer.Dts.Pipeline.Wrapper;
using Microsoft.SqlServer.Dts.Runtime.Wrapper;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using MongoDataSource;
using MongoDB.Bson;
using MongoDB.Driver;
using Telerik.JustMock;

namespace MongoSourceTests
{

    /// <summary>
    ///This is a test class for MongoDataSourceTest and is intended
    ///to contain all MongoDataSourceTest Unit Tests
    ///</summary>
    [TestClass()]
    public class MongoDataSourceTest
    {

        ///<summary>
        ///A test for GetColumnDataType
        ///</summary>
        [TestMethod(), DeploymentItem("MongoSsisDataSource.dll")]
        public void TestGetColumnDataTypeReturnsIntegerTypeForBsonInteger()
        {
            CheckForCorrectColumnDataType(new BsonInt32(12), DataType.DT_I8);

        }

        ///<summary>
        ///A test for GetColumnDataType
        ///</summary>
        [TestMethod(), DeploymentItem("MongoSsisDataSource.dll")]
        public void TestGetColumnDataTypeReturnsDoubleTypeForBsonDouble()
        {
            CheckForCorrectColumnDataType(new BsonDouble(0.2), DataType.DT_R8);
        }

        ///<summary>
        ///A test for GetColumnDataType
        ///</summary>
        [TestMethod(), DeploymentItem("MongoSsisDataSource.dll")]
        public void TestGetColumnDataTypeReturnsDateTypeForBsonDate()
        {
            CheckForCorrectColumnDataType(new BsonDateTime(new System.DateTime()), DataType.DT_DATE);
        }

        ///<summary>
        ///A test for GetColumnDataType
        ///</summary>
        [TestMethod(), DeploymentItem("MongoSsisDataSource.dll")]
        public void TestGetColumnDataTypeReturnsDateTypeForBsonString()
        {
            CheckForCorrectColumnDataType(new BsonString("value"), DataType.DT_STR);
        }

        ///<summary>
        ///A test for GetColumnDataType
        ///</summary>
        [TestMethod(), DeploymentItem("MongoSsisDataSource.dll")]
        public void TestGetColumnDataTypeReturnsBooleanTypeForBsonBoolean()
        {
            CheckForCorrectColumnDataType(BsonBoolean.True, DataType.DT_BOOL);
        }

        private void CheckForCorrectColumnDataType(BsonValue value, DataType dt)
        {
            PrivateObject p = new PrivateObject(typeof(MongoDataSource.MongoDataSource));
            Assert.AreEqual(dt, (DataType)p.Invoke("GetColumnDataType", new object[] { value}));

        }

        ///<summary>
        ///A test for GetDataTypeValueFromBsonValue
        ///</summary>
        [TestMethod(), DeploymentItem("MongoSsisDataSource.dll")]
        public void GetDataTypeValueFromBsonValueTestWithInt32()
        {
            Int64 expected = 1234;
            CheckForCorrectDataTypeFromBson(new BsonInt32(1234), DataType.DT_I4, expected);
        }

        ///<summary>
        ///A test for GetDataTypeValueFromBsonValue
        ///</summary>
        [TestMethod(), DeploymentItem("MongoSsisDataSource.dll")]
        public void GetDataTypeValueFromBsonValueTestWithInt64()
        {
            Int64 expected = 1234;
            CheckForCorrectDataTypeFromBson(new BsonInt64(expected), DataType.DT_I8, expected);
        }

        ///<summary>
        ///A test for GetDataTypeValueFromBsonValue
        ///</summary>
        [TestMethod(), DeploymentItem("MongoSsisDataSource.dll")]
        public void GetDataTypeValueFromBsonValueTestWithBoolean()
        {
            CheckForCorrectDataTypeFromBson(BsonBoolean.True, DataType.DT_BOOL, true);
            CheckForCorrectDataTypeFromBson(BsonBoolean.False, DataType.DT_BOOL, false);
        }

        ///<summary>
        ///A test for GetDataTypeValueFromBsonValue
        ///</summary>
        [TestMethod(), DeploymentItem("MongoSsisDataSource.dll")]
        public void GetDataTypeValueFromBsonValueTestWithShortDouble()
        {
            Double expected = 1234.1;
            CheckForCorrectDataTypeFromBson(new BsonDouble(expected), DataType.DT_R4, expected);
        }

        ///<summary>
        ///A test for GetDataTypeValueFromBsonValue
        ///</summary>
        [TestMethod(), DeploymentItem("MongoSsisDataSource.dll")]
        public void GetDataTypeValueFromBsonValueTestWithLongDouble()
        {
            Double expected = 1234.1;
            CheckForCorrectDataTypeFromBson(new BsonDouble(expected), DataType.DT_R8, expected);
        }

        ///<summary>
        ///A test for GetDataTypeValueFromBsonValue
        ///</summary>
        [TestMethod(), DeploymentItem("MongoSsisDataSource.dll")]
        public void GetDataTypeValueFromBsonValueTestWithString()
        {
            String expected = "value";
            CheckForCorrectDataTypeFromBson(new BsonString(expected), DataType.DT_STR, expected);
        }

        ///<summary>
        ///A test for GetDataTypeValueFromBsonValue
        ///</summary>
        [TestMethod(), DeploymentItem("MongoSsisDataSource.dll")]
        public void GetDataTypeValueFromBsonValueTestWithDate()
        {
            System.DateTime today = System.DateTime.Today;
            CheckForCorrectDataTypeFromBson(new BsonDateTime(today), DataType.DT_DATE, today);
        }

        ///<summary>
        ///A test for GetDataTypeValueFromBsonValue
        ///</summary>
        [TestMethod(), DeploymentItem("MongoSsisDataSource.dll")]
        public void GetDataTypeValueFromBsonValueTestWithDateTimeOffset()
        {
            System.DateTime today = System.DateTime.Today;
            CheckForCorrectDataTypeFromBson(new BsonDateTime(today), DataType.DT_DBTIMESTAMPOFFSET, today);
        }

        ///<summary>
        ///A test for GetDataTypeValueFromBsonValue
        ///</summary>
        [TestMethod(), DeploymentItem("MongoSsisDataSource.dll")]
        public void GetDataTypeValueFromBsonValueTestWithDateTime()
        {
            System.DateTime today = System.DateTime.Today;
            CheckForCorrectDataTypeFromBson(new BsonDateTime(today), DataType.DT_DBTIMESTAMP, today);
        }

        ///<summary>
        ///A test for GetDataTypeValueFromBsonValue
        ///</summary>
        [TestMethod(), DeploymentItem("MongoSsisDataSource.dll")]
        public void GetDataTypeValueFromBsonValueTestWithObjectId()
        {
            ObjectId objId = new ObjectId();
            CheckForCorrectDataTypeFromBson(new BsonObjectId(objId), DataType.DT_STR, objId.ToString());
        }

        ///<summary>
        ///A test for GetDataTypeValueFromBsonValue when input is string and output is integer
        ///</summary>
        [TestMethod(), DeploymentItem("MongoSsisDataSource.dll")]
        public void GetDataTypeValueFromBsonValueTestWithStringToInteger()
        {
            String inputInteger = "1234";
            Int64 expectedInt = 1234;
            CheckForCorrectDataTypeFromBson(new BsonString(inputInteger), DataType.DT_I8, expectedInt);
        }

        private void CheckForCorrectDataTypeFromBson(BsonValue bsonValue, DataType dataType, object expectedValue)
        {
            PrivateObject p = new PrivateObject(typeof(MongoDataSource.MongoDataSource));
            object actual = p.Invoke("GetDataTypeValueFromBsonValue", new object[] { bsonValue, dataType });
            Assert.AreEqual(expectedValue, actual);
        }

        ///<summary>
        ///A test for BuildOutputColumn
        ///</summary>
        [TestMethod(), DeploymentItem("MongoSsisDataSource.dll")]
        public void TestBuildBooleanOutputColumn()
        {
            TestBuildOutputColumn("elname", BsonBoolean.True, DataType.DT_BOOL, 0, 0);
        }

        ///<summary>
        ///A test for BuildOutputColumn
        ///</summary>
        [TestMethod(), DeploymentItem("MongoSsisDataSource.dll")]
        public void TestBuildIntegerOutputColumn()
        {
            TestBuildOutputColumn("elname", new BsonInt64(123), DataType.DT_I8, 0, 0);
        }

        ///<summary>
        ///A test for BuildOutputColumn
        ///</summary>
        [TestMethod(), DeploymentItem("MongoSsisDataSource.dll")]
        public void TestBuildDoubleOutputColumn()
        {
            TestBuildOutputColumn("elname", new BsonDouble(12.3), DataType.DT_R8, 0, 0);
        }

        ///<summary>
        ///A test for BuildOutputColumn
        ///</summary>
        [TestMethod(), DeploymentItem("MongoSsisDataSource.dll")]
        public void TestBuildDateOutputColumn()
        {
            TestBuildOutputColumn("elname", new BsonDateTime(DateTime.Now), DataType.DT_DATE, 0, 0);
        }

        ///<summary>
        ///A test for BuildOutputColumn
        ///</summary>
        [TestMethod(), DeploymentItem("MongoSsisDataSource.dll")]
        public void TestBuildStringOutputColumn()
        {
            TestBuildOutputColumn("elname", new BsonString("value"), DataType.DT_STR, 256, 1252);
        }

        public void TestBuildOutputColumn(String elementname, BsonValue bsonValue, DataType expectedDataType, int length, int codepage)
        {
            PrivateObject p = new PrivateObject(typeof(MongoDataSource.MongoDataSource));

            IDTSOutput100 output = Mock.Create<IDTSOutput100>(Constructor.Mocked);

            IDTSOutputColumnCollection100 outputCollection = Mock.Create<IDTSOutputColumnCollection100>(Constructor.Mocked);

            IDTSOutputColumn100 expected = Mock.Create<IDTSOutputColumn100>(Constructor.Mocked);

            Mock.Arrange(() => outputCollection.New()).Returns(expected);
            Mock.Arrange(() => output.OutputColumnCollection).Returns(outputCollection);
            Mock.Arrange(() => output.IsErrorOut).Returns(true);

            BsonElement el = new BsonElement(elementname, bsonValue);
            Mock.ArrangeSet(() => expected.Name = Arg.Matches<String>(x => x == elementname));

            IDTSOutputColumn100 actual = (IDTSOutputColumn100)p.Invoke("BuildOutputColumn", new object[] { output, el });

            Mock.Assert(() => expected.SetDataTypeProperties(expectedDataType, length, 0, 0, codepage));
            Mock.ArrangeSet(() => expected.ErrorRowDisposition = Arg.Matches<DTSRowDisposition>(x => x == DTSRowDisposition.RD_FailComponent)).OccursOnce();
            Mock.ArrangeSet(() => expected.TruncationRowDisposition = Arg.Matches<DTSRowDisposition>(x => x == DTSRowDisposition.RD_FailComponent)).OccursOnce();
        }

        /// <summary>
        ///A test for ReleaseConnections
        ///</summary>
        [TestMethod()]
        public void TestReleaseConnectionsDelegatesToConnectionManager()
        {
            PrivateObject p = new PrivateObject(typeof(MongoDataSource.MongoDataSource));

            IDTSConnectionManager100 connManager = Mock.Create<IDTSConnectionManager100>(Constructor.Mocked);
            p.SetField("m_ConnMgr", connManager);

            MongoDatabase mockedDb = Mock.Create<MongoDatabase>(Constructor.Mocked);
            p.SetField("database", mockedDb);

            p.Invoke("ReleaseConnections", null);

            Mock.Assert(() => connManager.ReleaseConnection(mockedDb));
        }

        /// <summary>
        ///A test for BuildExternalMetadataColumn
        ///</summary>
        [TestMethod()]
        [DeploymentItem("MongoSsisDataSource.dll")]
        public void BuildExternalMetadataColumnTest()
        {
            PrivateObject p = new PrivateObject(typeof(MongoDataSource.MongoDataSource));

            IDTSOutputColumn100 outputColumn = Mock.Create<IDTSOutputColumn100>(Constructor.Mocked);

            String expectedName = "name";
            int expectedPrecision = 1;
            int expectedLength = 2;
            int expectedScale = 3;
            DataType expectedDataType = DataType.DT_TEXT;
            int expectedCodePage = 4;

            Mock.Arrange(() => outputColumn.Name).Returns(expectedName);
            Mock.Arrange(() => outputColumn.Precision).Returns(expectedPrecision);
            Mock.Arrange(() => outputColumn.Length).Returns(expectedLength);
            Mock.Arrange(() => outputColumn.DataType).Returns(expectedDataType);
            Mock.Arrange(() => outputColumn.Scale).Returns(expectedScale);
            Mock.Arrange(() => outputColumn.CodePage).Returns(expectedCodePage);

            IDTSExternalMetadataColumnCollection100 outputCollection = Mock.Create<IDTSExternalMetadataColumnCollection100>(Constructor.Mocked);
            IDTSExternalMetadataColumn100 expected = Mock.Create<IDTSExternalMetadataColumn100>(Constructor.Mocked);

            Mock.Arrange(() => outputCollection.New()).Returns(expected);

            Mock.ArrangeSet(() => expected.Name = Arg.Matches<String>(x => x == expectedName)).OccursOnce();
            Mock.ArrangeSet(() => expected.Precision = Arg.Matches<int>(x => x == expectedPrecision)).OccursOnce();
            Mock.ArrangeSet(() => expected.Length = Arg.Matches<int>(x => x == expectedLength)).OccursOnce();
            Mock.ArrangeSet(() => expected.DataType = Arg.Matches<DataType>(x => x == expectedDataType)).OccursOnce();
            Mock.ArrangeSet(() => expected.Scale = Arg.Matches<int>(x => x == expectedScale)).OccursOnce();
            Mock.ArrangeSet(() => expected.CodePage = Arg.Matches<int>(x => x == expectedCodePage)).OccursOnce();

            IDTSExternalMetadataColumn100 actual = (IDTSExternalMetadataColumn100)p.Invoke("BuildExternalMetadataColumn", new object[] { outputCollection, outputColumn });

            Mock.Assert(expected);
        }

        /// <summary>
        ///A test for AddCustomProperties
        ///</summary>
        [TestMethod()]
        [DeploymentItem("MongoSsisDataSource.dll")]
        public void AddCustomPropertiesTest()
        {
            PrivateObject p = new PrivateObject(typeof(MongoDataSource.MongoDataSource));

            IDTSCustomPropertyCollection100 customPropertyCollection = Mock.Create<IDTSCustomPropertyCollection100>();

            IDTSCustomProperty100 collectionNameProp = Mock.Create<IDTSCustomProperty100>();
            IDTSCustomProperty100 conditionalFieldProp = Mock.Create<IDTSCustomProperty100>();
            IDTSCustomProperty100 fromValueProp = Mock.Create<IDTSCustomProperty100>();
            IDTSCustomProperty100 toValueProp = Mock.Create<IDTSCustomProperty100>();
            IDTSCustomProperty100 queryProp = Mock.Create<IDTSCustomProperty100>();
            IDTSCustomProperty100 sampleSizeProp = Mock.Create<IDTSCustomProperty100>();
            IDTSCustomProperty100 sampleOffsetProp = Mock.Create<IDTSCustomProperty100>();

            Mock.Arrange(() => customPropertyCollection.New()).Returns(collectionNameProp).InSequence();
            Mock.Arrange(() => customPropertyCollection.New()).Returns(conditionalFieldProp).InSequence();
            Mock.Arrange(() => customPropertyCollection.New()).Returns(fromValueProp).InSequence();
            Mock.Arrange(() => customPropertyCollection.New()).Returns(toValueProp).InSequence();
            Mock.Arrange(() => customPropertyCollection.New()).Returns(queryProp).InSequence();
            Mock.Arrange(() => customPropertyCollection.New()).Returns(sampleSizeProp).InSequence();
            Mock.Arrange(() => customPropertyCollection.New()).Returns(sampleOffsetProp).InSequence();

            System.Reflection.BindingFlags flags = System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.NonPublic;

            assertSetPropertyNameAndDescription(collectionNameProp, (string)p.GetField("COLLECTION_NAME_PROP_NAME", flags));
            Mock.ArrangeSet(() => collectionNameProp.UITypeEditor = Arg.Matches<string>(x => x == typeof(CollectionNameEditor).AssemblyQualifiedName));

            assertSetPropertyNameAndDescription(conditionalFieldProp, (string)p.GetField("CONDITIONAL_FIELD_PROP_NAME", flags));
            assertSetPropertyNameAndDescription(fromValueProp, (string)p.GetField("CONDITION_FROM_PROP_NAME", flags));
            assertSetPropertyNameAndDescription(toValueProp, (string)p.GetField("CONDITION_TO_PROP_NAME", flags));
            assertSetPropertyNameAndDescription(queryProp, (string)p.GetField("CONDITION_DOC_PROP_NAME", flags));
            assertSetPropertyNameAndDescription(sampleSizeProp, (string)p.GetField("SAMPLE_SIZE_PROP_NAME", flags));
            assertSetPropertyNameAndDescription(sampleOffsetProp, (string)p.GetField("SAMPLE_OFFSET_PROP_NAME", flags));

            p.Invoke("AddCustomProperties", new object[] { customPropertyCollection });

            Mock.Assert(() => customPropertyCollection.New(), Occurs.Exactly(7));

            Mock.Assert(collectionNameProp);
            Mock.Assert(conditionalFieldProp);
            Mock.Assert(fromValueProp);
            Mock.Assert(toValueProp);
            Mock.Assert(queryProp);
            Mock.Assert(sampleSizeProp);
            Mock.Assert(sampleOffsetProp);
        }

        private void assertSetPropertyNameAndDescription(IDTSCustomProperty100 propMock, String propName)
        {
            Mock.ArrangeSet(() => propMock.Description = Arg.IsAny<String>()).OccursOnce();
            Mock.ArrangeSet(() => propMock.Name = Arg.Matches<String>(x => x == propName)).OccursOnce();
            Mock.ArrangeSet(() => propMock.Value = Arg.IsAny<dynamic>()).OccursOnce();
        }

        /// <summary>
        ///A test for BuildQuery
        ///</summary>
        [TestMethod()]
        [DeploymentItem("MongoSsisDataSource.dll")]
        public void BuildQueryBuildsQueryWithFromAndToTest()
        {
            PrivateObject p = new PrivateObject(typeof(MongoDataSource.MongoDataSource));
            System.Reflection.BindingFlags flags = System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.InvokeMethod;
            string fieldName = "field1";
            string fromVal = "fromval";
            string toVal = "toval";

            Type[] types = new Type[] { typeof(string), typeof(BsonValue), typeof(BsonValue) };
            object[] pars = new object[] { fieldName, (BsonValue)fromVal, (BsonValue)toVal };
            IMongoQuery query = (IMongoQuery)p.Invoke("BuildQuery", flags, types, pars);

            Assert.AreEqual("{ \"" + fieldName + "\" : { \"$gte\" : \"" + fromVal + "\", \"$lte\" : \"" + toVal + "\" } }", query.ToString());
        }

        /// <summary>
        ///A test for BuildQuery
        ///</summary>
        [TestMethod()]
        [DeploymentItem("MongoSsisDataSource.dll")]
        public void BuildQueryBuildsQueryWithFromOnlyTest()
        {
            PrivateObject p = new PrivateObject(typeof(MongoDataSource.MongoDataSource));
            System.Reflection.BindingFlags flags = System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.InvokeMethod;

            string fieldName = "field1";
            string fromVal = "fromval";

            Type[] types = new Type[] { typeof(string), typeof(BsonValue), typeof(BsonValue) };
            object[] pars = new object[] { fieldName, (BsonValue)fromVal, (BsonValue)null};
            IMongoQuery query = (IMongoQuery)p.Invoke("BuildQuery", flags, types, pars);

            Assert.AreEqual("{ \"" + fieldName + "\" : { \"$gte\" : \"" + fromVal + "\" } }", query.ToString());
        }

        /// <summary>
        ///A test for BuildQuery
        ///</summary>
        [TestMethod()]
        [DeploymentItem("MongoSsisDataSource.dll")]
        public void BuildQueryBuildsQueryWithToOnlyTest()
        {
            PrivateObject p = new PrivateObject(typeof(MongoDataSource.MongoDataSource));

            string fieldName = "field1";
            string toVal = "toval";

            IMongoQuery query = (IMongoQuery)p.Invoke("BuildQuery", new object[] { fieldName, (BsonValue)null, (BsonValue)toVal });

            Assert.AreEqual("{ \"" + fieldName + "\" : { \"$lte\" : \"" + toVal + "\" } }", query.ToString());
        }

        /// <summary>
        ///A test for ParseConditionValue
        ///</summary>
        [TestMethod()]
        [DeploymentItem("MongoSsisDataSource.dll")]
        public void ParseConditionValueForSimpleDateTest()
        {
            PrivateObject p = new PrivateObject(typeof(MongoDataSource.MongoDataSource));
            String value = "12/12/2012";
            object parsedValue = p.Invoke("ParseConditionValue", new object[] { value, DataType.DT_DATE });
            Assert.AreEqual(DateTime.Parse(value).ToUniversalTime().ToLongDateString(), ((BsonDateTime)parsedValue).AsDateTime.ToLongDateString());
        }

        /// <summary>
        ///A test for ParseConditionValue
        ///</summary>
        [TestMethod()]
        [DeploymentItem("MongoSsisDataSource.dll")]
        public void ParseConditionValueForNowTest()
        {
            PrivateObject p = new PrivateObject(typeof(MongoDataSource.MongoDataSource));

            object parsedValue = p.Invoke("ParseConditionValue", new object[] { "now", DataType.DT_DATE });

            Assert.IsTrue(parsedValue is BsonDateTime);

            Assert.AreEqual(DateTime.Now.ToLongDateString(), ((BsonDateTime)parsedValue).AsDateTime.ToLongDateString());
        }

        /// <summary>
        ///A test for ParseConditionValue
        ///</summary>
        [TestMethod()]
        [DeploymentItem("MongoSsisDataSource.dll")]
        public void ParseConditionValueForTodayTest()
        {
            PrivateObject p = new PrivateObject(typeof(MongoDataSource.MongoDataSource));

            object parsedValue = p.Invoke("ParseConditionValue", new object[] { "today", DataType.DT_DATE });

            Assert.AreEqual(DateTime.Now.ToLongDateString(), ((BsonDateTime)parsedValue).AsDateTime.ToLongDateString());
        }

        /// <summary>
        ///A test for ParseConditionValue
        ///</summary>
        [TestMethod()]
        public void ParseConditionValueForYesterdayTest()
        {
            PrivateObject p = new PrivateObject(typeof(MongoDataSource.MongoDataSource));

            object parsedValue = p.Invoke("ParseConditionValue", new object[] { "yesterday", DataType.DT_DATE });

            Assert.AreEqual(DateTime.Now.AddDays(-1).ToLongDateString(), ((BsonDateTime)parsedValue).AsDateTime.ToLongDateString());
        }

        /// <summary>
        ///A test for ParseConditionValue
        ///</summary>
        [TestMethod()]
        public void ParseConditionValueForRelativeDateTest()
        {
            PrivateObject p = new PrivateObject(typeof(MongoDataSource.MongoDataSource));

            object parsedValue = p.Invoke("ParseConditionValue", new object[] { "-2", DataType.DT_DATE });

            Assert.AreEqual(DateTime.Now.AddDays(-2).ToLongDateString(), ((BsonDateTime)parsedValue).AsDateTime.ToLongDateString());

            parsedValue = p.Invoke("ParseConditionValue", new object[] { "-4", DataType.DT_DATE });

            Assert.AreEqual(DateTime.Now.AddDays(-4).ToLongDateString(), ((BsonDateTime)parsedValue).AsDateTime.ToLongDateString());
        }

        /// <summary>
        ///A test for ParseConditionValue
        ///</summary>
        [TestMethod()]
        public void ParseConditionValueForIntTest()
        {
            PrivateObject p = new PrivateObject(typeof(MongoDataSource.MongoDataSource));

            string value = "123";
            BsonInt64 expected = new BsonInt64(Int64.Parse(value));
            object parsedValue = p.Invoke("ParseConditionValue", new object[] { value, DataType.DT_I8 });

            Assert.AreEqual(expected, parsedValue);

            parsedValue = p.Invoke("ParseConditionValue", new object[] { value, DataType.DT_I4 });

            Assert.AreEqual(expected, parsedValue);
        }

        /// <summary>
        ///A test for ParseConditionValue
        ///</summary>
        [TestMethod()]
        public void ParseConditionValueForStringTest()
        {
            PrivateObject p = new PrivateObject(typeof(MongoDataSource.MongoDataSource));

            string value = "blah";
            object parsedValue = p.Invoke("ParseConditionValue", new object[] { value, DataType.DT_STR });

            Assert.AreEqual(new BsonString(value), parsedValue);
        }

        /// <summary>
        ///A test for ParseConditionValue
        ///</summary>
        [TestMethod()]
        public void ParseConditionValueForDoubleTest()
        {
            PrivateObject p = new PrivateObject(typeof(MongoDataSource.MongoDataSource));

            string value = "12" + System.Globalization.CultureInfo.CurrentCulture.NumberFormat.NumberDecimalSeparator + "34";
            BsonDouble expected = new BsonDouble(Double.Parse(value));

            object parsedValue = p.Invoke("ParseConditionValue", new object[] { value, DataType.DT_R8 });

            Assert.AreEqual(expected, parsedValue);

            parsedValue = p.Invoke("ParseConditionValue", new object[] { value, DataType.DT_R4 });

            Assert.AreEqual(expected, parsedValue);
        }
    }
}
