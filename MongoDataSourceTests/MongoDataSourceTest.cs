/*
 * Copyright (c) 2012 Xbridge Ltd
 * See the file license.txt for copying permission.
 */

using System;
using Microsoft.SqlServer.Dts.Pipeline.Wrapper;

using Microsoft.SqlServer.Dts.Runtime.Wrapper;
using MongoDB.Bson;
using MongoDB.Driver;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using MongoDataSource;
using Telerik.JustMock;

///<summary>
///This is a test class for MongoDataSourceTest and is intended
///to contain all MongoDataSourceTest Unit Tests
///</summary>
[TestClass()]
public class MongoSourceTests
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
        CheckForCorrectColumnDataType( new BsonDateTime(new System.DateTime()), DataType.DT_DATE);
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
        MongoDataSource_Accessor target = new MongoDataSource_Accessor();
        Assert.AreEqual(dt, target.GetColumnDataType(value));

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

    private void CheckForCorrectDataTypeFromBson(BsonValue bsonValue, DataType dataType, object expectedValue)
    {
        MongoDataSource_Accessor target = new MongoDataSource_Accessor();

        object actual = target.GetDataTypeValueFromBsonValue(bsonValue, dataType);
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
        TestBuildOutputColumn("elname", new BsonDateTime(DateTime.Now), DataType.DT_DATE, 0,0);
    }

    ///<summary>
    ///A test for BuildOutputColumn
    ///</summary>
    [TestMethod(), DeploymentItem("MongoSsisDataSource.dll")]
    public void TestBuildStringOutputColumn()
    {
        TestBuildOutputColumn("elname", new BsonString("value"), DataType.DT_STR, 256,1252);
    }

    public void TestBuildOutputColumn(String elementname, BsonValue bsonValue, DataType expectedDataType, int length, int codepage)
    {
        MongoDataSource_Accessor target = new MongoDataSource_Accessor();

        IDTSOutputColumnCollection100 outputCollection = Mock.Create<IDTSOutputColumnCollection100>(Constructor.Mocked);

        IDTSOutputColumn100 expected = Mock.Create<IDTSOutputColumn100>(Constructor.Mocked);

        Mock.Arrange(() => outputCollection.New()).Returns(expected);

        BsonElement el = new BsonElement(elementname, bsonValue);
        Mock.ArrangeSet(() => expected.Name = Arg.Matches<String>(x => x == elementname));

        IDTSOutputColumn100 actual = target.BuildOutputColumn(outputCollection, el);

        Mock.Assert(() => expected.SetDataTypeProperties(expectedDataType, length, 0, 0, codepage));

    }

    /// <summary>
    ///A test for ReleaseConnections
    ///</summary>
    [TestMethod()]
    public void TestReleaseConnectionsDelegatesToConnectionManager()
    {
        MongoDataSource_Accessor target = new MongoDataSource_Accessor();

        IDTSConnectionManager100 connManager = Mock.Create<IDTSConnectionManager100>(Constructor.Mocked);
        target.m_ConnMgr = connManager;

        MongoDatabase mockedDb = Mock.Create<MongoDatabase>(Constructor.Mocked);
        target.database = mockedDb;

        target.ReleaseConnections();

        Mock.Assert(() => connManager.ReleaseConnection(mockedDb));
    }

    /// <summary>
    ///A test for BuildExternalMetadataColumn
    ///</summary>
    [TestMethod()]
    [DeploymentItem("MongoSsisDataSource.dll")]
    public void PopulateExternalMetadataColumnTest()
    {
        MongoDataSource_Accessor target = new MongoDataSource_Accessor();
        IDTSOutputColumn100 outputColumn = Mock.Create<IDTSOutputColumn100>(Constructor.Mocked);

        String expectedName = "name";
        int expectedPrecision = 1;
        int expectedLength = 2;
        int expectedScale = 3;
        DataType expectedDataType = DataType.DT_TEXT;

        Mock.Arrange(() => outputColumn.Name).Returns(expectedName);
        Mock.Arrange(() => outputColumn.Precision).Returns(expectedPrecision);
        Mock.Arrange(() => outputColumn.Length).Returns(expectedLength);
        Mock.Arrange(() => outputColumn.DataType).Returns(expectedDataType);
        Mock.Arrange(() => outputColumn.Scale).Returns(expectedScale);

        IDTSExternalMetadataColumn100 expected = Mock.Create<IDTSExternalMetadataColumn100>(Constructor.Mocked);

        Mock.ArrangeSet(() => expected.Name = Arg.Matches<String>(x => x == expectedName)).OccursOnce();
        Mock.ArrangeSet(() => expected.Precision = Arg.Matches<int>(x => x == expectedPrecision)).OccursOnce();
        Mock.ArrangeSet(() => expected.Length = Arg.Matches<int>(x => x == expectedLength)).OccursOnce();
        Mock.ArrangeSet(() => expected.DataType = Arg.Matches<DataType>(x => x == expectedDataType)).OccursOnce();
        Mock.ArrangeSet(() => expected.Scale = Arg.Matches<int>(x => x == expectedScale)).OccursOnce();

        target.PopulateExternalMetadataColumn(expected, outputColumn);

        Mock.Assert(expected);
    }
}