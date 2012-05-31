using Microsoft.VisualBasic;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using Microsoft.SqlServer.Dts.Pipeline.Wrapper;

using Microsoft.SqlServer.Dts.Runtime.Wrapper;
using MongoDB.Bson;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using MongoDataSource;
using Telerik.JustMock;

///<summary>
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
        CheckForCorrectColumnDataType( new BsonDateTime(new System.DateTime()), DataType.DT_DATE);
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
        CheckForCorrectDataTypeFromBson(new BsonInt64(1234), DataType.DT_I4, new BsonInt64(1234).ToInt64());
    }

    ///<summary>
    ///A test for GetDataTypeValueFromBsonValue
    ///</summary>
    [TestMethod(), DeploymentItem("MongoSsisDataSource.dll")]
    public void GetDataTypeValueFromBsonValueTestWithInt64()
    {
        CheckForCorrectDataTypeFromBson(new BsonInt64(1234), DataType.DT_I8, new BsonInt64(1234).ToInt64());
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
        CheckForCorrectDataTypeFromBson(new BsonDouble(1234.1), DataType.DT_R4, 1234.1);
    }

    ///<summary>
    ///A test for GetDataTypeValueFromBsonValue
    ///</summary>
    [TestMethod(), DeploymentItem("MongoSsisDataSource.dll")]
    public void GetDataTypeValueFromBsonValueTestWithLongDouble()
    {
        CheckForCorrectDataTypeFromBson(new BsonDouble(1234.1), DataType.DT_R8, 1234.1);
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

        object actual = null;
        actual = target.GetDataTypeValueFromBsonValue(bsonValue, dataType);
        Assert.AreEqual(expectedValue, actual);
    }

    ///<summary>
    ///A test for BuildOutputColumn
    ///</summary>
    [TestMethod(), DeploymentItem("MongoSsisDataSource.dll")]
    public void TestBuildBooleanOutputColumn()
    {
        MongoDataSource_Accessor target = new MongoDataSource_Accessor();

        IDTSOutputColumnCollection100 outputCollection = Mock.Create<IDTSOutputColumnCollection100>(Constructor.Mocked);

        IDTSOutputColumn100 expected = Mock.Create<IDTSOutputColumn100>(Constructor.Mocked);

        Mock.Arrange(() => outputCollection.New()).Returns(expected);

        String elName = "elName";
        BsonValue value = BsonBoolean.True;

        BsonElement el = new BsonElement(elName, value);
        Mock.ArrangeSet(() => expected.Name = Arg.Matches<String>(x => x == elName));

        IDTSOutputColumn100 actual = target.BuildOutputColumn(outputCollection, el);

        Mock.Assert(() => expected.SetDataTypeProperties(DataType.DT_BOOL, 0, 0, 0, 0));
    }
}
