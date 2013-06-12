using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.SqlServer.Dts.Runtime.Wrapper;
using MongoDB.Bson;
using MongoDataSource;
using System.Collections.Generic;

namespace MongoSourceTests
{
    [TestClass]
    public class MongoDataDestinationTest
    {
        private BsonDocument _testDoc = BsonDocument.Parse("{ 'str' : 'stringvalue' , 'dbl' : 1234.78934 , 'nested' : { 'n1' : 'a', 'n2' : 'b' }, 'nullval' : '\0' }");

        [TestMethod(), DeploymentItem("MongoSsisDataSource")]
        public void TestCreateBsonValueTimeStamp()
        {
            DateTime t = DateTime.ParseExact("2013-06-11 12:51", "yyyy-MM-dd HH:mm", null);

            PrivateObject p = new PrivateObject(typeof(MongoDataSource.MongoDataDestination));
            BsonValue bv = (BsonValue)p.Invoke("CreateBsonValue", new object[] { t, new DestinationColumnInfo { DataType=DataType.DT_DBTIMESTAMP2 }});
            Assert.AreEqual(t.ToString(), ((DateTime)bv).ToString());
        }

        [TestMethod(), DeploymentItem("MongoSsisDataSource")]
        public void TestCreateBsonValueDate()
        {
            DateTime t = DateTime.ParseExact("2013-06-11", "yyyy-MM-dd", null);

            PrivateObject p = new PrivateObject(typeof(MongoDataSource.MongoDataDestination));
            BsonValue bv = (BsonValue)p.Invoke("CreateBsonValue", new object[] { t, new DestinationColumnInfo { DataType = DataType.DT_DATE } });
            Assert.AreEqual(t.ToString(), ((DateTime)bv).ToString());
        }

        [TestMethod(), DeploymentItem("MongoSsisDataSource")]
        public void TestCreateBsonValueBoolean()
        {
            bool b = false;

            PrivateObject p = new PrivateObject(typeof(MongoDataSource.MongoDataDestination));
            BsonValue bv = (BsonValue)p.Invoke("CreateBsonValue", new object[] { b, new DestinationColumnInfo { DataType = DataType.DT_BOOL } });
            Assert.AreEqual(b, bv);
        }

        [TestMethod(), DeploymentItem("MongoSsisDataSource")]
        public void TestCreateBsonValueInteger()
        {
            int i = 18;

            PrivateObject p = new PrivateObject(typeof(MongoDataSource.MongoDataDestination));
            BsonValue bv = (BsonValue)p.Invoke("CreateBsonValue", new object[] { i, new DestinationColumnInfo { DataType = DataType.DT_I4 } });
            Assert.AreEqual(i, bv);
        }

        [TestMethod(), DeploymentItem("MongoSsisDataSource")]
        public void TestCreateBsonValueDouble()
        {
            double d = 18.98765376;

            PrivateObject p = new PrivateObject(typeof(MongoDataSource.MongoDataDestination));
            BsonValue bv = (BsonValue)p.Invoke("CreateBsonValue", new object[] { d, new DestinationColumnInfo { DataType = DataType.DT_R8 } });
            Assert.AreEqual(d, bv);
        }

        [TestMethod(), DeploymentItem("MongoSsisDataSource")]
        public void TestCreateBsonValueNull()
        {
            BsonValue bvExp = BsonNull.Value;

            PrivateObject p = new PrivateObject(typeof(MongoDataSource.MongoDataDestination));
            BsonValue bv = (BsonValue)p.Invoke("CreateBsonValue", new object[] { null, new DestinationColumnInfo { DataType = DataType.DT_NULL } });
            Assert.AreEqual(bvExp, bv);
        }

        [TestMethod(), DeploymentItem("MongoSsisDataSource")]
        public void TestDestinationColumnInfoDepth()
        {
            List<DestinationColumnInfo> lst = new List<DestinationColumnInfo>();
            lst.Add(new DestinationColumnInfo { ColumnName = "a.b.c.d" });
            lst.Add(new DestinationColumnInfo { ColumnName = "x" });
            Assert.AreEqual(4, lst.Depth());
        }

        [TestMethod(), DeploymentItem("MongoSsisDataSource")]
        public void TestInsertDotNotatedRoot()
        {
            BsonDocument doc = (BsonDocument)_testDoc.DeepClone();
            DestinationColumnInfo ci = new DestinationColumnInfo();
            ci.ColumnName = "dbl";
            doc.InsertDotNotated(ci, 5.66);
            Assert.AreEqual(5.66, doc["dbl"]);
        }

        [TestMethod(), DeploymentItem("MongoSsisDataSource")]
        public void TestInsertDotNotatedNested()
        {
            BsonDocument doc = (BsonDocument)_testDoc.DeepClone();
            DestinationColumnInfo ci = new DestinationColumnInfo();
            ci.ColumnName = "nested.n2";
            doc.InsertDotNotated(ci, 5.66);
            Assert.AreEqual(5.66, ((BsonDocument)doc["nested"])["n2"]);
        }

        [TestMethod(), DeploymentItem("MongoSsisDataSource")]
        public void TestBsonInsertDocument_Create()
        {
            List<DestinationColumnInfo> cols = new List<DestinationColumnInfo>();
            cols.Add(new DestinationColumnInfo { BufferColumnIndex = 0, ColumnName = "a", DataType = Microsoft.SqlServer.Dts.Runtime.Wrapper.DataType.DT_I4 });
            cols.Add(new DestinationColumnInfo { BufferColumnIndex = 1, ColumnName = "b.b1", DataType = Microsoft.SqlServer.Dts.Runtime.Wrapper.DataType.DT_DATE });
            cols.Add(new DestinationColumnInfo { BufferColumnIndex = 2, ColumnName = "c", DataType = Microsoft.SqlServer.Dts.Runtime.Wrapper.DataType.DT_R8 });
            cols.Add(new DestinationColumnInfo { BufferColumnIndex = 3, ColumnName = "b.b2", DataType = Microsoft.SqlServer.Dts.Runtime.Wrapper.DataType.DT_R8 });

            BsonInsertDocument template = new BsonInsertDocument(cols);
            BsonDocument doc = template.Create();

            Assert.IsNotNull(doc["a"]);
            Assert.IsNotNull(doc["b"]);
            Assert.IsNotNull(doc["b"].AsBsonDocument["b1"]);
        }
    }
}
