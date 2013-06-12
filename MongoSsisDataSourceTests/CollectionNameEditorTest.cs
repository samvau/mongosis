/*
 * Copyright (c) 2012-2013 Xbridge Ltd
 * See the file license.txt for copying permission.
 */

using System.Drawing.Design;
using System.Windows.Forms;
using System.Windows.Forms.Design;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using MongoDataSource;
using MongoDB.Driver;
using Telerik.JustMock;

namespace MongoSourceTests
{

    /// <summary>
    ///This is a test class for CollectionNameEditorTest and is intended
    ///to contain all CollectionNameEditorTest Unit Tests
    ///</summary>
    [TestClass()]
    public class CollectionNameEditorTest
    {

        /// <summary>
        ///A test for EditValue
        ///</summary>
        [TestMethod()]
        public void EditValueTest()
        {
            NeedSealedClassMocks();
        }

        /// <summary>
        ///A test for GetDatabase
        ///</summary>
        [TestMethod()]
        [DeploymentItem("MongoSsisDataSource.dll")]
        public void GetDatabaseTest()
        {
            NeedSealedClassMocks();
        }

        /// <summary>
        ///A test for GetEditStyle
        ///</summary>
        [TestMethod()]
        public void GetEditStyleTest()
        {
            PrivateObject p = new PrivateObject(typeof(CollectionNameEditor));            
            Assert.AreEqual(UITypeEditorEditStyle.DropDown, (UITypeEditorEditStyle)p.Invoke("GetEditStyle", new object[] { null }));
        }

        /// <summary>
        ///A test for GetMongoDBConnectionManager
        ///</summary>
        [TestMethod()]
        [DeploymentItem("MongoSsisDataSource.dll")]
        public void GetMongoDBConnectionManagerFromContextTest()
        {
            NeedSealedClassMocks();
        }

        /// <summary>
        ///A test for GetMongoDBConnectionManager
        ///</summary>
        [TestMethod()]
        [DeploymentItem("MongoSsisDataSource.dll")]
        public void GetMongoDBConnectionManagerFromPackageTest()
        {
            NeedSealedClassMocks();
        }

        /// <summary>
        ///A test for GetPackageFromContext
        ///</summary>
        [TestMethod()]
        [DeploymentItem("MongoSsisDataSource.dll")]
        public void GetPackageFromContextTest()
        {
            NeedSealedClassMocks();
        }

        /// <summary>
        ///A test for OnListBoxSelectedValueChanged
        ///</summary>
        [TestMethod()]
        [DeploymentItem("MongoSsisDataSource.dll")]
        public void OnListBoxSelectedValueChangedTest()
        {
            PrivateObject p = new PrivateObject(typeof(CollectionNameEditor));
            IWindowsFormsEditorService serviceMock = Mock.Create<IWindowsFormsEditorService>();
            p.SetField("edSvc", serviceMock);
            Mock.Arrange(() => serviceMock.CloseDropDown()).OccursOnce();
            p.Invoke("OnListBoxSelectedValueChanged", new object[] { null, null });
            Mock.Assert(serviceMock);
        }

        /// <summary>
        ///A test for BuildListBox
        ///</summary>
        [TestMethod()]
        [DeploymentItem("MongoSsisDataSource.dll")]
        public void BuildListBoxTest()
        {
            string[] collectionNames = new string[] { "collection1", "collection2" };

            ListBox lb = SetUpListBoxForTest(collectionNames);

            Assert.AreEqual(SelectionMode.One, lb.SelectionMode);

            Assert.AreEqual(2, lb.Items.Count);
            foreach (string name in collectionNames)
            {
                Assert.IsTrue(lb.Items.Contains(name));
            }
        }

        /// <summary>
        ///A test for BuildListBox
        ///</summary>
        [TestMethod()]
        [DeploymentItem("MongoSsisDataSource.dll")]
        public void BuildListBoxIgnoresSystemCollectionsTest()
        {

            string[] collectionNames = new string[] { "collection1", "collection2", "system_collection" };
            ListBox lb = SetUpListBoxForTest(collectionNames);

            Assert.AreEqual(2, lb.Items.Count);

            foreach (string name in collectionNames)
            {
                if (name.StartsWith("system"))
                {
                    Assert.IsFalse(lb.Items.Contains(name));
                }
            }
        }

        private ListBox SetUpListBoxForTest(string[] names)
        {
            PrivateObject p = new PrivateObject(typeof(CollectionNameEditor));
            MongoDatabase database = Mock.Create<MongoDatabase>(Constructor.Mocked);
            Mock.Arrange(() => database.GetCollectionNames()).Returns(names);
            return (ListBox)p.Invoke("BuildListBox", new object[] { database });
        }

        private void NeedSealedClassMocks()
        {
            Assert.Inconclusive("Cannot test without paid-for mocking framework that allows mocking sealed classes.");
        }
    }
}