using MongoDataSource;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using MongoDB.Driver;
using System.Drawing.Design;
using Telerik.JustMock;
using System.Windows.Forms.Design;
using System.Windows.Forms;

/// <summary>
///This is a test class for CollectionNameEditorTest and is intended
///to contain all CollectionNameEditorTest Unit Tests
///</summary>
[TestClass()]
public class CollectionNameEditorTest {

    /// <summary>
    ///A test for EditValue
    ///</summary>
    [TestMethod()]
    public void EditValueTest() {
        NeedSealedClassMocks();
    }

    /// <summary>
    ///A test for GetDatabase
    ///</summary>
    [TestMethod()]
    [DeploymentItem("MongoSsisDataSource.dll")]
    public void GetDatabaseTest() {
        NeedSealedClassMocks();
    }

    /// <summary>
    ///A test for GetEditStyle
    ///</summary>
    [TestMethod()]
    public void GetEditStyleTest() {
        CollectionNameEditor_Accessor target = new CollectionNameEditor_Accessor(); 
        Assert.AreEqual(UITypeEditorEditStyle.DropDown, target.GetEditStyle(null));
    }

    /// <summary>
    ///A test for GetMongoDBConnectionManager
    ///</summary>
    [TestMethod()]
    [DeploymentItem("MongoSsisDataSource.dll")]
    public void GetMongoDBConnectionManagerFromContextTest() {
        NeedSealedClassMocks();
    }

    /// <summary>
    ///A test for GetMongoDBConnectionManager
    ///</summary>
    [TestMethod()]
    [DeploymentItem("MongoSsisDataSource.dll")]
    public void GetMongoDBConnectionManagerFromPackageTest() {
        NeedSealedClassMocks();
    }

    /// <summary>
    ///A test for GetPackageFromContext
    ///</summary>
    [TestMethod()]
    [DeploymentItem("MongoSsisDataSource.dll")]
    public void GetPackageFromContextTest() {
        NeedSealedClassMocks();
    }

    /// <summary>
    ///A test for OnListBoxSelectedValueChanged
    ///</summary>
    [TestMethod()]
    [DeploymentItem("MongoSsisDataSource.dll")]
    public void OnListBoxSelectedValueChangedTest() {
        CollectionNameEditor_Accessor target = new CollectionNameEditor_Accessor();
        IWindowsFormsEditorService serviceMock = Mock.Create<IWindowsFormsEditorService>();
        target.edSvc = serviceMock;
        Mock.Arrange(() => serviceMock.CloseDropDown()).OccursOnce();
        target.OnListBoxSelectedValueChanged(null, null);
        Mock.Assert(serviceMock);
    }

    /// <summary>
    ///A test for BuildListBox
    ///</summary>
    [TestMethod()]
    [DeploymentItem("MongoSsisDataSource.dll")]
    public void BuildListBoxTest() {
        string[] collectionNames = new string[]{"collection1","collection2"};

        ListBox lb = SetUpListBoxForTest(collectionNames);

        Assert.AreEqual(SelectionMode.One, lb.SelectionMode);

        Assert.AreEqual(2, lb.Items.Count);
        foreach (string name in collectionNames) {
            Assert.IsTrue(lb.Items.Contains(name));
        }
    }

    /// <summary>
    ///A test for BuildListBox
    ///</summary>
    [TestMethod()]
    [DeploymentItem("MongoSsisDataSource.dll")]
    public void BuildListBoxIgnoresSystemCollectionsTest() {
       
        string[] collectionNames = new string[] { "collection1", "collection2", "system_collection" };
        ListBox lb = SetUpListBoxForTest(collectionNames);
        
        Assert.AreEqual(2, lb.Items.Count);

        foreach (string name in collectionNames) {
            if (name.StartsWith("system")) {
                Assert.IsFalse(lb.Items.Contains(name));
            } 
        }
    }

    private ListBox SetUpListBoxForTest(string[] names) {
        CollectionNameEditor_Accessor target = new CollectionNameEditor_Accessor();
        MongoDatabase database = Mock.Create<MongoDatabase>(Constructor.Mocked);

        Mock.Arrange(() => database.GetCollectionNames()).Returns(names);

       return target.BuildListBox(database);
    }

    private void NeedSealedClassMocks() {
        Assert.Inconclusive("Cannot test without paid-for mocking framework that allows mocking sealed classes.");
    }
}

