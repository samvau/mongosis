/*
 * Copyright (c) 2012 Xbridge Ltd
 * See the file license.txt for copying permission.
 */

using Microsoft.SqlServer.Dts.Runtime;
using MongoDB.Driver;

namespace MongoDataSource {

    [DtsConnection(ConnectionType = "MongoDB", DisplayName = "MongoDBConnectionManager", Description = "Connection manager for MongoDB")]
    public class MongoConnectionManager : ConnectionManagerBase {

        // Default values.
        private string _serverName = "localhost";
        private string _databaseName = string.Empty;
        private string _userName = string.Empty;
        private string _password = string.Empty;

        private string _connectionString = string.Empty;

        private const string CONNECTIONSTRING_TEMPLATE = "mongodb://<username>:<password>@<servername>";
        public string ServerName {
            get { return _serverName; }
            set { _serverName = value; }
        }

        public string DatabaseName {
            get { return _databaseName; }
            set { _databaseName = value; }
        }

        public string UserName {
            get { return _userName; }
            set { _userName = value; }
        }

        public string Password {
            get { return _password; }
            set { _password = value; }
        }

        public override string ConnectionString {
            get {
                UpdateConnectionString();
                return _connectionString;
            }
            set { _connectionString = value; }
        }


        private void UpdateConnectionString() {
            string temporaryString = CONNECTIONSTRING_TEMPLATE;

            if (!string.IsNullOrEmpty(_serverName)) {
                temporaryString = temporaryString.Replace("<servername>", _serverName);
            }
            if (!string.IsNullOrEmpty(_userName)) {
                temporaryString = temporaryString.Replace("<username>", _userName);
            }
            if (!string.IsNullOrEmpty(_password)) {
                temporaryString = temporaryString.Replace("<password>", _password);
            }

            _connectionString = temporaryString;

        }

        public override Microsoft.SqlServer.Dts.Runtime.DTSExecResult Validate(Microsoft.SqlServer.Dts.Runtime.IDTSInfoEvents infoEvents) {

            if (string.IsNullOrEmpty(_serverName)) {
                if (infoEvents != null) {
                    infoEvents.FireError(0, "MongoConnectionManager", "No server name specified", string.Empty, 0);
                }
                return DTSExecResult.Failure;
            } else {
                return DTSExecResult.Success;
            }

        }

        public override object AcquireConnection(object txn) {

            MongoServer mongoinstance = MongoServer.Create(_connectionString);
            MongoDatabase database = mongoinstance.GetDatabase(DatabaseName);

            return database;
        }


        public override void ReleaseConnection(object connection) {
            if (connection != null) {
                MongoDatabase database = (MongoDatabase)connection;

                database.Server.Disconnect();
            }
        }

    }

}