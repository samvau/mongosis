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

        public bool SlaveOk { get; set; }

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

            if (SlaveOk) {
                temporaryString = temporaryString + "/?connect=direct;slaveok=true";
            }

            _connectionString = temporaryString;

        }

        public override Microsoft.SqlServer.Dts.Runtime.DTSExecResult Validate(Microsoft.SqlServer.Dts.Runtime.IDTSInfoEvents infoEvents) {

            if (string.IsNullOrEmpty(_serverName)) {
                return HandleValidationError(infoEvents, "No server name specified");
            } else if(string.IsNullOrEmpty(_databaseName)) {
                return HandleValidationError(infoEvents, "No database name specified");
            } else if (string.IsNullOrEmpty(_password)) {
                return HandleValidationError(infoEvents, "No password specified");
            } else if (string.IsNullOrEmpty(_userName)) {
                return HandleValidationError(infoEvents, "No username specified");
            } else {
                return DTSExecResult.Success;
            }

        }

        private Microsoft.SqlServer.Dts.Runtime.DTSExecResult HandleValidationError(Microsoft.SqlServer.Dts.Runtime.IDTSInfoEvents infoEvents, string message) {
            if (infoEvents != null) {
                infoEvents.FireError(0, "MongoConnectionManager", message, string.Empty, 0);
            }
            return DTSExecResult.Failure;
        }

        public override object AcquireConnection(object txn) {
            MongoDatabase database = null;
            if (string.IsNullOrEmpty(_connectionString)) {
                UpdateConnectionString();
            }

            if (!string.IsNullOrEmpty(_connectionString)) {
                MongoServer mongoinstance = MongoServer.Create(_connectionString);

                if (string.IsNullOrEmpty(DatabaseName)) {
                    throw new System.Exception("No database specified");
                }

                database = mongoinstance.GetDatabase(DatabaseName);
            } else {
                throw new System.Exception("Can not connect to MongoDB with empty connection string");
            }

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
