/*
 * Copyright (c) 2012-2013 Xbridge Ltd
 * See the file license.txt for copying permission.
 */

using System;
using System.Linq;
using System.Xml.Linq;
using Microsoft.SqlServer.Dts.Runtime;
using MongoDB.Driver;

namespace MongoDataSource
{
    [DtsConnection(ConnectionType = "MongoDB", DisplayName = "MongoDBConnectionManager", Description = "Connection manager for MongoDB")]
    public class MongoConnectionManager : ConnectionManagerBase
    {

        #region private variables
        // Default values.
        private string _serverName = "localhost";
        private string _databaseName = string.Empty;
        private string _userName = string.Empty;
        private string _password = string.Empty;
        private bool _ssl = false;
        private string _connectionString = string.Empty;
        #endregion

        #region constants
        private const string CONNECTIONSTRING_TEMPLATE = "mongodb://<username>:<password>@<servername>";
        #endregion

        #region public properties
        /// <summary>
        /// The host name or IP address of the database server
        /// </summary>
        public string ServerName
        {
            get { return _serverName; }
            set { _serverName = value; }
        }

        /// <summary>
        /// The name of the mongo db database
        /// </summary>
        public string DatabaseName
        {
            get { return _databaseName; }
            set { _databaseName = value; }
        }

        /// <summary>
        /// The database user
        /// </summary>
        public string UserName
        {
            get { return _userName; }
            set { _userName = value; }
        }

        /// <summary>
        /// The password for the database user
        /// </summary>
        public string Password
        {
            get { return _password; }
            set { _password = value; }
        }

        /// <summary>
        /// If true, this will allow the component to connect to a database node other than the primary node
        /// </summary>
        public bool SlaveOk { get; set; }

        /// <summary>
        /// If true, this will force the connection to use SSL
        /// The database must be running in SSL mode in order for this to work
        /// </summary>
        public bool Ssl
        {
            get { return _ssl; }
            set { _ssl = value; }
        }
        #endregion

        #region ConnectionManagerBase members
        /// <summary>
        /// Returns the version of this connection manager.
        /// </summary>
        public override int Version { get { return 170; } }

        /// <summary>
        /// Gets or sets a Boolean that determines whether a connection manager supports
        ///     upgrading the connection XML to a newer version.
        /// </summary>
        /// <param name="CreationName">The name of the connection to upgrade.</param>
        /// <returns>true if support exists for upgrades; otherwise, false. The default value is false</returns>
        public override bool CanUpdate(string CreationName) { return true; }

        /// <summary>
        /// Updates the XML persisted by a previous version of the connection manager.
        /// </summary>
        /// <param name="ObjectXml">The XML used to update the connection manager XML.</param>
        public override void Update(ref string ObjectXml)
        {
            var xDocument = XDocument.Parse(ObjectXml);
            // Check if the Ssl property already exists, if not, we must create it
            var ssl = xDocument.Root.Descendants("Ssl").SingleOrDefault();
            if (ssl == null)
            {
                // We expect the SlaveOk property to exist, so we can model the Ssl property after it
                var slaveOk = xDocument.Root.Descendants("SlaveOk").SingleOrDefault();
                if (slaveOk != null)
                {
                    ssl = XElement.Parse(slaveOk.ToString());
                    ssl.Name = "Ssl";
                    ssl.SetAttributeValue("Value", 0); // Default Ssl to false
                    slaveOk.AddAfterSelf(ssl);
                    ObjectXml = xDocument.ToString();
                }
            }
        }

        /// <summary>
        /// Gets or sets the connection string for the connection.
        /// </summary>
        public override string ConnectionString
        {
            get
            {
                UpdateConnectionString();
                return _connectionString;
            }
            set { _connectionString = value; }
        }
        /// <summary>
        /// Validates the connection and returns an enumeration that indicates success or failure.
        /// </summary>
        /// <param name="infoEvents"> An object that implements the Microsoft.SqlServer.Dts.Runtime.IDTSInfoEvents events interface to raise errors, warning, or informational events.</param>
        /// <returns>A Microsoft.SqlServer.Dts.Runtime.DTSExecResult enumeration.</returns>
        public override Microsoft.SqlServer.Dts.Runtime.DTSExecResult Validate(Microsoft.SqlServer.Dts.Runtime.IDTSInfoEvents infoEvents)
        {
            if (string.IsNullOrWhiteSpace(_serverName))
                return HandleValidationError(infoEvents, "No server name specified");
            else if (string.IsNullOrWhiteSpace(_databaseName))
                return HandleValidationError(infoEvents, "No database name specified");
            else if (string.IsNullOrWhiteSpace(_password))
                return HandleValidationError(infoEvents, "No password specified");
            else if (string.IsNullOrWhiteSpace(_userName))
                return HandleValidationError(infoEvents, "No username specified");
            else
                return DTSExecResult.Success;
        }

        /// <summary>
        /// Creates an instance of the connection type.
        /// </summary>
        /// <param name="txn">The handle to a transaction object. (optional)</param>
        /// <returns></returns>
        public override object AcquireConnection(object txn)
        {
            MongoDatabase database = null;
            if (string.IsNullOrWhiteSpace(_connectionString))
                UpdateConnectionString();

            if (!string.IsNullOrWhiteSpace(_connectionString))
            {
                MongoServer mongoinstance = new MongoClient(_connectionString).GetServer();

                if (string.IsNullOrWhiteSpace(DatabaseName))
                    throw new Exception("No database specified");

                database = mongoinstance.GetDatabase(DatabaseName);
            }
            else
            {
                throw new Exception("Can not connect to MongoDB with empty connection string");
            }

            return database;
        }

        /// <summary>
        /// Frees the connection established during Microsoft.SqlServer.Dts.Runtime.ConnectionManagerBase.AcquireConnection(System.Object).
        ///     Called at design time and run time.
        /// </summary>
        /// <param name="connection">The connection to release.</param>
        public override void ReleaseConnection(object connection)
        {
            if (connection != null)
            {
                MongoDatabase database = (MongoDatabase)connection;
                database.Server.Disconnect();
            }
        }
        #endregion

        #region private methods
        private void UpdateConnectionString()
        {
            string temporaryString = CONNECTIONSTRING_TEMPLATE;

            if (!string.IsNullOrWhiteSpace(_serverName))
                temporaryString = temporaryString.Replace("<servername>", _serverName);

            if (!string.IsNullOrWhiteSpace(_userName))
                temporaryString = temporaryString.Replace("<username>", _userName);

            if (!string.IsNullOrWhiteSpace(_password))
                temporaryString = temporaryString.Replace("<password>", _password);

            if (SlaveOk || Ssl)
            {
                temporaryString += "/?";

                if (SlaveOk)
                {
                    temporaryString += "connect=direct;slaveok=true";

                    if(Ssl)
                        temporaryString += ";";
                }

                if (Ssl)
                    temporaryString += "ssl=true;sslverifycertificate=false";
            }

            _connectionString = temporaryString;
        }

        private Microsoft.SqlServer.Dts.Runtime.DTSExecResult HandleValidationError(Microsoft.SqlServer.Dts.Runtime.IDTSInfoEvents infoEvents, string message)
        {
            if (infoEvents != null)
                infoEvents.FireError(0, "MongoConnectionManager", message, string.Empty, 0);

            return DTSExecResult.Failure;
        }
        #endregion
    }
}
