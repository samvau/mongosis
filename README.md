# Custom SSIS Data Source and Connection Manager for Mongo DB

This is the Custom Adapter for Microsoft's SSIS to connect to MongoDB

## Dependencies

This data source depends on the C# MongoDB drivers found at:

https://github.com/mongodb/mongo-csharp-driver/downloads

The unit tests depend on the .NET mocking framework 'JustMock Lite' available at:

http://www.telerik.com/freemocking.aspx

## Deployment

	* Copy DLL file to:
	** C:\Program Files (x86)\Microsoft SQL Server\110\DTS\PiplineComponents
	** C:\Program Files (x86)\Microsoft SQL Server\110\DTS\Connections
	* Run gacutil.exe with '/iF' option to load DLL in to the GLobal Assembly Cache, util is found at:
	** C:\Program Files (x86)\Microsoft SDKs\Windows\v7.0A\bin\NETFX 4.0 Tools\gacutil.exe

## Usage

* In SQL Server Data Tools, create a 'Business Intelligence > Integration Services' project.
* Create a MongoDB Connection Manager in the Connection Managers area.
* Set the database, server, username and password values in the Component Properties
* Drag a MongoDB Data Source in to the Data Flow area
* Right click the MongoDB data source and click Edit
* Under Connection Managers, ensure that MongoDBConnectionManager is selected.
* In the Component Properties tab, enter the name of the Mongo collection you wish to pull data from in the 'CollectionName' custom property.
* Under Input and Output properties, verify that all the expected fields are listed under Output - Output Columns
* Clicking onto each Output Column, ensure that the DataType field is correct
* Drag your data destination to the Data Flow area
* Click on the MongoDB Data Source and create a connection to the destination.
* Select 'Start Debugging' (key F5) to begin the data flow process.

## Notes:

	* Ensure that 'Run64BitRuntime' option in SSIS project Configuration/Debugging properties is set to 'False'.
	* If you're running the 32-bit version of Windows there is no 'Program Files(x86)' so deploy to folders under 'Program Files' instead.