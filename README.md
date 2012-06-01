# Custom SSIS Data Source and Connection Manager for Mongo DB

This is the Custom Adapter for Microsoft's SSIS to connect to MongoDB

## Dependencies

This data source depends on the C# MongoDB drivers found at:

https://github.com/mongodb/mongo-csharp-driver/downloads

The unit tests depend on the .NET mocking framework 'JustMock Lite' available at:

http://www.telerik.com/freemocking.aspx

## Deployment

* Copy DLL file to:
	* C:\Program Files (x86)\Microsoft SQL Server\110\DTS\PiplineComponents
	* C:\Program Files (x86)\Microsoft SQL Server\110\DTS\Connections
* Run gacutil.exe with '/iF' option to load DLL in to the GLobal Assembly Cache, util is found at:
	* C:\Program Files (x86)\Microsoft SDKs\Windows\v7.0A\bin\NETFX 4.0 Tools\gacutil.exe

## Usage

1. In SQL Server Data Tools, create a 'Business Intelligence > Integration Services' project.
2. Create a MongoDB Connection Manager in the Connection Managers area.
3. Set the database, server, username and password values in the Component Properties
4. Drag a MongoDB Data Source in to the Data Flow area
5. Right click the MongoDB data source and click Edit
6. Under Connection Managers, ensure that MongoDBConnectionManager is selected.
7. In the Component Properties tab, enter the name of the Mongo collection you wish to pull data from in the 'CollectionName' custom property.
8. Under Input and Output properties, verify that all the expected fields are listed under Output - Output Columns
9. Clicking onto each Output Column, ensure that the DataType field is correct
10. Drag your data destination to the Data Flow area
11. Click on the MongoDB Data Source and create a connection to the destination.
12. Select 'Start Debugging' (key F5) to begin the data flow process.

## Notes:

* Ensure that 'Run64BitRuntime' option in SSIS project Configuration/Debugging properties is set to 'False'.
* If you're running the 32-bit version of Windows there is no 'Program Files(x86)' so deploy to folders under 'Program Files' instead.