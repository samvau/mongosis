# Generating the mongosis.msi Installer

Run through the following steps to recreate the **mongosis.msi** installer:

1. Install the WiX toolset from (http://wix.sourceforge.net/) (Note that this installs handy Visual Studio plugins)
2. Open the project in Visual Studio
3. Make sure the correct _MongoSsisDataSource.dll_ file is in the 'Installer' folder
3. Under 'Installer' in the 'Solution Explorer', right click 'References' and select 'Add References...'
4. Select and add the 'WixUIExtension' Reference
5. Ensure the 'ProductVersion' in the 'Product.wxs' file matches the version defined in the 'AssemblyInfo'
6. Right click the 'Installer' in the 'Solution Explorer' and select 'Build' in 'Release' mode
7. Find the **mongosis.msi** file under the _bin/Release_ folder.