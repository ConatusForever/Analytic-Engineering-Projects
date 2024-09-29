IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name = 'SynapseParquetFormat') 
	CREATE EXTERNAL FILE FORMAT [SynapseParquetFormat] 
	WITH ( FORMAT_TYPE = PARQUET)
GO

IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name = 'files_oucapstone_dfs_core_windows_net') 
	CREATE EXTERNAL DATA SOURCE [files_oucapstone_dfs_core_windows_net] 
	WITH (
		LOCATION = 'abfss://files@oucapstone.dfs.core.windows.net' 
	)
GO

CREATE EXTERNAL TABLE dbo.serviceRequests (
	[Unique Key] bigint,
	[Created Date] nvarchar(4000),
	[Closed Date] nvarchar(4000),
	[Agency] nvarchar(4000),
	[Agency Name] nvarchar(4000),
	[Complaint Type] nvarchar(4000),
	[Descriptor] nvarchar(4000),
	[Location Type] nvarchar(4000),
	[Incident Zip] nvarchar(4000),
	[Incident Address] nvarchar(4000),
	[Street Name] nvarchar(4000),
	[Cross Street 1] nvarchar(4000),
	[Cross Street 2] nvarchar(4000),
	[Intersection Street 1] nvarchar(4000),
	[Intersection Street 2] nvarchar(4000),
	[Address Type] nvarchar(4000),
	[City] nvarchar(4000),
	[Landmark] nvarchar(4000),
	[Facility Type] nvarchar(4000),
	[Status] nvarchar(4000),
	[Due Date] nvarchar(4000),
	[Resolution Description] nvarchar(4000),
	[Resolution Action Updated Date] nvarchar(4000),
	[Community Board] nvarchar(4000),
	[BBL] float,
	[Borough] nvarchar(4000),
	[X Coordinate (State Plane)] float,
	[Y Coordinate (State Plane)] float,
	[Open Data Channel Type] nvarchar(4000),
	[Park Facility Name] nvarchar(4000),
	[Park Borough] nvarchar(4000),
	[Vehicle Type] nvarchar(4000),
	[Taxi Company Borough] nvarchar(4000),
	[Taxi Pick Up Location] nvarchar(4000),
	[Bridge Highway Name] nvarchar(4000),
	[Bridge Highway Direction] nvarchar(4000),
	[Road Ramp] nvarchar(4000),
	[Bridge Highway Segment] nvarchar(4000),
	[Latitude] float,
	[Longitude] float,
	[Location] nvarchar(4000)
	)
	WITH (
	LOCATION = 'PragmaticCoders/**',
	DATA_SOURCE = [files_oucapstone_dfs_core_windows_net],
	FILE_FORMAT = [SynapseParquetFormat]
	)
GO


SELECT TOP 100 * FROM dbo.serviceRequests
GO
