USE MASTER;

IF NOT EXISTS (SELECT name FROM master.dbo.sysdatabases WHERE name = N'AlzheimerStaging')
BEGIN
CREATE DATABASE AlzheimerStaging;
END 
GO

IF EXISTS (SELECT name FROM master.dbo.sysdatabases WHERE name = N'AlzheimerStaging')
BEGIN

USE AlzheimerStaging;
DROP TABLE IF EXISTS dbo.AlzheimersDiseaseTransformada;

CREATE TABLE [dbo].[AlzheimersDiseaseTransformada](
	[RowId] [nvarchar](100)  NULL,
	[YearStart] [smallint]  NULL,
	[YearEnd] [smallint]  NULL,
	[LocationAbbr] [nvarchar](50)  NULL,
	[LocationDesc] [nvarchar](50)  NULL,
	[Geolocation] [nvarchar](50) NULL,
	[Latitude] [nvarchar](50) NULL,
	[Longitude] [nvarchar](50) NULL,
	[Class] [nvarchar](50)  NULL,
	[Topic] [nvarchar](150)  NULL,
	[Question] [nvarchar](200)  NULL,
	[Data_Value] [float] NULL,
	[Low_Confidence_Limit] [float] NULL,
	[High_Confidence_Limit] [float] NULL,
	[Data_Value_Unit] [nvarchar](50)  NULL,
	[Data_Value_Type] [nvarchar](50)  NULL,
	[AgeGroup] [nvarchar](50)  NULL,
	[StratificationCategory2] [nvarchar](50) NULL,
	[Stratification2] [nvarchar](50) NULL,
	[DataValueTypeID] [nvarchar](50)  NULL,
	[ClassID] [nvarchar](50) NULL,
	[TopicID] [nvarchar](50)  NULL,
	[QuestionID] [money] NULL,
	[LocationID] [smallint]  NULL,
	[StratificationCategoryID2] [nvarchar](50)  NULL,
	[StratificationID2] [nvarchar](50)  NULL
)
END
GO