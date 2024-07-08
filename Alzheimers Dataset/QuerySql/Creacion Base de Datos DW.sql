USE MASTER;

IF NOT EXISTS (SELECT name FROM master.dbo.sysdatabases WHERE name = N'AlzheimerDW')
BEGIN
CREATE DATABASE AlzheimerDW;
END 
GO

IF EXISTS (SELECT name FROM master.dbo.sysdatabases WHERE name = N'AlzheimerDW')
BEGIN

USE AlzheimerDW;
DROP TABLE IF EXISTS dbo.DimTopic ;
DROP TABLE IF EXISTS dbo.DimLocation 
DROP TABLE IF EXISTS dbo.DimQuestion ;
DROP TABLE IF EXISTS dbo.DimDataValueType ;
DROP TABLE IF EXISTS dbo.DimAgeGroup ;
DROP TABLE IF EXISTS dbo.DimStratification2 ;
DROP TABLE IF EXISTS dbo.FactAlzheimersDisease ;

CREATE TABLE dbo.DimTopic(
    TopicId NVARCHAR(50) PRIMARY KEY,
    Topic NVARCHAR(150),
    Class NVARCHAR(150)
);

CREATE TABLE dbo.DimLocation(
    LocationId SMALLINT PRIMARY KEY,
    LocationAbbr NVARCHAR(50),
    LocationDesc NVARCHAR(50),
    Geolocation NVARCHAR(50),
	Latitude NVARCHAR(50) NULL,
	Longitude NVARCHAR(50) NULL
);

CREATE TABLE dbo.DimQuestion (
    QuestionId SMALLINT PRIMARY KEY,
    Question NVARCHAR(200)
);

CREATE TABLE dbo.DimDataValueType(
    DataValueTypeId NVARCHAR(50) PRIMARY KEY,
	Data_Value_Unit NVARCHAR(50),
    Data_Value_Type VARCHAR(50)
);

CREATE TABLE dbo.DimAgeGroup(
    AgeGroup NVARCHAR(50) PRIMARY KEY
);

CREATE TABLE dbo.DimStratification2 (
    StratificationId2 NVARCHAR(50) PRIMARY KEY,
    StratificationCategory2 VARCHAR(50),
    Stratification2 VARCHAR(50)
);

CREATE TABLE FactAlzheimersDisease (

    RowId NVARCHAR(100) PRIMARY KEY ,
    YearStart SMALLINT,
    YearEnd SMALLINT,
    Data_Value FLOAT,
    Low_Confidence_Limit FLOAT,
    High_Confidence_Limit FLOAT,
    LocationId SMALLINT,
    TopicId NVARCHAR(50),
    QuestionId SMALLINT,
    DataValueTypeId NVARCHAR(50),
    AgeGroup NVARCHAR(50),
    StratificationId2 NVARCHAR(50),
    FOREIGN KEY (LocationId) REFERENCES DimLocation(LocationId),
    FOREIGN KEY (TopicId) REFERENCES DimTopic(TopicId),
    FOREIGN KEY (QuestionId) REFERENCES DimQuestion(QuestionId),
    FOREIGN KEY (DataValueTypeId) REFERENCES DimDataValueType(DataValueTypeId),
    FOREIGN KEY (AgeGroup) REFERENCES DimAgeGroup(AgeGroup),
    FOREIGN KEY (StratificationId2) REFERENCES DimStratification2(StratificationId2)
);



END
GO