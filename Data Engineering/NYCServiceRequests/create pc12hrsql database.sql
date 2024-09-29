CREATE DATABASE pc12hrsqlchallenge
    COLLATE Latin1_General_100_BIN2_UTF8;
GO;

USE pc12hrsqlchallenge;
GO;

CREATE EXTERNAL DATA SOURCE pc21hrchllg with(
    LOCATION = 'https://oucapstone.dfs.core.windows.net/files/PragmaticCoders/serviceRequestsP/'
);