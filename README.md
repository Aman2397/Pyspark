# Pyspark
It consist of pyspark code

Data Profiling Code=

Purpose:
We are performing data profiling on raw files to check data integrity and quality, before client provides whole data. With the help of data profiling utility, we can eliminate costly errors that usually occur during transformation and Reconciliation. The insights which utility is able to give us, we would be able to improve data consistency and would be able to understand trend much better.

FlowChart=
                                             |------> Data Profiling -----> Data_ProfilingTable && FrequencyDistribution_percent
                                             |
Data Source ----> Data Profiling utility ----|
                                             |
                                             |------>Raw_layer Hive script

Data_ProfilingTable=
Following is basic analysis done using data profiling table,
RecordCount : Total number of records.
NullCount : Number of missing value in the column.
DistinctValueCount: Count of distinct value in the column.
MinValue: Minimum value for the column.
MaxValue: Maximum value for the column. 
MinValueLength: length of minimum value.
MaxValueLength: length of maximum value.
MinDate: Minimum date value of the date column.
MaxDate: Maximum date value of the date column.

FrequencyDistribution_percent= 
Percentage of distinct to total count.
Following is basic analysis done using frequency distribution table,
AttributeValue: Distinct value present in column.
Frequency : How many times the attribute is occurred.
Frequency_Percent : Percentage of frequency to total count.

Implementation: One-time Activity.
Hive:
Create Database=
create database DataProfiling;
create database Raw_Layer;

Create Script for tables in DataProfiling database=
CREATE EXTERNAL TABLE DataProfiling.Data_ProfilingTable (
OrganizationName string,
EntityName string,
FileName string,
AttributeName string,
RecordCount integer,
NullCount integer,
DistinctValueCount integer,
MinValue BIGINT,
MaxValue BIGINT,
MinValueLength integer,
MaxValuelength integer,
MinDate string,
MaxDate string,
FrequencyDistribution_percent decimal(4,2),
RunDate timestamp)
STORED AS PARQUET LOCATION '<Path of data analysis output>';



CREATE EXTERNAL TABLE DataProfiling.Data_FrequencyDistributionTable (
OrganizationName string,
EntityName string,
FileName string,
ColumnName string,
AttributeValue string,
Frequency string,
Frequency_Percent integer,
RunDate timestamp)
STORED AS PARQUET LOCATION '<Path of FrequencyDistributionTable >';

