from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from decimal import *
import re
import sys
import os
import numpy as np
import pandas as pd
from datetime import datetime
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, LongType, TimestampType, DecimalType

spark = SparkSession.builder.appName("DataProfiling").getOrCreate()
spark.sparkContext.setLogLevel('WARN')
RunDateValue=datetime.now()
# False For Not NULL value
AnalyzeDataSchema = StructType([ 
    StructField("OrganizationName",StringType(),False), 
    StructField("EntityName",StringType(),False), 
    StructField("FileName",StringType(),False), 
    StructField("AttributeName", StringType(), False), 
    StructField("RecordCount", IntegerType(), False),
    StructField("NullCount", IntegerType(), False),
    StructField("DistinctValueCount", IntegerType(), False),    
    StructField("MinValue", LongType(), True), 
    StructField("MaxValue", LongType(), True),
    StructField("MinValueLength", IntegerType(), True),
    StructField("MaxValuelength", IntegerType(), True),    
    StructField("MinDate", StringType(), True),
    StructField("MaxDate", StringType(), True),
    StructField("FrequencyDistribution_percent", DecimalType(4,2), True),
    StructField("RunDate", TimestampType(), False)
  ])  
     
FreqDistAnalyzeDataSchema = StructType([
    StructField("OrganizationName",StringType(),False), 
    StructField("EntityName",StringType(),False), 
    StructField("FileName",StringType(),False),
    StructField("ColumnName",StringType(),False),
    StructField("AttributeValue",StringType(),False),
	  StructField("Frequency",StringType(),False),
    StructField("Frequency_Percent",DecimalType(),False),
    StructField("RunDate", TimestampType(), False)
])  


def readFile(filename,orgname,entityname,FreqDistCriteria,absInterimPath,rawfilePath,nodeURLStmt,sqlScriptPath,analyzeDataOutputParq,FrequenceDistributionCalculationParq):
    try:
        print("/****************Trying to read File********************************/")
        interimPath=absInterimPath+"/*"
        hivePath=rawfilePath
        if not os.path.exists(sqlScriptPath):os.mkdir(sqlScriptPath)
        #print(interimPath)
        InterimData=spark.read.parquet(interimPath)
        InterimData=InterimData.filter(InterimData.Filename==filename)
        CreateInterimHive(InterimData,hivePath,orgname,entityname,filename,FreqDistCriteria,rawfilePath,nodeURLStmt,sqlScriptPath,analyzeDataOutputParq,FrequenceDistributionCalculationParq)
    
    except Exception as e: 
        print("/********************ERROR while reading file****************************/")
        print(e)
            


def CreateInterimHive(InterimData,hivePath,orgname,entityname,filename,FreqDistCriteria,rawfilePath,nodeURLStmt,sqlScriptPath,analyzeDataOutputParq,FrequenceDistributionCalculationParq):
  try: 
      print("/****************Creating hive script ********************************/")
      CreateHiveStmt="CREATE EXTERNAL TABLE "
      TableNameStmt="Raw_Layer."+orgname+"_"+entityname
      dropHiveStmt="drop table "+TableNameStmt+"; "
      ColumnListStmt=""
      for colName in InterimData.columns:
        if colName !='SequenceId':
          if(str(colName).upper()=='FILELOADDATE'):ColumnListStmt+=colName+" timestamp,"
          else:ColumnListStmt+=colName+" string,"
      
      ColumnListStmt=ColumnListStmt[:-1] #Remoce last ,
      StoredAsStmt="ROW FORMAT SERDE \"org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe\" WITH SERDEPROPERTIES (\"timestamp.formats\"=\"yyyy-MM-dd'T'HH:mm:ss\") STORED AS PARQUET LOCATION "
      HiveCreateStatement=dropHiveStmt+CreateHiveStmt+TableNameStmt+" ("+ColumnListStmt+") "+StoredAsStmt+"\""+nodeURLStmt+hivePath+"\";"
      sqlScriptPathFull=sqlScriptPath+"Hive_"+orgname+"_"+entityname+".sh"
      
      writeIntoSQlScript(HiveCreateStatement,sqlScriptPathFull)
      analyzeData(InterimData,orgname,entityname,filename,sqlScriptPathFull,FreqDistCriteria,sqlScriptPath,analyzeDataOutputParq,FrequenceDistributionCalculationParq)      
      
  except Exception as e:
      print("/********************ERROR while Create for Raw_layer****************************/")
      print(e)




def analyzeData(InterimData,orgname,entityname,filename,sqlScriptPathFull,FreqDistCriteria,sqlScriptPath,analyzeDataOutputParq,FrequenceDistributionCalculationParq):
  try:
      print("/****************Creating analysis table********************************/")
      AnalyzeDataDF=spark.createDataFrame(data=spark.sparkContext.emptyRDD(),schema=AnalyzeDataSchema)#creating Empty dataframe having AnalyzeDataSchema schema
      OrganizationNameValue=orgname
      EntityNameValue=entityname
      FileNameValue=filename #Gets distinct FileName
      RecordCountValue=InterimData.select('Filename').count()
      
      excludeColsWhileAnalyze=["etl_batch_id","Filename","FileLoadDate","Payer_Code","SequenceId"]
      InterimData=InterimData.drop(*excludeColsWhileAnalyze)
      MemberIdRegex=re.compile(r'^[\w]*ID')
      print("Analysis columns :")
      for colName in InterimData.columns:
        print(colName)
        CountDist=InterimData.select(colName).distinct().count()
        if(MemberIdRegex.search(str(colName).upper())==None):newRow=InterimData.select(lit(OrganizationNameValue),lit(EntityNameValue),lit(FileNameValue),lit(colName),lit(RecordCountValue),count(when(col(colName).isNull(),colName)).cast("integer"),lit(CountDist).cast("Integer"),min(col(colName).cast("string")).cast("integer"),max(col(colName).cast("string")).cast("integer"),min(length(col(colName))).cast("integer"),max(length(col(colName))).cast("integer"),min(coalesce(to_date(col(colName),'M/d/yyyy'),to_date(col(colName),'yyyy/M/d'),to_date(col(colName),'M-d-yyyy'),to_date(col(colName),'yyyy-M-d'),to_date(col(colName),'M/d/yyyy'),to_date(col(colName),'yyyyMMdd'),to_date(col(colName),'ddMMMyyyy')).cast("string")),max(coalesce(to_date(col(colName),'M/d/yyyy'),to_date(col(colName),'yyyy/M/d'),to_date(col(colName),'M-d-yyyy'),to_date(col(colName),'yyyy-M-d'),to_date(col(colName),'M/d/yyyy'),to_date(col(colName),'yyyyMMdd'),to_date(col(colName),'ddMMMyyyy')).cast("string")),((lit(CountDist)/RecordCountValue)*100).cast("decimal(4,2)"),lit(RunDateValue))
        else:newRow=InterimData.select(lit(OrganizationNameValue),lit(EntityNameValue),lit(FileNameValue),lit(colName),lit(RecordCountValue),count(when(col(colName).isNull(),colName)).cast("integer"),lit(CountDist).cast("Integer"),min(col(colName).cast("string")).cast("integer"),max(col(colName).cast("string")).cast("integer"),min(length(col(colName))).cast("integer"),max(length(col(colName))).cast("integer"),lit(None).cast("string"),lit(None).cast("string"),((lit(CountDist)/RecordCountValue)*100).cast("decimal(4,2)"),lit(RunDateValue))
        AnalyzeDataDF=AnalyzeDataDF.union(newRow)
  
      interimHiveShellScripPath="sh "+sqlScriptPathFull
      os.system(interimHiveShellScripPath)            # shell script
      AnalyzeDataDF.persist()
      FrequenceDistributionCalculation(AnalyzeDataDF,InterimData,OrganizationNameValue,EntityNameValue,FileNameValue,RecordCountValue,FreqDistCriteria,analyzeDataOutputParq,FrequenceDistributionCalculationParq)


      
  except Exception as e:
      print("/********************Error while creating analysis table****************************/")
      print(e)




def FrequenceDistributionCalculation(AnalyzeDataDF,InterimData,OrganizationNameValue,EntityNameValue,FileNameValue,RecordCountValue,FreqDistCriteria,analyzeDataOutputParq,FrequenceDistributionCalculationParq):
  print("/********************Calculating Frequence Distribution****************************/")
  try:
    FreqDistrColumn=AnalyzeDataDF.filter(AnalyzeDataDF.FrequencyDistribution_percent<=FreqDistCriteria).filter(AnalyzeDataDF.FrequencyDistribution_percent>0)
    AttributeNameList=list(FreqDistrColumn.select('AttributeName').toPandas()['AttributeName'])
    print("Frequency distribution columns :",AttributeNameList)
    FreqDistributionData=InterimData.select(*AttributeNameList)
    FreqDistributionData=FreqDistributionData.fillna('NA')
    FreqRow=[]
    for colName in FreqDistributionData.columns:
      newRowFreq=FreqDistributionData.select(colName).rdd.map(lambda word:(word,1)).reduceByKey(lambda a,b:a +b).collect()
      FreqRow=FreqRow+newRowFreq
    
    rdd=spark.sparkContext.parallelize(FreqRow)
    collData=rdd.collect()
    colnNameList=[]
    valueList=[]
    CountList=[]
    df_list=[]
    i=0
    
    for row in collData:
      colnNameList.append(str(row).split('=',1)[0].split("(Row(")[1].replace("'","").strip())
      valueList.append(str(row).split('=',1)[1].split('),',1)[0].split("u",1)[1].replace("'","").strip())
      CountList.append(str(row).split('=',1)[1].split('),',1)[1].split(')',1)[0].replace("'","").strip())
      Frequence_PercentValue=((Decimal(CountList[i])/Decimal(RecordCountValue))*100)#.quantize(Decimal('.01'), rounding=ROUND_DOWN)
      if i <len(collData):
        data = [OrganizationNameValue,EntityNameValue,FileNameValue,colnNameList[i],valueList[i],CountList[i],Frequence_PercentValue,RunDateValue]
        data
        i=i+1
        df_list.append(data)
  
    
    FreqDistAnalyzeDataDF=spark.createDataFrame(df_list,schema=FreqDistAnalyzeDataSchema)
    FreqDistAnalyzeDataDF=FreqDistAnalyzeDataDF.replace('NA',None)
    writeIntoParquet(AnalyzeDataDF,FreqDistAnalyzeDataDF,analyzeDataOutputParq,FrequenceDistributionCalculationParq)
    

  except Exception as e:
    print("/********************ERROR in FrequenceDistributionCalculation/********************")
    print(e)

def writeIntoParquet(AnalyzeDataDF,FreqDistAnalyzeDataDF,analyzeDataOutputParq,FrequenceDistributionCalculationParq):
  try:
    print("/********************Writing output into parquet/********************")
    AnalyzeDataDF.coalesce(1).write.mode('append').parquet(analyzeDataOutputParq)
    FreqDistAnalyzeDataDF.coalesce(1).write.mode('append').parquet(FrequenceDistributionCalculationParq)
  except Exception as e:
    print("/********************ERROR in writeIntoParquet****************************/")
    print(e)  

def writeIntoSQlScript(statement,sqlScriptPathFull):
  try:
    file=open(sqlScriptPathFull,"w")
    file.writelines("hive -e '")
    file.writelines(statement)
    file.writelines("'")
    file.close()
  except Exception as e:
    print("/********************ERROR in writeIntoSQlScript****************************/")
    print(e)
      
def main():
    orgname = sys.argv[1] #orgnaization code
    entityname = sys.argv[2]  # Entity Name
    filename = sys.argv[3] # file name
    FreqDistCriteria= sys.argv[4] # freq disct criteria
    absInterimPath= sys.argv[5] # interim path
    rawfilePath= absInterimPath[:-10] #Raw file path
    nodeURLStmt= sys.argv[6] # Node URL
    sqlScriptPath= sys.argv[7] # hive script path
    analyzeDataOutputParq= sys.argv[8] # Data analysis output path
    FrequenceDistributionCalculationParq= sys.argv[9] # freq distribution output path
    readFile(filename,orgname,entityname,FreqDistCriteria,absInterimPath,rawfilePath,nodeURLStmt,sqlScriptPath,analyzeDataOutputParq,FrequenceDistributionCalculationParq)



if __name__ == '__main__':
    main()
