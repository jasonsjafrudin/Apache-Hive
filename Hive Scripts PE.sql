--create database / schema for your work
CREATE DATABASE IF NOT EXISTS cnet;
SHOW DATABASES;




--switch to my database;
USE cnet;
SHOW TABLES;



-- Display the columns names of the table in HiveQL
set hive.cli.print.header=true;
set hive.cli.print.current.db=true;

--Drop table, in-case it exists
DROP TABLE salaries_nk01;
!clear;

-- What happens if we drop the table that does not exist?
DROP TABLE salaries_nk01;

--Create table structure
CREATE TABLE salaries_nk01 (Employee_Name string, Job_Title string, Department string, Full_or_Part_Time string, Salary_or_Hourly string, Typical_Hours float, Annual_Salary float, Hourly_Rate float) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' STORED AS TEXTFILE;




--Check table layout
DESCRIBE salaries_nk01;
DESCRIBE FORMATTED salaries_nk01;

--Get record count
SELECT COUNT (*) FROM salaries_nk01;

!clear;





--In case file gets deleted from HDFS
--hadoop fs -put /home/<file_path>









LOAD DATA INPATH '/user/<file_path>' INTO TABLE salaries_nk01;     --phsyically move data from that directory into the latter directory. When you load data from HDFS, it moves the data rather than copy.
-- from HDFS
-- source file is moved

hadoop fs -ls '/user/<directory_path>'
hadoop fs -ls /user/hive/warehouse/<table/directory_path>




--Check record counts
SELECT COUNT (*) AS records FROM salaries_nk01;

LOAD DATA LOCAL INPATH '/home/<file_path>' INTO TABLE salaries_nk01;     -- copy data from the left directory to the latter directory. When you do it from linux, it does not move data, but rather copy the file
-- from LINUX
-- source file stays

ls -l /home/<file_path>






--Check record counts - what happened?
SELECT COUNT (*) AS records FROM salaries_nk01;





--Overwrite to avoid duplications
hadoop fs -put /home/<file_path> /user/<directory_path>/

LOAD DATA INPATH '/user/<directory_path>/' OVERWRITE INTO TABLE salaries_nk01;     --this will completely overwrite the data in the "salaries_nk01" table. Verses the previous load lines that does not have the overwrite, it will append the data in the file to whatever data is there in the table.
SELECT COUNT (*) AS records FROM salaries_nk01;







hadoop fs -put /home/<file_path> /user/nick/<directory_path>/

DROP TABLE salaries_nk02;

--External table.  Drop only affects metadata
CREATE EXTERNAL TABLE salaries_nk02 (Employee_Name string, Job_Title string, Department string, Full_or_Part_Time string, Salary_or_Hourly string, Typical_Hours float, Annual_Salary float, Hourly_Rate float) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' STORED AS TEXTFILE LOCATION '/user/<directory_path>/';




--Specifying alternative file formats.
--?????? Can I specify RCFile from the beginning??????*/

--Drop table, in-case it exists
DROP TABLE salaries_nk03;

--Create table structure;
CREATE TABLE salaries_nk03 (Employee_Name string, Job_Title string, Department string, Full_or_Part_Time string, Salary_or_Hourly string, Typical_Hours float, Annual_Salary float, Hourly_Rate float) 
STORED AS RCFile;



hadoop fs -put /home/nick/chicago/Chicago_Salaries.csv /user/nick/chicago_salaries/
LOAD DATA INPATH '/user/<file_path>' INTO TABLE salaries_nk03;     --this will return error, because you cannot load a csv file into a RCFile (as it is a columnar file)






--??? Why did it fail ???

LOAD DATA LOCAL INPATH '/home/nick/chicago/<file_path>' INTO TABLE salaries_nk03;
--??? Why did this one fail as well ???






--Specifying alternative file formats.
DROP TABLE salaries_nk04;
CREATE TABLE salaries_nk04
   STORED AS RCFile
   AS
SELECT * FROM salaries_nk01;

DESCRIBE FORMATTED salaries_nk04;

SELECT COUNT (*) FROM salaries_nk04;


SELECT * FROM salaries_nk01 LIMIT 5;








--Proper CSV file with SerDe
DROP TABLE salaries_nk05;

CREATE TABLE salaries_nk05 (Employee_Name string, Job_Title string, Department string, Full_or_Part_Time string, Salary_or_Hourly string, Typical_Hours float, Annual_Salary float, Hourly_Rate float)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",")  
STORED AS TEXTFILE
tblproperties ("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH '/home/<file_path>' INTO TABLE salaries_nk05;

SELECT * FROM salaries_nk05 limit 10;

SELECT Employee_Name, Annual_Salary FROM salaries_nk05 limit 10;



--OpenCSVSerde
--https://cwiki.apache.org/confluence/display/Hive/CSV+Serde
/*
WITH SERDEPROPERTIES (
   "separatorChar" = "\t",
   "quoteChar"     = "'",
   "escapeChar"    = "\\"
) 
DEFAULT_ESCAPE_CHARACTER \
DEFAULT_QUOTE_CHARACTER  "
DEFAULT_SEPARATOR        ,
*/




DROP TABLE salaries_nk06;

CREATE TABLE salaries_nk06 AS SELECT
   Department,
   AVG(Annual_Salary) AS Annual_Salary
FROM salaries_nk05
   GROUP BY Department;

	
SELECT * FROM salaries_nk06 ORDER BY Annual_Salary DESC LIMIT 5;



--Now we will store our data as Parquet to permanently avoid problems with quotes, delimiters, etc.
--However storing as Parquet (binary file) will eliminate ability to use head / tail options to see data as text file

DROP TABLE salaries_nk07;

CREATE EXTERNAL TABLE IF NOT EXISTS salaries_nk07 (Employee_Name string, Job_Title string, Department string, Full_or_Part_Time string, Salary_or_Hourly string, Typical_Hours float, Annual_Salary float, Hourly_Rate float)
STORED AS PARQUET
LOCATION '/user/<directory_path>/';

--Will invoke Map-Reduce
INSERT INTO salaries_nk07 SELECT * FROM salaries_nk05;


SELECT Employee_Name, Annual_Salary FROM salaries_nk07 limit 10;

DESCRIBE FORMATTED salaries_nk07;


















--IN CLASS ASSIGNMENT WEEK 3
--Load "cities" data from /user/nick/cities into external Hive table named "cities"
DROP TABLE cities;

CREATE EXTERNAL TABLE cities (Chicago int, London int, Helsinki int, Dubai int) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' STORED AS TEXTFILE LOCATION "/user/<directory_path>/";

select count(*) as records from cities;

--Calculate SUM for "Chicago" and write down the value

SELECT SUM(Chicago) as Sums FROM cities;


--Clean-up any extraneous records as needed
DROP TABLE cities_clean;
CREATE TABLE cities_clean STORED AS PARQUET AS SELECT * FROM cities WHERE chicago IS NOT NULL;
SELECT SUM(Chicago) as Sums FROM cities_clean;

--Drop resulting table
DROP TABLE cities;
DROP TABLE cities_clean;



CREATE EXTERNAL TABLE cities (Chicago int, London int, Helsinki int, Dubai int) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' STORED AS TEXTFILE LOCATION "/user/<directory_path>/";










-- DIFFERENT WAYS OF CREATING TABLES-----------------------------------------------------------------------------------------------------------------------
--Create table structure
CREATE TABLE salaries_nk01 (Employee_Name string, Job_Title string, Department string, Full_or_Part_Time string, Salary_or_Hourly string, Typical_Hours float, Annual_Salary float, Hourly_Rate float) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

LOAD DATA INPATH '/user/<file_path>' INTO TABLE salaries_nk01;     --phsyically move data from that directory into the latter directory. When you load data from HDFS, it moves the data rather than copy.
-- from HDFS
-- source file is moved
LOAD DATA LOCAL INPATH '/home/<file_path>' INTO TABLE salaries_nk01;     -- copy data from the left directory to the latter directory. When you do it from linux, it does not move data, but rather copy the file
-- from LINUX
-- source file stays
LOAD DATA INPATH '/user/<directory_path>/' OVERWRITE INTO TABLE salaries_nk01;     --this will completely overwrite the data in the "salaries_nk01" table. Verses the previous load lines that does not have the overwrite, it will append the data in the file to whatever data is there in the table.



--External table. When you use EXTERNAL during creating a table in hive, it means the table data is not stored in hive datawarehouse, but in some external location (like in HDFS for example).
--so when you drop EXTERNAL table, the table definition and metadata is dropped in hive datawarehouse, but the actual table dataset still exists outside wherever you put it (like in HDFS, Linux, etc)
CREATE EXTERNAL TABLE salaries_nk02 (Employee_Name string, Job_Title string, Department string, Full_or_Part_Time string, Salary_or_Hourly string, Typical_Hours float, Annual_Salary float, Hourly_Rate float) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' STORED AS TEXTFILE LOCATION '/user/<directory_path>/';
-- when you specify location means it directly copies the data from that location path you provide into the table you are creating.



--Create table structure;
CREATE TABLE salaries_nk03 (Employee_Name string, Job_Title string, Department string, Full_or_Part_Time string, Salary_or_Hourly string, Typical_Hours float, Annual_Salary float, Hourly_Rate float) 
STORED AS RCFile;

LOAD DATA INPATH '/user/<file_path>' INTO TABLE salaries_nk03;     --this will return error, because you cannot load a csv file into a RCFile (as it is a columnar file)




CREATE TABLE salaries_nk05 (Employee_Name string, Job_Title string, Department string, Full_or_Part_Time string, Salary_or_Hourly string, Typical_Hours float, Annual_Salary float, Hourly_Rate float)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",")  
STORED AS TEXTFILE
tblproperties ("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH '/home/<file_path>' INTO TABLE salaries_nk05;




CREATE TABLE salaries_nk06 AS SELECT
   Department,
   AVG(Annual_Salary) AS Annual_Salary
FROM salaries_nk05
   GROUP BY Department;





CREATE EXTERNAL TABLE IF NOT EXISTS salaries_nk07 (Employee_Name string, Job_Title string, Department string, Full_or_Part_Time string, Salary_or_Hourly string, Typical_Hours float, Annual_Salary float, Hourly_Rate float)
STORED AS PARQUET
LOCATION '/user/<directory_path>/';

--Will invoke Map-Reduce
INSERT INTO salaries_nk07 SELECT * FROM salaries_nk05;
----------------------------------------------------------------------------------------------------------------------------------------------------------------

















-- HOMEWORK ASSIGNMENT WEEK3
/home/nick/austin/Municipal_Court_Caseload_Information.zip



--1) Copy the file into your own directory on Linux
mkdir /home/jasonsjafrudin/austin
scp /home/nick/austin/Municipal_Court_Caseload_Information.zip jasonsjafrudin@34.42.58.41:/home/jasonsjafrudin/austin  --Linux to another Linux in a remote server

--2) Unzip the file
unzip /home/jasonsjafrudin/austin/Municipal_Court_Caseload_Information.zip 

cat Municipal_Court_Caseload_Information.csv  -- checking contents of csv

--3) Load the file into Hive table
DROP TABLE Municipal_Court_Caseload_Information;
CREATE TABLE Municipal_Court_Caseload_Information (
Offense_Case_Type string, 
Offense_Date string,
Offense_Time string,
Offense_Charge_Description string,
Offense_Street_Name string,
Offense_Cross_Street_Check string, 
Offense_Cross_Street string,
School_Zone string,
Constructiion_Zone string,
Case_Closed string) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '/home/jasonsjafrudin/Municipal_Court_Caseload_Information.csv' INTO TABLE Municipal_Court_Caseload_Information;


--4) Ensure you process the header record correctly
INSERT OVERWRITE TABLE Municipal_Court_Caseload_Information
SELECT * FROM Municipal_Court_Caseload_Information
WHERE Offense_Case_Type != 'Offense Case Type';



--5) Calculate frequency of offenses by Offense Case Type
INSERT OVERWRITE LOCAL DIRECTORY '/home/jasonsjafrudin' 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
select Offense_Case_Type, count(Offense_Case_Type) AS Frequency from Municipal_Court_Caseload_Information
GROUP BY Offense_Case_Type
ORDER BY Offense_Case_Type;

get /home/jasonsjafrudin/000000_0 "C:\Users\Jason Sjafrudin\Downloads\Offense_Case_Type.csv"


--6) Identify the most frequent offenses by Offense Charge Description (Show Offense Charge Description and offense frequency count in descending order)
INSERT OVERWRITE LOCAL DIRECTORY '/home/jasonsjafrudin' 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT * FROM
(
SELECT Offense_Charge_Description, COUNT(Offense_Charge_Description) AS Frequency FROM Municipal_Court_Caseload_Information
GROUP BY Offense_Charge_Description
) AS T1 
ORDER BY Frequency DESC;

get /home/jasonsjafrudin/000000_0 "C:\Users\Jason Sjafrudin\Downloads\Offense_Charge_Description.csv"



--7) After processing, delete all data in your Linux / HDFS directories and Hive tables to save disk space (zipped file, CSV file, etc.)
rm -r /home/jasonsjafrudin/Municipal_Court_Caseload_Information.csv
rm -r /home/jasonsjafrudin/austin
rm -r /home/jasonsjafrudin/000000_0
DROP TABLE Municipal_Court_Caseload_Information;


--Your final output / project result can be in any format (i.e. Excel)