# HiveProblem1
Perform the following tasks:

1. Copy input file from local filesystem directory/home/osgdev/Downloads to hadoop filesystem directory /user/hadoopTraining/Assignment1/input/.


hadoop fs -mkdir gs://cksproject/Assignment1/input
hadoop fs -ls gs://cksproject/Assignment1/input

hadoop fs -put bhav040416_080416.csv gs://cksproject/Assignment1/input

hadoop fs -ls gs://cksproject/Assignment1/input


Found 1 items
-rw-r--r--   3 chandan hadoop     730203 2018-09-16 18:29 Assignment1/input/bhav040416_080416.csv



2. Using Apache Hive create a database, an external table 'stockTrdData' in database and load this data in Hive table. Verify the data loaded 
by selecting first 100 rows from table.

create database cksproject;
use cksproject;
create table stockTrdData
(SYMBOL string ,
SERIES string ,
OPEN float ,
HIGH float,
LOW float,
CLOSE float,
LAST float,
PREVCLOSE float,
TOTTRDQTY float,
TOTTRDVAL float,
TIMESTAMP1 string,
TOTALTRADES int,
ISIN string) row format delimited fields terminated by ','
location "gs://cksproject/Assignment1/input" ;

select * from stockTrdData limit 10;

3. Using Apache Hive queries, list symbol on each series with highest TOTTRDVAL. Copy results to local filesystem.


select SYMBOL   from stockTrdData group by SERIES;
select max(TOTTRDVAL),SERIES   from stockTrdData group by SERIES limit 10;


5.9934404E7     BE
4.9E7   BL
108285.55       BZ
2177057.2       DR
6.9289841E9     EQ
9728000.0       IL
9068400.0       IT
16200.0 MF
1.7599872E7     N1
1.5802628E7     N2




select b.symbol , a.series,a.tot
from stockTrdData as b
join (
select max(TOTTRDVAL) as tot,SERIES   from stockTrdData group by SERIES ) as a 
on b.TOTTRDVAL=a.tot
and b.SERIES=a.SERIES limit 10;

BHARATWIRE      BE      5.9934404E7
ARVINDREM       BZ      108285.55
HDFCBANK        IL      9728000.0
IRFC    N2      1.5802628E7
L&TFINANCE      N4      4692634.0
SBIN    N5      9406041.0
RECLTD  N6      6406640.0
MUTHOOTFIN      NB      283224.2
NTPC    ND      6809296.0
SHRIRAMCIT      NG      175328.4
Time taken: 7.993 seconds, Fetched: 10 row(s)


INSERT OVERWRITE DIRECTORY output row format delimited fields terminated by ',' stored as textfile
select b.symbol , a.series,a.tot
from stockTrdData as b
join (
select max(TOTTRDVAL) as tot,SERIES   from stockTrdData group by SERIES ) as a 
on b.TOTTRDVAL=a.tot
and b.SERIES=a.SERIES limit 10; 


4. Using Apache Hive queries, list symbol on each series with highest TOTTRQTY. Check the results on Hadoop filesystem using cat command.

create table stockQuery4 ( symbol string , series string , tot float ) row format delimited feilds terminated by ',' ;

insert overwrite table stockQuery4
select b.symbol , a.series,a.tot
from stockTrdData as b
join (
select max(TOTTRDVAL) as tot,SERIES   from stockTrdData group by SERIES ) as a 
on b.TOTTRDVAL=a.tot
and b.SERIES=a.SERIES ;

hadoop fs -cat /hive/warehouse/stockQuery4/*


select * from stockQuery4 limit 10;

BHARATWIRE      BE      5.9934404E7
ARVINDREM       BZ      108285.55
HDFCBANK        IL      9728000.0
IRFC    N2      1.5802628E7
L&TFINANCE      N4      4692634.0
SBIN    N5      9406041.0
RECLTD  N6      6406640.0
MUTHOOTFIN      NB      283224.2
NTPC    ND      6809296.0
SHRIRAMCIT      NG      175328.4


5. Using Apache Hive queries, list symbol on only EQ series with highest TOTTRQTY.


select max(TOTTRDVAL),SERIES   from stockTrdData  group by SERIES having SERIES='EQ' ;

6.9289841E9     EQ

select b.symbol , a.series,a.tot
from stockTrdData as b
join (
select max(TOTTRDVAL) as tot,SERIES   from stockTrdData  group by SERIES having SERIES='EQ' 
) as a 
on b.TOTTRDVAL=a.tot
and b.SERIES=a.SERIES ;

SBIN    EQ      6.9289841E9


