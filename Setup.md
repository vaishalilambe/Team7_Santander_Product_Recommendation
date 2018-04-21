## Test Project

	$ sbt test

## Build Project

	$ sbt package

## Build Fat(Uber) Jar

	$ sbt assembly

## Generating Coverage Jar

	$ sbt clean coverage test
	$ sbt coverage test
	$ sbt coverageReport
	$ sbt coverageAggregate

	target/scala-2.11/scoverage-report/index.html

## Submit Fatjat to Spark in Local Mode

#### 1. Data Cleaning App

	
	$ /path/to/spark-2.2.0-bin-hadoop2.6/bin/spark-submit  --class edu.neu.coe.csye7200.prodrec.dataclean.main.AppRunner --master local[*] /path/to/Team_7_Santander_Product_Recommendation/data-cleaning-app/target/scala-2.11/DataCleaningApp-assembly-1.0.jar  -i /path/to/train_ver2.csv -o /path/to/outputFolder

#### 2. Machine Learning App
