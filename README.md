# [Santander Product Recommendation](https://www.kaggle.com/c/santander-product-recommendation/data)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/3dcce2c12f2649b0bcf3ec036c8456e2)](https://www.codacy.com/app/lambe.v/Team7_Santander_Product_Recommendation?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=vaishalilambe/Team7_Santander_Product_Recommendation&amp;utm_campaign=Badge_Grade)

### Build Status: [![CircleCI](https://circleci.com/gh/vaishalilambe/Team7_Santander_Product_Recommendation.svg?style=svg)](https://circleci.com/gh/vaishalilambe/Team7_Santander_Product_Recommendation)


## Introduction:

Course : [CSYE7200 Big Data Engineering with Scala](https://www.coursicle.com/neu/courses/CSYE/7200/)

Professor: Robin Hillyard

Team member:

Vaishali Lambe - [lambe.v@husky.neu.edu] (mailto:lambe.v@husky.neu.edu )

Nishant Gandhi - [gandhi.n@husky.neu.edu] (mailto:gandhi.n@husky.neu.edu])

Arpit Rawat - [rawat.a@husky.neu.edu] (mailto:rawat.a@husky.neu.edu)

Programming Language: **Scala**

## Tools / Framework: 
 - Apache Spark
 - Zepplin
 - Play Framework
 - IntelliJ IDEA
 - CircleCI
 - [GitlabCI](https://gitlab.com/nishantgandhi99/Team_7_Santander_Product_Recommendation) 

## Data Source: 

https://www.kaggle.com/c/santander-product-recommendation/data

Data Size: ~2.3GB [Rows: ~1.3M]

Backup Repository: https://github.com/vaishalilambe/Team7_Santander_Product_Recommendation

## Synopsis:

- Problem Statement:

  In this project, we build a recommendation system for a customer on the basis of behavior of his existing product consumption and recommend products similar to it.

- Approach:

  To achieve it we followed the approach of divide single task into multiple subtasks
  Here is the pipeline of our project:
    - Data Exploratory Analysis (Zepplin) -> Data Cleaning (Spark) -> Data Modelling (Spark MLLib) -> Predictions -> Play Framework (to show predictions)

  Predicted top 10 products 

- Model Accuracy

  Precision achieved with this predictive model is 0.63

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

## Submit Fatjar to Spark in Local Mode

#### 1. Data Cleaning App

	$ /path/to/spark-2.2.0-bin-hadoop2.6/bin/spark-submit  --class edu.neu.coe.csye7200.prodrec.dataclean.main.AppRunner --master local[*] /path/to/Team_7_Santander_Product_Recommendation/data-cleaning-app/target/scala-2.11/DataCleaningApp-assembly-1.0.jar  -i /path/to/train_ver2.csv -o /path/to/outputFolder

#### 2. UI App
 - Go to UI directory
 - Run the command `sbt run`
 - Open address -  http://localhost:9000

## Final Project Prsentation

https://prezi.com/view/L9AIqnlsLZrmKhNYkX50/