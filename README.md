# TopNotch

[![Build Status](https://travis-ci.org/blackrock/TopNotch.svg?branch=master)](https://travis-ci.org/blackrock/TopNotch) [![codecov.io](https://codecov.io/github/blackrock/TopNotch/coverage.svg?branch=master)](https://codecov.io/github/blackrock/TopNotch?branch=master)

## What Is TopNotch?

TopNotch is a system for quality controlling large scale data sets. It addresses the following three problems:

1. How to define and measure data quality
2. How to efficiently ensure data quality across many data sets
3. How to institutionalize existing knowledge of data sets

TopNotch uses rules to verify individual components of a data set. Each rule defines and measures some small component of data quality. The combination of rules provides a complete definition of and metrics for quality in a data set. The rules can be reused on other data sets to maximize efficiency. Finally, the clear definitions and reuseability of these rules allows users to institutionalize knowledge by documenting a data set.

## Getting Started

### Requirements
    
1. The java command and the JAVA_HOME environment variable pointing to Java 8
2. Spark 2.0.2

### Quick Start Steps
1. Clone this repo.
1. Get the latest JAR, TopNotch-assembly-0.2.1.jar, either by building this project (see __docs/DEVELOPMENT.md__ for guidance on this) or by downloading it from the releases portion of TopNotch's GitHub page. Place it in this project's top level bin folder.
2. Create the configuration files to test your data set
    1. See the __example__ folder for a sample data set and configuration files.
3. Run __bin/TopNotchRunner.sh__ with the plan file passed in as an argument. 
    1. To try the example, run ```bin/TopNotchRunner.sh --planPath example/plan.json```.
    1. Note that you must set the SPARK_HOME variable either in the script or as external environment variables
    1. Note that if you have configured your Spark installation to use an existing HDFS system, you will need to upload __example/exampleAssertionInput.parquet__ to that HDFS system. You should make an example folder in your home folder on HDFS and upload __example/exampleAssertionInput.parquet__ to that folder on HDFS.
4. View the resulting report and parquet file in the __topnotch__ folder in your home directory on HDFS.
    1. To view the results of the example, look at the JSON file __topnotch/plan.json__ and the Parquet file __example/exampleAssertionOutput.parquet__. Note that if you have configured your Spark installation to use an exisiting HDFS system, the JSON and Parquet files will appear in the topnotch and example folders in your home directory on HDFS.

Please note that you must change __bin/TopNotchRunner.sh__ in order to run TopNotch with a master other than local. It is currently recommended that you run TopNotch in local or client mode.

## What To Read Next

The __docs__ folder contains the documentation. What documentation you should read depends on whether you want to use, deploy, or further develop TopNotch:

1. CONCEPTS.md 
    1. Target Audience: All
    1. Content: An overview of the parts of TopNotch and what they should be used for.
1. USER_GUIDE.md     
    1. Target Audience: Users
    1. Content: A guide for how to write the TopNotch JSON input and the specific options available for each feature. 
1. DEVELOPMENT.md 
    1. Target Audience: Developers
    1. Content: A guide on how to setup TopNotch on your local computer for development and how to run the unit tests.
1. CLUSTER_INSTALL.md 
    1. Target Audience: Developers/DevOps/ProdOps
    1. Content: A guide on how to install TopNotch on your cluster.
    
Copyright © 2017 BlackRock, Inc. All Rights Reserved.
