# TopNotch

[![Build Status](https://travis-ci.org/blackrock/TopNotch.svg?branch=master)](https://travis-ci.org/blackrock/TopNotch) [![Coverage Status](https://coveralls.io/repos/github/blackrock/TopNotch/badge.svg?branch=master)](https://coveralls.io/github/blackrock/TopNotch?branch=master)

## What Is TopNotch?

TopNotch is a system for quality controlling large scale data sets. It addresses the following three problems:

1. How to define and measure data quality
2. How to efficiently ensure data quality across many data sets
3. How to institutionalize existing knowledge of data sets

TopNotch uses rules to verify individual components of a data set. Each rule defines and measures some small component of data quality. The combination of rules provides a complete definition of and metrics for quality in a data set. The rules can be reused on other data sets to maximize efficiency. Finally, the clear definitions and reuseability of these rules allows users to institutionalize knowledge by documenting a data set.

## Getting Started

TopNotch has three requirements:

1. Installing SBT, version 0.13.9 or greater
2. Installing Spark 1.6
3. Setting ```$SPARK_HOME``` to the top-level folder of the Spark installation.

Follow the below steps to run TopNotch:

1. Clone this repo.
2. Get the latest JAR either by building this project or by downloading it from the releases portion of TopNotch's GitHub page. Place it in this project's top level __bin__ folder.
2. Create the configuration files to test your data set.
    * See the __example__ folder for a sample data set and configuration files.
3. Run __bin/TopNotchRunner.sh__ with all the configuration files passed as arguments. Ensure that your plan file is the first argument.
    * To try the example, run the commands ```chmod u+x bin/TopNotchRunner.sh``` and then ```bin/TopNotchRunner.sh example/plan.json example/assertions.json``` . Please note that the example must be run from the root folder of the TopNotch project, as shown in the prior commands.
4. View the resulting report and parquet file in the __topnotch__ folder in your home directory on HDFS.
    * To view the results of the example, look at the JSON file __topnotch/exampleAssertionReport__ and the Parquet file __example/exampleAssertionOutput.parquet__ .

Please note that you must change __bin/TopNotchRunner.sh__ in order to run TopNotch with a master other than local. It is currently recommended that you run TopNotch in local or client mode.

## High-Level Technical Summary

At the top level there are _plans_. These _plans_ group together _commands_ into a series that is run on one or more data sets.
TopNotch quality controls data using three _commands_: _assertions_, _diffs_, and _views_. _Assertions_ encode rules. _Diffs_ and _views_ transform data sets so that they can be processed by rules.

- _Assertion_: define and measure metrics of data quality
- _Diff_: create a new data set by comparing two other data sets with similar schemas
- _View_: create a new data set by joining or selecting a subset of existing data sets to be used in diffs and assertions

The _commands'_ reusable configuration settings are defined in files separate from the _plan_. The setting externalParamsFile in the _command_ references that file. See the section Components for a more complete description.

Here is a basic example of TopNotch. It is a _plan_ which contains one _command_, an _assertion_.

##### The Plan

```javascript
    {
      "topnotch": [ {
        "command": "assertion",
        "externalParamsFile": "testAssertion.json",
        "input": {
          "ref": "viewKey",
          "onDisk": false
        },
        "outputKey": "exampleAssertionKey",
        "outputPath": "/user/durst/topnotch/exampleAssertionOutput.parquet"
      } ]
    }
```

##### The Assertion

```javascript
    {
      "topnotch" : {
        "assertions" : [ {
          "query": "loanBal > 0",
          "description": "Loan balances are positive",
          "threshold": 0.01
        } ]
      }
    }
```

## Full Technical Details

Go to http://blackrock.github.io/TopNotch to see the Scaladocs.

### Terminology
- Dataframe: A nested, tabular data set
- Assertion: A rule _command_. It defines a measure of quality and filters all rows which are invalid according to that metric
    - Invalid: A row is considered "invalid" if it does not pass the query clause of an assertion
    - Failure: An assertion "fails" if the fraction of rows that it declares to be "invalid" in a data set is greater than a user-specified threshold.
- View: A _command_ that transforms one or more data sets into a single data set against which assertions can be run
- Diff: A _command_ that transforms two data sets into one by joining them on a unique key and then comparing user-specified columns
- Plan: A user-defined combination of assertions, views, and diffs
- Row: A single data point in a dataframe

### Components

There are four components to TopNotch. Each component is persisted in JSON format.
The following list describes each component in-depth and demonstrates how to write the JSON files for each of these components:

1. The _Plan_: This runs a series of commands that quality controls data. Subsequent commands in the _plan_ can depend on the outputs of previous ones. The user can have zero, one, or any greater number of operations of each type of views, diffs, and assertions. There must be at least one operation.
    * Each _command_ takes at least one input. The _view command_ takes a list of inputs in the JSON field "inputs". The _diff command_ takes two inputs in the fields "input1" and "input2". The _assertion command_ takes one input in the field "input". Each input is either a character-delimited or Parquet file on HDFS or a dataframe in memory that is the output of a previous _command_ in the _plan_.
        * Set "onDisk" to "true" if the input is a file on disk and "false" if it is the result of a previous _command_.
        * Set "delimiter" only if the input is a character-delimited file. In that case, set the option to the file's delimiter.
    * Each _command_ outputs a dataframe that can be referred to by later commands. Set the name of this with the "outputKey" value. If the user want this value to be cached for faster access, set "cache" to "true". The "cache" flag is optional and defaults to "false" if not specified.
    * Each _command_ can write its output to disk. Set "outputPath" to a path on HDFS if the result of a command is to be persisted. Relative paths will be relative to the user's home directory on HDFS. If "outputPath" is not set, the _command_'s result will not be not persisted.
    * For each "externalParamsFile" entry, enter the path to the file relative to the plan. Currently, it is recommended that users place the plan and all associated command files in the same directory. 
    * For each assertion _command_, a report is written to the __~/topnotch__ folder on HDFS in JSON with the name of the command's output key.

    ```javascript
    {
      "topnotch": [
        {
          "command": "view",
          "externalParamsFile": "testView.json",
          "inputs": [
            {
              "ref": "topnotch/viewInput.csv",
              "onDisk": true,
              "delimiter": ","
            }
          ],
          "outputKey": "viewKey",
          "cache": true
        },
        {
          "command": "diff",
          "externalParamsFile": "testDiff.json",
          "input1": {
            "ref": "topnotch/currentLoans.parquet",
            "onDisk": true
          },
          "input1Name": "cur",
          "input2": {
            "ref": "topnotch/oldLoans.parquet",
            "onDisk": true
          },
          "input2Name": "old",
          "outputKey": "diffKey",
          "outputPath": "topnotch/diffOutput.parquet"
        },
        {
          "command": "assertion",
          "externalParamsFile": "testAssertion.json",
          "input": {
            "ref": "viewKey",
            "onDisk": false
          },
          "outputKey": "assertionKey",
          "outputPath": "topnotch/assertionOutput.parquet"
        }
      ]
    }
    ```

2. The _Assertion_ Runner: For each assertion command in a plan, this applies a number of _assertions_ to a data set, produces a dataframe containing all the rows declared invalid by any _assertion_, and creates a summary of how well the  data set abides by the assertions run against it.
    * The query uses syntax from the where clause of a HiveQL query. Each query (where clause) defines rows that are declared valid. Those not selected by the where clause are declared invalid.
    * The format for an assertion json file is:

    ```javascript
    {
      "topnotch" : {
        "assertions" : [
          {
            "query": "loanBal > 0",
            "description": "Loan balances are positive",
            "threshold": 0.01
          },
          {
            "query": "loanBal > 1",
            "description": "Loan balances are greater than 1",
            "threshold": 0.02
          }
        ]
      }
    }
    ```

3. The _Diff_ Creator: For diff each command in a plan, this joins two data sets on columns that form a unique key and then compares the values in other columns of the data sets.
    * Columns in equal locations in the "joinColumns" arrays are the keys to join the two data sets.
    * Columns in equal locations in the "diffColumns" arrays are the columns to be compared.
    * There must be at least one set of join columns and another of diff columns. The "joinColumns" arrays must have the same number of elements and so too must the "diffColumns" arrays.
    * Columns can be joined and compared even if they have different names because comparisons are determined by positions in the "joinColumns" and "diffColumns" arrays.
    * The format for a _diff_ json file is:

    ```javascript
       {
         "topnotch": {
           "input1Columns": {
             "joinColumns": [
               "loanID",
               "poolNum"
             ],
             "diffColumns": [
               "loanBal"
             ]
           },
           "input2Columns": {
             "joinColumns": [
               "loanIDOld",
               "poolNumOld"
             ],
             "diffColumns": [
               "loanBalOld"
             ]
           }
         }
       }
    ```

4. The _View_ Creator: For each view command in a plan, this takes in one or more data sets and produces a single dataframe based on a HiveQL query defined by the user. Use this to transform data into a form against which _diffs_ and _assertions_ can be run .
    * The inputs specified in the _command_ file will be loaded as tables with names specified in the "tableAliases" array for the query.
    * The result of the query is the _command_'s output.
    * The format for a _view_ json file is:

    ```javascript
    {
      "topnotch": {
        "tableAliases": [
          "loanData"
        ],
        "query": "select * from loanData"
      }
    }
    ```

Copyright © 2016 BlackRock, Inc. All Rights Reserved.

