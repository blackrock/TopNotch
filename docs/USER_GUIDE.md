# Detailed Users' Guide

There are four components to TopNotch: plans, assertions, diffs, and views. All plans and commands are specified in JSON files.
 
There are separate JSON files for the plan and each command. The plan's commands list specifies the order in which commands are run, the input and output to each command, and where to find the files for each command. Each command file specifies the data manipulation and qc logic of the command. 
 
The following list describes each component in-depth and demonstrates how to write the JSON files for each of these components.

## How To Run TopNotch
1. Run the script using ```bin/TopNotchRunner.sh --planPath (location of your plan)```
1. Go to the driver's web UI to monitor your job.
1. Your job's logs will print to stdout.
1. The script has the following exit codes
    * 0 - The fractionInvalid is less than every threshold for every assertion
    * 3 - The fractionInvalid is greater than every threshold for at least one assertion
    * 4 - The command line arguments were specified incorrectly.
1. Please note that plans and command json files can be parameterized with variables passed in through the command line. See the Variables section of this page for more information.

### TopNotch Command Line Interface Features
TopNotch can read plans and commands from standalone files on disk, from files that are packaged inside of a jar on disk,
 or from a REST API. The below documentation, which is the result of running the TopNotch jar with the
 --help flag, shows how to use these and other options.

```
If the plan and commands are standalone files on disk, set planPath to the path on disk. 
If the plan and commands are packaged in one or more jars, add the jars containing the plans and commands to the Spark submit --jars flag. (Note: TopNotchRunner.sh does not support the --jars flag) Then, set --planPath to the classpath to the jar.
If the plan and commands are accessible by a REST API, set planServer to the base URL for the API and planPath to the route in the API for accessing the plan.
  -l, --planPath <path>    planPath is the path to the plan on disk or relative to planServerURL
  -s, --planServerURL <URL>
                           planServerURL is the base URL of the REST server for loading a plan. Note that this URL should not include the route relative to the URL for loading the plan
  -k, --reportKey <key>    the key for referring to the report. The filename from planPath will be used if this is not set
  -c, --haltIfPotentialErrors
                           If set, TopNotch will halt after parsing and before before executing any commands if there are potential errors in the configurations. By default, TopNotch will parse the configs, print a warning,and proceed with execution in the event of potential errors as certain valid configurations may appear to be invalid.
  -d, --variableDictionary variable1=value1,variable2=value2...
                           variables and values for string replacement in the plan and commands
```

## Plan
The _Plan_: This specifies an ordered sequence of commands and nested plans. Subsequent commands in the _plan_ can depend on the outputs of previous ones. A plan can have any number of views, diffs, assertions, and nested plans as long as there is at least one operation.

The plan has two main components
1. io: An object that specifies where the the assertion reports should be written to. This can be HBase, a file on HDFS, or a server. The following points show how to set all possible values of io.
    - HBase - the reports will be written to the TopNotch table, reports column family, and qualifier column family. To change this location, you must change the values in the TnHBaseWriter object in src/main/scala/com/bfm/topnotch/tnengine/TnWriter.scala.
    ```javascript
    "io": {
        "writer": "hbase"
      }
    ```
    - HDFS without directory specification - the reports will be written to the topnotch folder in the user's home directory on HDFS
    ```javascript
    "io": {
        "writer": "hdfs"
      }
    ```
    - HDFS with directory specification
    ```javascript
    "io": {
        "writer": "hdfs"
        "dest": "/user/testUser/"
      }
    ```
    - REST - the reports will be sent as a PUT request to the URL specified, with the outputKey for the report's assertion command appended to the end of the URL
    ```javascript
    "io": {
        "writer": "rest"
        "dest": "http://www.testurl.com"
      }
    ```  
1. commands: A list that specifies which commands to run. The commands will be run sequentially in the order specified in the commands list.
    - There is a different format for specifying each of the types of commands. The section on commands explains how to specify each type of command.  
#### Sample Plan 

```javascript
{
  "io": {
    "writer": "file",
    "dest": "topnotchReports/"
  },
  "commands": [
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
      "threshold": 0.1,
      "filterEqualRows": true,
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
    },
    {
      "command": "plan",
      "externalParamsFile": "topnotch/nestedPlan.json"
    }
  ]
}
```
## Commands
### How To Specify To Commands and Nested Plans In a Plan JSON File
#### Input
Specify each input data set using an object. This can either be a data set from disk or the result of a previous command.
  * Mandatory Arguments:
    * ref - The string the refers to the data set. If reading from disk, this should be the location of the file on disk. If it is the output of a prior command, ref should be the outputKey of the command who produced the data set.
      * type - string
      * example value - "priorCommandKey"
    * onDisk - A boolean for whether loading a file off disk or reading the result of a previous command. If reading a file from disk, set to true. If input is the result of a previous command, set to false
      * type - boolean
      * example value - true
  * Optional Arguments
    * delimiter - Set this option if and only if the input is a csv, tsv, or other character-delimited file instead of a Parquet file. This string is the delimiter used in the input file. Do not use this field if reading in a Parquet file. This string must contain a single character.
      * type - string
      * example value - ","
      
#### externalParamsFile
Each command and nested plan in a plan uses externalParamsFile to specify the path of a JSON file with more information. The path can have the following forms:
1. If the parent plan containing the externalParamsFile key is a standalone JSON file on disk, then each externalParamsFile path must refer to a standalone JSON file on disk. Each path can either be relative to the parent plan's location, if the path doesnt start with a /, or absolute, if the path starts with a /.
    1. Note: externalParamsFile paths in a nested plan will be relative to the path of the nested plan
1. If the parent plan is packaged in a jar on the local file system, then each externalParamsFile must refer to a JSON file packaged in a jar on the local file system. Each externalParamsFile path must be the absolute classpath to a JSON file.
1. If using a REST API to load plans and commands, then all plans and commands must be accessed via the REST API. Each externalParamsFile path must point to the appropriate REST url for retrieving the JSON plan or command by a GET request when the path is appended to the value passed in for the --planServerURL flag. 
      
#### Assertion:
  * Mandatory Arguments:
    * command - specifies that the command is of type assertion
      * type - string
      * example value - "assertion" (NOTE: assertion is the only valid value)
    * externalParamsFile - specifies the location of the JSON file for the assertion
      * type - string
      * example value - "../path/to/assertion.json" 
    * input - specifies the input to the assertion
      * type - object
      * example value - see the above section on how to specify input
    * outputKey - This value should be globally unique. It provides a key for referring to the output of this assertion at a later step in the plan. This output will contain all the rows that are invalid according to at least one assertion in this assertion command. Also, assertion reports are named by the outputKey.
      * type - string
      * example value - "exampleAssertionKey"
  * Optional Arguments
    * outputPath - The location to persist the output of this command to. If this is not specified, the output will not be saved to disk.
      * type - string
      * example value - "location/on/hdfs" (NOTE: path is on HDFS and relative to user's HDFS home directory unless it starts with /)
    * cache - Store the output of this command in RAM. If this is set to true, the results of this command will be stored in RAM after the first time that they are written to disk or referred to by a later command. If cache is not specified or set to false, the output of this command will be recomputed each time it is used.
      * type - boolean
      * example value - true
    * tableName - Mount the output of this command as a table for access through SparkSQL. This can only be set if outputPath is also set.
      * type - string
      * example value - "tableToMount" (NOTE: the table will be mounted in the default )
  * Example of How to Specify an Assertion in a Plan
  ```javascript
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
  ```
  
#### Diff
  * Note: when diffing on nested dataframes, the columns will be unnested and all occurrences of "." in any column name will be replaced with "_".
  * Mandatory Arguments:
    * command - specifies that the command is of type diff
      * type - string
      * example value - "diff" (NOTE: diff is the only valid value)
    * externalParamsFile - specifies the location of the JSON file for the assertion
      * type - string
      * example value - "../path/to/diff.json"
    * input1 - specifies the first of the two input data sets to diff
      * type - object
      * example value - see the above section on how to specify input
    * input1Name - specifies the name to use when referring to the columns from the first input data set in the diff
      * type - string
      * example value - "firstDiffDataSet"
    * input2 - specifies the second of the two input data sets to diff
      * type - object
      * example value - see the above section on how to specify input
    * input2Name - specifies the name to use when referring to the columns from the second input data set in the diff
      * type - string
      * example value - "secondDiffDataSet"
    * outputKey - This value should be globally unique. It provides a key for referring to the output of this diff at a later step in the plan. This output will contain the difference between the two data sets as described in CONCEPTS.md.
      * type - string
      * example value - "exampleDiffKey"
  * Optional Arguments
    * filterEqualRows - Whether to filter out rows where all compared columns are equal. If true, the equal rows are filtered out. If false, the equal rows are included. This is false if not set.
      * type - boolean
      * example value - True
    * threshold - The threshold to use when determining if numeric values are equal. By default, this is 1e-6. This threshold will apply for all columns that are compared. An optional thresholds argument can also be set in the diff command json file that enables users to set a separate threshold for each column. If both the threshold option in the plan json file and the thresholds option in the command json file are set, then the thresholds argument in the command json file will override the threshold in the plan json file. 
      * type - double
      * example value - 0.001
    * outputPath - The location to persist the output of this command to. If this is not specified, the output will not be saved to disk.
      * type - string
      * example value - "location/on/hdfs" (NOTE: path is on HDFS and relative to user's HDFS home directory unless it starts with /)
    * cache - Store the output of this command in RAM. If this is set to true, the results of this command will be stored in RAM after the first time that they are written to disk or referred to by a later command. If cache is not specified or set to false, the output of this command will be recomputed each time it is used.
      * type - boolean
      * example value - true
    * tableName - Mount the output of this command as a table for access through SparkSQL. This can only be set if outputPath is also set.
      * type - string
      * example value - "tableToMount" (NOTE: the table will be mounted in the default )
    
  * Example of How to Specify a Diff in a Plan
  ```javascript
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
    "threshold": 0.1,
    "filterEqualRows": true,
    "outputKey": "diffKey",
    "outputPath": "topnotch/diffOutput.parquet"
    }
  ```
  
#### View 
  * Mandatory Arguments:
    * command - specifies that the command is of type view
      * type - string
      * example value - "view" (NOTE: view is the only valid value)
    * externalParamsFile - specifies the location of the JSON file for the view
      * type - string
      * example value - "../path/to/view.json"
    * inputs - specifies the inputs to the view
      * type - a list of objects
      * example value - see the above section on how to specify each input object
    * outputKey - This value should be globally unique. It provides a key for referring to the output of this view at a later step in the plan. 
      * type - string
      * example value - "exampleViewKey"
  * Optional Arguments
    * outputPath - The location to persist the output of this command to. If this is not specified, the output will not be saved to disk.
      * type - string
      * example value - "location/on/hdfs" (NOTE: path is on HDFS and relative to user's HDFS home directory unless it starts with /)
    * cache - Store the output of this command in RAM. If this is set to true, the results of this command will be stored in RAM after the first time that they are written to disk or referred to by a later command. If cache is not specified or set to false, the output of this command will be recomputed each time it is used.
      * type - boolean
      * example value - true
    * tableName - Mount the output of this command as a table for access through SparkSQL. This can only be set if outputPath is also set.
      * type - string
      * example value - "tableToMount" (NOTE: the table will be mounted in the default )
  * Example of How to Specify a View in a Plan
  ```javascript
  {
    "command": "view",
    "externalParamsFile": "testView.json",
    "inputs": [
      {
        "ref": "topnotch/viewInput.csv",
        "onDisk": true,
        "delimiter": ","
      },
      {
        "ref": "topnotch/otherViewInput.parquet",
        "onDisk": true
      }
    ],
    "outputKey": "viewKey",
    "cache": true
  }
  ```
  
#### Nested Plan 
  * Mandatory Arguments:
    * command - specifies that this is a nested plan
      * type - string
      * example value - "plan" (NOTE: plan is the only valid value)
    * externalParamsFile - specifies the location of the JSON file for the nested plan
      * type - string
      * example value - "../path/to/nestedPlan.json" 
  * Example of How to Specify a Nested Plan in a Plan
  ```javascript
  {
    "command": "plan",
    "externalParamsFile": "topnotch/nestedPlan.json"
  }
  ```
  
### How To Write Commands JSON Files
#### Assertion:
  * Mandatory Arguments:
    * assertions - specifies the list of assertions contained in this file
      * type - array of objects
      * example value 
    ```javascript
    [
      {
        "query": "loanBal > 0",
        "description": "Loan balances are positive",
        "threshold": 0.01
      }
    ]
    ```
  * Mandatory Elements of Object in Assertions 
    * query - the where clause of a HiveQL statement that specifies a condition for validity; THE CONDITION SHOULD BE TRUE FOR VALID DATA
      * type - string
      * example value - "loanBal > 0"
    * description - a description of what the assertion checks for in English (or any other human-readable language)
      * type - string
      * example value - "loan balances are positive"
    * threshold - specifies the percent of rows in the data set that must be invalid in order for the assertion to fail
      * type - double
      * example value - 0.01
  * Optional Arguments
    * userDefinedFeatures - Additional, derived columns that should be included in the output data set and in the samples in the report. These columns can be any valid HiveQL columns.  
      * type - object where all keys and values are strings. Each key is the name of a new column. Each value is a HiveQL expression for a new column. 
      * example value 
      ```javascript
      {
        "loanAgeInYears": "loan_age / 12.0",
      }
      ```
    * userDefinedSummaryExpr - Summary statistics of the invalid data points. These columns can be any valid HiveQL expressions that produce a single row, such as sum, average, or a combination of such functions.
      * type - object where all keys and values are strings. Each key is the name of a new column. Each value is a HiveQL expression for a new column that contains one row.
      * example value - true
      ```javascript
      {
        "averageLoanAge": "avg(loan_age)"
      }
      ```
    * sampleWindowParams - For each invalid row in an assertion's report, this argument adds a collection of related, potentially valid rows that provide context for the invalid row. This collection is referred to as a "window".
      * type - object with two keys:
        1. idsForWindowParitioning - The set of columns that determine which rows are related. For each row that is invalid, the returned window will contain all rows that have equal values for these columns.
          * type - array of strings, where each string is a column name
          * example value - ["loanBal", "zipCode"]
        2. orderEachWindowBy - The set of columns that determine how to order the window when it is presented in the report. The first column has the highest priority when determining order, the second column has the second highest priority, and further columns are decreasing priority in the order provided.
          * type - array of strings
          * example value - ["date", "loanBal"]
       * example value
       ```javascript
         {
           "idsForWindowPartitioning":  ["loanBal", "zipCode"],
           "orderEachWindowBy": ["date", "loanBal"]
         }
      ```
  * Example of How to Write an Assertion JSON File
    ```javascript
    {   
      "assertions" : [
        {
          "query": "loanBal > 0",
          "description": "Loan balances are positive",
          "threshold": 0.01
        },
        {
          "query": "loanBal > 1",
          "description": "Loan balances are greater than 1",
          "threshold": 0.02,
          "userDefinedFeatures": {
            "loanAgeInYears": "loan_age / 12.0",
          },
          "userDefinedSummaryExpr": {
            "averageLoanAge": "avg(loan_age)"
          },
          "sampleWindowParams": {
            "idsForWindowPartitioning":  ["loanBal", "zipCode"],
            "orderEachWindowBy": ["date", "loanBal"]
          }
        }
      ]
    }
    ```
#### Diff:
  * Mandatory Arguments:
    * input1Columns - specifies the columns from input1 to join on and to diff with input2
      * type - object with two keys
        1. joinColumns - The columns from input1 to use when joining input1 with input2. Columns in joinColumns for input1 and input2 will be joined based on order. The first column in each joinColumns array will be joined on, then the second one in each array, etc. Therefore, it is possible to join columns with different names. The only requirements are that the two joinColumns arrays have the same number of values and that they both have at least one value.
          * type - array of strings, where each string is a column name.
          * example value - ["loanID", "poolNum"] 
        2. diffColumns - The columns from input1 to compare with the columns from input2. Columns in diffColumns for input1 and input2 will be compared based on order. The first column in each diffColumns array will be diffed, then the second one in each array, etc. Therefore, it is possible to diff columns with different names. The only requirement are that the two diffColumns arrays have the same number of values and that they both have at least one value.
          * type - array of strings
          * example value - ["loanBal", "loan_age", "balance"]
      * example value 
      ```javascript
      {
        "joinColumns": [
          "loanID",
          "poolNum"
        ],
        "diffColumns": [
          "loanBal",
          "loan_age",
          "balance"
        ]
      }
       ```
    * input2Columns - specifies the columns from input2 to join on and to diff with input1. It has the same structure as input1Columns. See the above documentation on input1Columns.
  * Optional Arguments: 
    * thresholds - The thresholds to use for each column when determining if numeric values are equal. By default, this is 1e-6 for all columns. This overrides the the threshold argument optionally provided in the plan for each diff command. This argument has no effect of non-numeric columns. 
      * type - array of doubles
      * example value - [1.0, 2, 10000]
  * Example of How to Write an Diff JSON File
    ```javascript
       {
         "thresholds": [1.0, 2, 10000],
         "input1Columns": {
           "joinColumns": [
             "loanID",
             "poolNum"
           ],
           "diffColumns": [
             "loanBal",
             "loan_age",
             "balance"
           ]
         },
         "input2Columns": {
           "joinColumns": [
             "loanIDOld",
             "poolNumOld"
           ],
           "diffColumns": [
             "loanBalOld",
             "loan_age",
             "balance"
           ]
         }
       }
    ```
    
#### View:
  * Mandatory Arguments:
    * tableAliases - Aliases for how the query should refer to the inputs passed in through the plan. Each alias is a table that can be used in the query. The aliases are matched to the inputs based order, so that the first alias refers to the first input, the second alias to the second input, etc. There must be the same number of aliases as inputs.
      * type - array of strings
      * example value - ["loanData", "oldLoanData"]
    * query - The sql query to run that produces a new data set.
      * type - string
      * example value - "select * from loanData inner join oldLoanData on loanData.cusip = oldLoanData.cusip"

  * Example of How to Write an View JSON File
    ```javascript
    {
      "tableAliases": [
        "loanData",
        "oldLoanData"
      ],
      "query": "select * from loanData inner join oldLoanData on loanData.cusip = oldLoanData.cusip"
    }
    ```
    
## Variables
1. Any part of any command or plan can contain a variable. Just add ${variableName} in the key or value of any JSON.
1. Pass in a dictionary that defines values for the variables using the --variableDictionary flag. The dictionary should be a comma-separated list of pairs with the format variableName=variableValue.
1. Below is an example for how to parameterize an input to an assertion using the date and product type:
  * Command line argument: --variableDictionary="date=$(date +%D),productType=mtg"
  * Input example:
  ```
  "input": {
    "ref": "topnotch/${productType}-${date}.parquet",
    "onDisk": true
  }
  ```