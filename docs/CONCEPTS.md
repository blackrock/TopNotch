# Conceptual Guide

TopNotch is a system for testing the quality of a data set. The core of TopNotch is the assertion. Assertions are SQL where clauses that unambiguously state whether each individual data point is valid for a given condition. The rest of TopNotch allows users to organize many assertions to perform complex quality checks and to manipulate data so that it is in the correct form for assertion-based quality controls.
 
## Terminology
- Dataframe: A nested, tabular data set
- Assertion: A rule _command_. It defines a measure of quality and filters all rows which are invalid according to that metric
    - Invalid: A row is considered "invalid" if it does not pass the query clause of an assertion
    - Failure: An assertion "fails" if the fraction of rows that it declares to be "invalid" in a data set is greater than a user-specified threshold.
- View: A _command_ that transforms one or more data sets into a single data set against which assertions can be run
- Diff: A _command_ that transforms two data sets into one by joining them on a unique key and then comparing user-specified columns
- Plan: A user-defined combination of assertions, views, and diffs
- Row: A single data point in a dataframe

## The Plan And Commands

Each run of TopNotch consists of a single plan and multiple commands. The commands perform the assertion and data manipulation operations. The plan organizes the commands into an ordered list and defines the input and output for each command.

For each run of TopNotch, there is a single plan JSON file. There is a separate JSON file for each command. 
 
The two major reasons for separating plans from commands are:
1. Separation of concerns: Plans deal with the data sets that are the input and output of each command. These inputs can be files on disk or the outputs of prior commands in a plan. The outputs can be written to disk as files and/or stored in memory for use by later commands in a plan. Commands deal with data manipulation and quality control operations on individual columns.
1. Reusability of logic: Since commands only deal with specific columns and not entire data sets, commands can be reused within a plan or across multiple plans. A command can be used for any input data sets as long as the data sets have the columns refered to in the command. 

## The Types of Commands

There are three main types of commands:
1. _Assertion_: define and measure metrics of data quality
1. _Diff_: create a new data set by comparing two other data sets with similar schemas
1. _View_: create a new data set by running a SQL query on one or more existing data sets

### The Assertion Command

1. Input: A single data set
1. Operation: An assertion command contains many assertions. Each assertion is a single where clause from a SQL query that tests for a small unit of correctness. The command combines the where clauses to test for a concept.
1. Output: A report and a data set
    1. The report shows, for each assertion, what percent of rows are invalid for and a sample of invalid rows. Note that there is a seperate sample for each assertion. The only columns included in each sample are those in the assertion's where clause and the columns in the optional userDefinedFeatures argument. See USER_GUIDE.md for more details on the userDefinedFeatures argument. 
    1. The data set contains all rows that are invalid for at least one assertion in the assertion command. Each row contains all columns in the original data set along with an additional column describing all the assertions in the assertion command for which the row is invalid. 

#### Assertion Terminology
  1. Invalid: A row is considered "invalid" if it does not pass the where clause of an assertion.
  1. Failure: An assertion "fails" if the fraction of rows that it declares to be "invalid" in a data set is greater than a user-specified threshold.

### The Diff Command
1. Input: Two data sets
1. Operation: Join the two data sets on one or more columns and compare the values in one or more columns. Please note that the names of the columns that are joined on and compared do not need to match in the two data sets.
1. Output: A data set containing the join columns, and for each pair of compared columns:
    1. Two columns containing the original values in the compared columns
    1. One column stating whether the compared columns are of different types, whether either value is null, and if the values are equal
    1. If the compared columns are both numeric, one column stating the numeric difference between the values.

### The View Command
1. Input: One or more data sets
1. Operation: A SQL command on the data sets
1. Output: A data set containing the result of the SQL command.