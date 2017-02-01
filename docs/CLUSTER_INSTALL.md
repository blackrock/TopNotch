# Cluster Installation

## Cluster Requirements

1. The java command and the JAVA_HOME environment variable pointing to Java 8
2. Spark 2.0.2

## Installation Steps 

1. Ensure that users have access to TopNotch-assembly-0.2.jar and TopNotchRunner.sh. Additionally, both the script and the jar must be in the same folder.
    1. Checkout the TopNotch repository and build TopNotch-assembly-0.2.jar using the instructions in DEVELOPMENT.md. TopNotch-assembly-0.2.jar will be in the __target__ folder. TopNotchRunner.sh will be in the __bin__ folder.  
1. Ensure that TopNotchRunner.sh is configured to use the appropriate version of TopNotch, Spark, and the Spark and Hadoop configuration settings.
    1. Ensure TopNotchRunner.sh has the correct values for SPARK_HOME, HADOOP_CONF_DIR, and TOPNOTCH_JAR.
    1. Ensure that the spark-submit command at the end of the script connects to the correct cluster manager. By default, it runs in local mode. If you are using another manager, such as YARN, Mesos, or Standalone Mode, you may need to alter the spark-submit section of the runner scripts to connect to your manager.
    1. Ensure that TopNotchRunner.sh has the correct permissions for users to execute it. 
1. Test the installation.
    1. Run ```bin/TopNotchRunner.sh --planPath example/plan.json``` from the top folder in the TopNotch repository.
    1. Check the __topnotch__ folder in your home directory on HDFS for test output.
