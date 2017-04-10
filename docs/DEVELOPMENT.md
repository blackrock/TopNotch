# Getting Started On Development

TopNotch development requires:
1. Git.  
1. Maven 3. 
1. An IDE. I recommend [IntelliJ Community Edition](https://www.jetbrains.com/idea/download/). Be sure to install the Scala plugin.  


Follow the below steps to being TopNotch Development

1. Clone this repo.
1. Import the project into Intellij.
1. Run ``mvn test`` to run the unit tests and ``mvn package`` to produce the __target/TopNotch-assembly-0.2.1.jar__ file for users.
1. See CLUSTER_INSTALL.md for how to setup TopNotch to run on your cluster.
1. See USER_GUIDE.md for an example plan and commands that can be used with your TopNotch-assembly-0.2.1.jar and runner scripts.