# Lab Instructions for Apache Spark and Monte Carlo for Financial Risk

This document explains how to run the sample code that uses Monte Carlo simulation to Value at Risk (VaR) using Apache Spark.

## Prerequisites

The following prerequisites are needed in order to build & execute the code found in this example.

1. A **Unix**-like operating system with a **Bash** shell and Internet access.
2. **JDK 8** 1.8.0_31 or newer with its `bin` directory on the path.  See http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html.
3. **Scala 2.10.6** or the latest 2.10.x version with its `bin` directory on the path.  See http://www.scala-lang.org/download or just change to a temp directory and
   ​
   `curl -L http://downloads.lightbend.com/scala/2.10.6/scala-2.10.6.tgz | tee scala-2.10.6.tgz | tar xzf - ; PATH="$(pwd)/scala-2.10.6/bin:$PATH"`
   ​
   **Note:  Do not use Scala 2.11.x (because binary distributions of Spark are built against Scala 2.10, not 2.11) unless you are using your own build of Spark built against Scala 2.11.  Refer to the demo's parent-pom.xml file for which profiles to activate during the build.**
   ​
4. **Apache Spark 1.5.2** or later with its `bin` directory on the path (in particular, the `spark-submit` command must be on the path).  See https://spark.apache.org/downloads.html, or just change to the same temp directory as you did in the last step and
   ​
   `curl -L http://d3kbcqa49mib13.cloudfront.net/spark-1.5.2-bin-hadoop2.6.tgz | tee spark-1.5.2-bin-hadoop2.6.tgz | tar xzf - ; PATH="$(pwd)/spark-1.5.2-bin-hadoop2.6/bin:$PATH"`
   ​
5. **Apache Maven 3.2** or later with its `bin` directory on the path (in particular, the `mvn` command must be on the path).  See http://maven.apache.org/download.cgi, or just change to the same temp directory as you did in the last step and
   ​
   `curl -L http://apache.cs.utah.edu/maven/maven-3/3.3.9/binaries/apache-maven-3.3.9-bin.tar.gz | tee apache-maven-3.3.9.tgz | tar xzf - ; PATH="$(pwd)/apache-maven-3.3.9/bin:$PATH"`.

## Getting, Building & Running the Code

The GitHub repository is at **https://github.com/SciSpike/aas in the `vbacvanski` branch**.  You can get the code directly at https://github.com/SciSpike/aas/archive/vbacvanski.tar.gz, extract it, then change to its `ch09-risk` directory, or just 

`curl -L https://github.com/SciSpike/aas/archive/vbacvanski.tar.gz | tee aas.tgz | tar xzf - ; cd aas-vbacvanski/ch09-risk`


Inside that directory, there is a Bash script called `go`, which will download all financial data from Yahoo! Finance for the demo, build the demo, then run it using Apache Spark.  To run it, simply execute `./go` in the demo directory.

It takes a while to download & clean all of the stock market data from Yahoo! Finance (some symbols may return a `404 Not Found` so we have to check for that after downloading).  Then, the maven build should succeed fairly quickly.  After that, the big job of performing the Monte Carlo simulation on a fully local run of Apache Spark (with a master URL of `local[*]`) will start, and it takes a while.  On a 2012 MacBook Pro with eight cores, 16 Gb RAM and an SSD drive with a 300 Mbit/sec Internet connection, it takes on the order of hours to complete, depending on your local network, Internet traffic, and other processing that are running, so please be patient.  The terminal will echo progress as its made.  If you have access to your own Spark cluster, feel free to massage the source code to use that.

*NOTE:  if you get an error with text like `curl: (56) Recv failure: Connection reset by peer` while downloading Yahoo! Finance data , simply retry the `./go` command; it'll pretty much pick up where it left off.*

Remember, you can see the Spark console at http://localhost:4040 if you're curious.  Enjoy!  :)