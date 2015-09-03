# HBaseJavaExample

### Introduction

This repo contains example code for accessing HBase from Java.  The example code 
will import daily stock price data from Google Finance into HBase and run simple 
queries against it.

The example was developed with HBase 1.0.1.1 or compatible, Java 8 JDK update 60,
and Fedora 22 linux (4.1.6-200.fc22.x86_64).  

### Installing HBase

HBase can be downloaded from http://hbase.apache.org/. 
The example code contained in this archive was tested with HBase 1.0.1.1.

Unpack the HBase archive and edit the configuration scripts if desired.  HBase
should start up running against the /tmp folder.  To change the folder HBase
uses for its store, edit the configuration file conf/hbase-site.xml as follows. 

```xml
<configuration>
  <property>
    <name>hbase.rootdir</name>
    <value>file:///data/hbase</value>
  </property>
  <property>
    <name>hbase.zookeeper.property.dataDir</name>
    <value>/data/zookeeper</value>
  </property>
</configuration>
```

The foregoing configuration will cause HBase to use the /data folder.  Note that
it is not necessary to create the /data/hbase and the /data/zookeeper folders;
HBase will do that for you.  However, the /data folder should be writable by
whatever user is running the HBase daemon.

To start HBase, issue the command

```
bin/start-hbase.sh.
```

### Compiling the Code

The example code contained in this archive was compiled under Java version 8 update 60 
from http://www.java.com.

The Java code must be compiled against a rather large number of jar files that
come with HBase.  I do not know if all of the jar files are really needed, but
including them all works.  There is a small shell script called makeCPATH to
help with this.  The script must be sourced with the location of the lib folder
as first argument.

```
. ./makeCPATH.sh /path/to/hbase/lib
echo $CPATH
```
Afterwards the variable CPATH should contain a list of all of the jar files in
the HBase lib folder.

To compile the Java code, change to the folder containing the Java source code
for this example, eg- TestHBase.java.  Execute the command

```
javac -cp $CPATH *.java
```

### Running the code

The location of the configuration folder of HBase should be set in the
environment variable HBASE_CONF_DIR.  This allows the code to find and read the
HBase configuration.  (The file hbase-site.xml should be in this folder.)

In addition, the Java environment variable JAVA_HOME should be set to the
folder containing the partial path "bin/java" for your Java installation.  (NOTE:
make sure this is the installation folder and not a folder containing a
symbolic link.  For example it should look like "/usr/java/jdk1.8.0_60/jre".)

The example code comes with four stock price datasets from Google Finance
obtained through http://www.quandl.com for the symbols ABT, BMY, MRK, and PFE.
These datasets are contained in the folder FinData.

The TestHBase class is defined outside of a package, so you can run it by
just

```
java -cp $CPATH:. TestHBase
```

The code will connect to the HBase instance defined in the conf/hbase-site.xml
configuration file.  Then it will drop the table (if it already exists from a
previous run), (re)create the table, load the four example stock datasets
into the table, and run some example queries.

The name of the table is BarData.  it will contain daily "candlestick" bars
of stock price movements: opening, high, low, and closing prices, and the
daily volume.  This table can be inspected offline with the hbase shell.
(See https://learnhbase.wordpress.com/2013/03/02/hbase-shell-commands/ for
more info.)

### Obtaining the example stock data 

The folder FinData contains four example datasets downloaded from
Google Finance datasets collected at https://www.quandl.com/data/GOOG.

To find and download a dataset, enter a search term like "Exxon"
into the Datasets search box.  Navigate to the desired dataset
and click on the "CSV" button in the upper right corner under "Export".

The dataset should have the schema Date, Open, High, Low, Close, Volume.
The data will not contain entries for weekends and some holidays, but
will sometimes contain incomplete or blank data for holidays.  Merely
loading this data into HBase will not tidy it up.
