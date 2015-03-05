# sequencefile-examples

A collection of examples of working with sequence files

Setup:
------

* Clone the project
```
cd /tmp && git clone https://github.com/sakserv/sequencefile-examples.git
```

* Build the project
```
cd /tmp/sequencefile-examples && bash -x bin/build.sh
```

* Write a sequence file to HDFS
```
hadoop jar target/sequencefile-examples-0.0.1-SNAPSHOT.jar \
   com.github.sakserv.sequencefile.SequenceFileWriter \
   /tmp/seqfile_ex/seqfile.seq
```

* Read the sequence file from HDFS
```
hadoop jar target/sequencefile-examples-0.0.1-SNAPSHOT.jar \
   com.github.sakserv.sequencefile.SequenceFileReader \
   /tmp/seqfile_ex/seqfile.seq
```
