all: awesome_hdfs.so

awesome_hdfs.so:
	g++ --shared -O2 -Wall -fPIC -L$(JAVA_HOME)/jre/lib/amd64/server  -Wl,-rpath=$(JAVA_HOME)/jre/lib/amd64/server -ljvm python_hdfs_extension.cc log.c hadoop_fs.cc libhdfs.a -o awesome_hdfs.so -DDEBUG -DHOST=\"127.0.0.1\" -DPORT=9000

clean:
	rm -rf awesome_hdfs.so
