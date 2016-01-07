# awesome_hdfs
A Python extension written in C++ with libhdfs. If you are working with HDFS and have no good ways for daily developing. This is exactly what you are looking for!

For unknown reasons, my company does not provide Thrift interface for HDFS, and I was really tired of typing “subprocess.popen()” every day.
Whenever I am working with hadoop, and it's not cool by using subprocess. Once I was checking existence of hdfs directorys, and there were
years of data and there were hunders of folders named by day. Calling "hadoop fs -test -e" is too expensive, 30 minutes passed, just for checking
these folders. So this project was born.

##Features

* Auto classpath configuration.
* No C++ related *.so dependency problems
* Easy to use, all function are named by shell commands.

##Compiling

* Edit Makefile and change "HOST" and "PORT" according to your hadoop namenode.
* make

##Usage

Compile and copy awesome_hdfs.so to any folder of python import paths.
```python
import awesome_hdfs as hdfs

hdfs.ls('/user/your-name/')
hdfs.exist('/user/your-name')

```


