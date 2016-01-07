/*
The MIT License (MIT)

Copyright (c) [2015] [liangchengming]

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

#ifndef DANGDANG_HDFS
#define DANGDANG_HDFS

#include <string>
#include <vector>

#ifdef __cplusplus
extern "C" {
#endif

#include "hdfs.h"

class HDFS_FILE {
	public:
		HDFS_FILE(const char* host, const int port);
		HDFS_FILE();
		~HDFS_FILE();

		int init(const char* host, const int port);
		int open(const char* path, const char* mode);

		size_t read(void* buf, size_t size);
		size_t write(void* line);
		int connect(const char* host, int port);
		bool exist(const char* path);
		int cp(const char* src, const char* dst);
		int mv(const char* src, const char* dst);
		int put(const char* src, const char* dst);
		int putf(const char* src, const char* dst);
		int rename(const char* src, const char* dst);
		int rm(const char* path);
		int mkdir(const char* path);
		hdfsFileInfo* ls(const char* path, int* cnt);
		hdfsFileInfo* dirinfo(const char* path);
		int chmod(const char* path, short mode);
		int chown(const char* path, const char* owner, const char* group);
		int flush();
		int getmerge(const char *src, const char *dst);

		char* getline();
		void close();
	private:
		void hadoop_env();
		std::string add_schema(std::string path);
		bool match(std::vector<std::string> &fields);
		int port;
		std::string host;

		hdfsFile _f;
		hdfsFS connection;

		char buffered_chars();
		char buffer[1024];
		char *current;
		bool eof;
};


#ifdef __cplusplus
}
#endif


#endif
