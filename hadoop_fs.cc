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

#include "hadoop_fs.h"
#include "log.h"

#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <dirent.h>
#include <unistd.h>
#include <fnmatch.h>
#include <string>
#include <libgen.h>
#include <vector>

#ifdef __cplusplus
extern "C" {
#endif

HDFS_FILE::HDFS_FILE() {
	this->init("", 0);
}

HDFS_FILE::HDFS_FILE(const char* host, const int port) {
	this->init(host, port);
}

int HDFS_FILE::connect(const char* host, int port) {
	check(host != NULL and strlen(host) > 0 and port > 0);
	check(this->connection == NULL);

	this->connection = hdfsConnect(this->host.c_str(), this->port);
	if (this->connection == NULL) {
		error(strerror(errno));
		return errno;
	}

	return 0;
}

static char* append(const char* s1, const char* s2) {
	int len = strlen(s1) + strlen(s2) + 2;
	char* curr = (char*)malloc(len);
	memset(curr, 0, len);
	snprintf(curr, len, "%s/%s", s1, s2);
	return curr;
}

static int scan(const char* path, std::vector<std::string> &libjars) {
	struct dirent **namelist;
	int n = scandir(path, &namelist, NULL, alphasort);
	if (n > 0) {
		while (n--) {
			if (strcmp(namelist[n]->d_name, ".") == 0 or strcmp(namelist[n]->d_name, "..") == 0) {
				free(namelist[n]);
				continue;
			}
			if (namelist[n]->d_type == DT_DIR) {
				char* pwd = append(path, namelist[n]->d_name);
				scan(pwd, libjars);
				free(pwd);
			} else {
				char * filename = append(path, namelist[n]->d_name);
				if (fnmatch("*.jar", filename, 0) == 0 and \
					fnmatch("*tomcat/webapps/*", filename, 0) != 0 and \
					fnmatch("*mapreduce1*", filename, 0) != 0 and \
					fnmatch("*spark*", filename, 0) != 0) {
					libjars.push_back(filename);
				}
				free(filename);
			}
			free(namelist[n]);
		}
		free(namelist);
	} else {
		perror(path);
	}
	return 0;
}

void HDFS_FILE::hadoop_env() {

	std::vector<std::string> libjars;
	libjars.reserve(600);
	char* libpath = append(getenv("HADOOP_HOME"), "share/hadoop");
	scan(libpath, libjars);
	free(libpath);

	std::string paths;
	for (size_t i = 0; i < libjars.size(); i++) {
		paths += ":";
		paths += libjars[i];
	}

	std::string classpath = getenv("CLASSPATH");
	setenv("CLASSPATH", (classpath+":"+paths).c_str(), 1);
}

int HDFS_FILE::init(const char* host, const int port) {
	check(host != NULL);
	this->host = host;
	this->port = port;

	this->_f = NULL;
	this->connection = NULL;

	memset(this->buffer, 0, sizeof(this->buffer));
	this->current = this->buffer;
	this->eof = false;

	this->hadoop_env();

	if (this->host.size() > 0 and this->port > 0) {
		this->connect(this->host.c_str(), this->port);
	}

	return 0;
}

HDFS_FILE::~HDFS_FILE() {
	if (this->_f != NULL) {
		this->close();
	}
	if (this->connection != NULL) {
		hdfsDisconnect(this->connection);
		this->connection = NULL;
	}
}

size_t HDFS_FILE::read(void* buf, size_t size) {
	check(this->connection != NULL and this->_f != NULL and size > 0);
	tSize bytes = hdfsRead(this->connection, this->_f, buf, size);
	if (bytes == -1) {
		error(strerror(errno));
		return 0;
	}
	return static_cast<size_t>(bytes);
}

char HDFS_FILE::buffered_chars() {
	check(this->connection != NULL and this->_f != NULL);

	if (this->current == this->buffer+sizeof(this->buffer)-1 or this->current == this->buffer) {
		memset(this->buffer, 0, sizeof(this->buffer));
		tSize bytes = hdfsRead(this->connection, this->_f, this->buffer, sizeof(this->buffer));
		if (bytes == -1) {
			error(strerror(errno));
			return 0;
		}
		this->current = this->buffer;
	}

	char ch = *(this->current);
	this->current++;

	return ch;
}

char* HDFS_FILE::getline() {
	check(this->connection != NULL and this->_f != NULL);

	size_t max_len = 1024*sizeof(char);
	char* line = (char*)malloc(max_len);
	memset(line, 0, max_len);
	char* ptr = line;

	while (true) {
		if (this->eof) {
			memset(line, 0, max_len);
			return line;
		}
		char ch = this->buffered_chars();
		*ptr = ch;
		if (ch == '\0') {
			this->eof = true;
			break;
		}
		if (ptr == line+max_len-1) {
			line = (char*)realloc(line, max_len*2);
			ptr = line+max_len;
			max_len = max_len*2;
		}
		if (*ptr == '\n') {
			*(ptr+1) = 0;
			return line;
		}
		ptr++;
	}

	return line;
}


size_t HDFS_FILE::write(void* line) {
	const char *_line = reinterpret_cast<const char*>(line);
	int size = strlen(_line);
	if (size == 0) {
		return 0;
	}

	tSize nwrite = hdfsWrite(this->connection, this->_f, _line, size);
	if (nwrite == -1) {
		error(strerror(errno));
		return 0;
	}
	return static_cast<size_t>(nwrite);
}

static bool contains_wildchars(std::string &path) {
	return (
		path.find('*') != std::string::npos or
		path.find('[') != std::string::npos or
		path.find('?') != std::string::npos or
		path.find('{') != std::string::npos
	);
}

bool HDFS_FILE::match(std::vector<std::string> &fields) {
	check(fields.size() > 3);

	std::string path = "";
	for (size_t i = 0; i < fields.size(); i++) {
		if (contains_wildchars(fields[i])) {
			int cnt = 0;
			hdfsFileInfo* fs = hdfsListDirectory(this->connection, path.c_str(), &cnt);
			bool found = false;
			for(int j = 0; j < cnt; j++) {
				if (strcmp((path+"/_SUCCESS").c_str(), fs[j].mName) == 0) {
					continue;
				}
				if (fnmatch((path+fields[i]).c_str(), fs[j].mName, 0)  == 0) {
					path = fs[j].mName;
					found = true;
					break;
				}
			}
			hdfsFreeFileInfo(fs, cnt);
			if (found == false) {
				return false;
			}
		} else {
			path += fields[i];
		}
	}
	int st = hdfsExists(this->connection, path.c_str());
	if (st == -1) {
		return false;
	}
	return true;
}


std::string remove_double_slash(std::string path) {
	char* p = (char*)malloc(sizeof(char)*strlen(path.c_str())+1);
	char* ptr = p;
	int schema = 0;
	for (size_t i = 0; i < strlen(path.c_str()); i++) {
		if (path[i] == ':') {
			schema = i;
		}
		if (path[i] == '/' and *(p-1) == '/') {
			if (i - schema == 2) { /* dealing with schema <file://> or <hdfs://>  */
				*p++ = path[i];
			}
			continue;
		}
		*p++ = path[i];
	}
	*p = 0;
	std::string slim_path = ptr;
	free(ptr);
	return slim_path;
}

std::string HDFS_FILE::add_schema(std::string path) {
	if (path.find("hdfs://") != std::string::npos) {
		return path;
	}
	char fname[1024];
	snprintf(fname, sizeof(fname)-1, "hdfs://%s:%d/%s", this->host.c_str(), this->port, path.c_str());
	std::string full_path = fname;
	return full_path;
}

int split_by_wildchars(const char* path, std::vector<std::string> &fields) {
	char buffer[128];
	memset(buffer, 0, 128);
	char* p = buffer;
	int left = -1;
	int right = -1;
	for (size_t i = 0 ; i < strlen(path); i++) {
		if (path[i] == '{' or path[i] == '[') {
			if (left < 0) {
				left = i;
			}
		}
		if (path[i] == '}' or path[i] == ']') {
			right = i;
		}
		if (path[i] == '/') {
			if (left > 0 and right < 0) {
				continue;
			}
			if (strlen(buffer) > 0) {
				fields.push_back(buffer);
			}
			memset(buffer, 0, 128);
			p = buffer;
			left = -1;
			right = -1;
		}
		*p++ = path[i];
	}
	if (p != buffer and strcmp(buffer, "/") != 0) {
		fields.push_back(buffer);
	}
	return fields.size();
}

bool HDFS_FILE::exist(const char* path) {
	check(path != NULL and strlen(path) > 0);
	check(this->connection != NULL);

	std::vector<std::string> fields;
	split_by_wildchars(remove_double_slash(add_schema(path)).c_str(), fields);

	return match(fields);
}


int HDFS_FILE::open(const char* path, const char* mode) {
	check(strlen(path) > 0);
	check(this->_f == NULL and this->connection != NULL);

	char fname[1024];
	snprintf(fname, sizeof(fname)-1, "hdfs://%s:%d%s", this->host.c_str(), this->port, path);

	int flag = 0;
	if (!strcmp(mode, "r")) {
		flag = O_RDONLY;
	} else if (!strcmp(mode, "w"))  {
		flag = O_WRONLY;
	} else if (!strcmp(mode, "a"))  {
		flag = O_WRONLY | O_APPEND;
	} else {
		error("Unknown mode:%s", mode);
		return false;
	}
	
	this->_f = hdfsOpenFile(this->connection, fname, flag, 0, 0, 0);
	if (this->_f == NULL) {
		error(strerror(errno));
	}

	return 0;
}


void HDFS_FILE::close() {
	check(this->_f != NULL and this->connection != NULL);

	if (hdfsCloseFile(this->connection, this->_f) == -1) {
		error(strerror(errno));
	}

	this->_f = NULL;
	this->current = NULL;
	this->buffer[0] = '\0';
}


int HDFS_FILE::flush() {
	check(this->_f != NULL and this->connection != NULL);
	check(hdfsFileIsOpenForWrite(this->_f) == 0);

	return hdfsFlush(this->connection, this->_f);
}

int HDFS_FILE::cp(const char* src, const char* dst) {
	check(src != NULL and dst != NULL and this->connection != NULL);
	check(strcmp(src, dst) != 0);
	return hdfsCopy(this->connection, src, this->connection, dst);
}

int HDFS_FILE::mv(const char* src, const char* dst) {
	check(src != NULL and dst != NULL and this->connection != NULL);
	check(strcmp(src, dst) != 0);
	return hdfsMove(this->connection, src, this->connection, dst);
}

int HDFS_FILE::put(const char* src, const char* dst) {
	check(src != NULL and dst != NULL and this->connection != NULL);
	check(strcmp(src, dst) != 0);

	std::string dest = remove_double_slash(add_schema(dst));
	if (dest[dest.size()-1] == '/') {
		dest = dest.substr(0, dest.size()-1);
	}

	if(exist(dest.c_str()) == true) {
		hdfsFileInfo * f_info = hdfsGetPathInfo(this->connection, dest.c_str());
		if (f_info != NULL and f_info->mKind == kObjectKindDirectory) {
			hdfsFreeFileInfo(f_info, 1);
			dest += "/";
			dest += std::string(basename(const_cast<char*>(src)));

			int cnt = 0;
			hdfsFileInfo* fs = hdfsListDirectory(this->connection, dest.c_str(), &cnt);
			for (int i = 0; i < cnt; i++) {
				if (strcmp(fs[i].mName, dest.c_str()) == 0) {
					error("%s:%s\n", dest.c_str(), "File Existed !");
					return -1;
				}
			}
			hdfsFreeFileInfo(fs, cnt);

		} else {
			error("%s:%s\n", dest.c_str(), "File Existed !");
			return -1;
		}
	}

	if(access(src, R_OK) == -1) {
		error("%s:%s\n", src, strerror(errno));
		return errno;
	}

	hdfsFile f = hdfsOpenFile(this->connection, dest.c_str(), O_WRONLY, 0, 0, 0);
	if (f == NULL) {
		error("%s:%s\n", dst, strerror(errno));
		return errno;
	}

	FILE* local_f = fopen(src, "rb");
	if (local_f == NULL) {
		error("%s:%s\n", src, strerror(errno));
		return errno;
	}

	char buffer[20480];
	while (!feof(local_f)) {
		size_t cnt = fread(buffer, 1, sizeof(buffer), local_f);
		tSize nwrite = hdfsWrite(this->connection, f, buffer, cnt);
		if (nwrite == -1) {
			error("%s:%s\n", dest.c_str(), strerror(errno));
			fclose(local_f);
			hdfsCloseFile(this->connection, f);
			return errno;
		}
	}

	fclose(local_f);
	hdfsCloseFile(this->connection, f);

	return 0;
}

int HDFS_FILE::putf(const char* src, const char* dst) {
	check(src != NULL and dst != NULL and this->connection != NULL);
	check(strcmp(src, dst) != 0);

	std::string dest = remove_double_slash(add_schema(dst));
	if (dest[dest.size()-1] == '/') {
		dest = dest.substr(0, dest.size()-1);
	}

	bool is_exist = false;
	if(exist(dest.c_str()) == true) {
		hdfsFileInfo * f_info = hdfsGetPathInfo(this->connection, dest.c_str());
		if (f_info != NULL and f_info->mKind == kObjectKindDirectory) {
			hdfsFreeFileInfo(f_info, 1);
			dest += "/";
			dest += std::string(basename(const_cast<char*>(src)));

			int cnt = 0;
			hdfsFileInfo* fs = hdfsListDirectory(this->connection, dest.c_str(), &cnt);
			for (int i = 0; i < cnt; i++) {
				if (strcmp(fs[i].mName, dest.c_str()) == 0) {
					is_exist = true;
				}
			}
			hdfsFreeFileInfo(fs, cnt);

		} else {
			is_exist = true;
		}
	}
	if (is_exist == true) {
		rm(dest.c_str());
	}
	return put(src, dst);
}

int HDFS_FILE::rename(const char* src, const char* dst) {
	return this->mv(src, dst);
}

int HDFS_FILE::rm(const char* path) {
	check(path != NULL and strlen(path) > 0 and this->connection != NULL);
	check(strcmp(path, "/") != 0); /* weak */
	int  recursive = 1;
	return hdfsDelete(this->connection, path, recursive);
}

int HDFS_FILE::mkdir(const char* path) {
	check(path != NULL and strlen(path) > 0 and this->connection != NULL);
	return hdfsCreateDirectory(this->connection, path);
}

hdfsFileInfo* HDFS_FILE::ls(const char* path, int* cnt) {
	check(path != NULL and strlen(path) > 0 and this->connection != NULL);
	return hdfsListDirectory(this->connection, path, cnt);
}

int HDFS_FILE::chmod(const char* path, short mode) {
	check(path != NULL and strlen(path) > 0 and this->connection != NULL);
	return hdfsChmod(this->connection, path, mode);
}

int HDFS_FILE::chown(const char* path, const char* owner, const char* group) {
	check(path != NULL and strlen(path) > 0 and this->connection != NULL);
	check(owner != NULL and strlen(owner) > 0 and group != NULL and strlen(group) > 0);
	return hdfsChown(this->connection, path, owner, group);
}

int HDFS_FILE::getmerge(const char *src, const char *dst) {
	check(src != NULL and dst != NULL and this->connection != NULL);
	check(strcmp(src, dst) != 0);

	std::string source = remove_double_slash(add_schema(src));

	if (source[source.size()-1] == '/') {
		source = source.substr(0, source.size()-1);
	}

	if(exist(source.c_str()) == false) {
		error("%s:%s\n", src, "File Not Found !");
		return errno;
	}

	FILE* local_f = fopen(dst, "w");
	if (local_f == NULL) {
		error("%s:%s\n", dst, strerror(errno));
		return errno;
	}
	int part_cnt = 0;
	char buffer[20480];
	size_t cnt;
	tSize bytes;
	hdfsFile part_f;
	bool is_exist_part = false;
	hdfsFileInfo* fs = hdfsListDirectory(this->connection, source.c_str(), &part_cnt);
	if (part_cnt == 0) {
		error("%s:%s\n", src, "Directory is empty !"); 
		return -1;
	}
	for(int i = 0; i < part_cnt; i++) {
		if (strcmp((source+"/_SUCCESS").c_str(), fs[i].mName) == 0) {
			continue;
		}

		hdfsFileInfo * f_info = hdfsGetPathInfo(this->connection, fs[i].mName);
		if (f_info != NULL and f_info->mKind == kObjectKindDirectory) {
			hdfsFreeFileInfo(f_info, 1);
			continue;
		}
		hdfsFreeFileInfo(f_info, 1);
		is_exist_part = true;

		part_f = hdfsOpenFile(this->connection, fs[i].mName, O_RDONLY, 0, 0, 0);

		if (part_f == NULL) {
			error(strerror(errno));
		}

		bytes = hdfsRead(this->connection, part_f, buffer, sizeof(buffer));
		cnt = static_cast<size_t>(bytes);

		while (cnt != 0) {

			fwrite(buffer, sizeof(char), cnt, local_f);

			bytes = hdfsRead(this->connection, part_f, buffer, sizeof(buffer));
			cnt = static_cast<size_t>(bytes);
		}

		check(part_f != NULL);

		if (hdfsCloseFile(this->connection, part_f) == -1) {
			error(strerror(errno));
		}
	}
	if (is_exist_part == false) {
		error("%s:%s\n", src, "Not found any data to be merged in directory !");
		return -1;
	}
	hdfsFreeFileInfo(fs, part_cnt);
	fclose(local_f);

	return 0;
}

hdfsFileInfo* HDFS_FILE::dirinfo(const char* path) {
	check(path != NULL and strlen(path) > 0 and this->connection != NULL);
	if (exist(path)) {
		return hdfsGetPathInfo(this->connection,remove_double_slash(add_schema(path)).c_str());
	}
	error("%s:%s\n", path, "Not Found");
	return NULL;
}

#ifdef __cplusplus
}
#endif

