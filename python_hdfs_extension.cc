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

#ifdef __cplus_cplus
extern "C" {
#endif

#include <python2.7/Python.h>
#include <stdio.h>
#include "hadoop_fs.h"
#include "log.h"

static HDFS_FILE hdfs;

static PyObject *open(PyObject *self, PyObject *args) {
	char* fname = NULL;
	char* mode = NULL;
	if (PyArg_ParseTuple(args, "ss", &fname, &mode) == 0) {
		return NULL;
	}

	int ok = hdfs.open(fname, mode);
	return Py_BuildValue("i", ok);
}



static PyObject *readline(PyObject *self, PyObject *args) {
	char* buff = hdfs.getline();
	PyObject *line = NULL;
	if (buff) {
		line = Py_BuildValue("s", buff);
		free(buff);
	} else {
		line = Py_BuildValue("s", "");
	}
	return line;
}


static PyObject *writeline(PyObject *self, PyObject *args) {
	char* line = NULL;
	if (PyArg_ParseTuple(args, "s", &line) == 0) {
		return NULL;
	}
	size_t nwrite = hdfs.write(line);

	return Py_BuildValue("i", nwrite);
}


static PyObject *close(PyObject *self, PyObject *args) {
	hdfs.close();
	return Py_BuildValue("i", 0);
}

static PyObject *ls(PyObject *self, PyObject *args) {
	char* path = NULL;
	if (PyArg_ParseTuple(args, "s", &path) == 0) {
		return NULL;
	}
	int cnt = 0;
	hdfsFileInfo* fs = hdfs.ls(path, &cnt);

	PyObject* list = PyList_New(cnt);
	for(int i = 0; i < cnt; i++) {
		PyList_SetItem(list, i, Py_BuildValue("s", fs[i].mName));
	}

	hdfsFreeFileInfo(fs, cnt);
	return list;
}

static PyObject *mv(PyObject *self, PyObject *args) {
	char* src = NULL;
	char* dst = NULL;
	if (PyArg_ParseTuple(args, "ss", &src, &dst) == 0) {
		return NULL;
	}
	return Py_BuildValue("i", hdfs.mv(src, dst));
}

static PyObject *put(PyObject *self, PyObject *args) {
	char* src = NULL;
	char* dst = NULL;
	if (PyArg_ParseTuple(args, "ss", &src, &dst) == 0) {
		return NULL;
	}
	return Py_BuildValue("i", hdfs.put(src, dst));
}

static PyObject *putf(PyObject *self, PyObject *args) {
	char* src = NULL;
	char* dst = NULL;
	if (PyArg_ParseTuple(args, "ss", &src, &dst) == 0) {
		return NULL;
	}
	return Py_BuildValue("i", hdfs.putf(src, dst));
}

static PyObject *rm(PyObject *self, PyObject *args) {
	char* path = NULL;
	if (PyArg_ParseTuple(args, "s", &path) == 0) {
		return NULL;
	}
	return Py_BuildValue("i", hdfs.rm(path));
}

static PyObject *exist(PyObject *self, PyObject *args) {
	char* path = NULL;
	if (PyArg_ParseTuple(args, "s", &path) == 0) {
		return NULL;
	}
	if (hdfs.exist(path)) {
		return Py_BuildValue("O", Py_True);
	}
	return Py_BuildValue("O", Py_False);
}


static PyObject *chown(PyObject *self, PyObject *args) {
	char* path = NULL;
	char* owner = NULL;
	char* group = NULL;
	if (PyArg_ParseTuple(args, "sss", &path, &owner, &group) == 0) {
		return NULL;
	}
	return Py_BuildValue("i", hdfs.chown(path, owner, group));
}

static PyObject *chmod(PyObject *self, PyObject *args) {
	char* path = NULL;
	int mode = 0;
	if (PyArg_ParseTuple(args, "si", &path, &mode) == 0) {
		return NULL;
	}
	return Py_BuildValue("i", hdfs.chmod(path, mode));
}

static PyObject *mkdir(PyObject *self, PyObject *args) {
	char* path = NULL;
	if (PyArg_ParseTuple(args, "s", &path) == 0) {
		return NULL;
	}
	return Py_BuildValue("i", hdfs.mkdir(path));
}

static PyObject *getmerge(PyObject *self, PyObject *args) {
	char* src = NULL;
	char* dst = NULL;
	if (PyArg_ParseTuple(args, "ss", &src, &dst) == 0) {
		return NULL;
	}
	return Py_BuildValue("i", hdfs.getmerge(src, dst));
}

static PyObject *dirinfo(PyObject *self, PyObject *args) {
	char* path = NULL;
	if (PyArg_ParseTuple(args, "s", &path) == 0) {
		return NULL;
	}
	hdfsFileInfo* fs = hdfs.dirinfo(path);
	if (fs == NULL) {
		return Py_BuildValue("()");
	} else {
		PyObject *tuple = PyTuple_New(2);
		PyTuple_SetItem(tuple, 0, Py_BuildValue("s", fs->mName));
		PyTuple_SetItem(tuple, 1, Py_BuildValue("l", fs->mLastMod));
		hdfsFreeFileInfo(fs, 1);
		return tuple;
	}
}



static PyMethodDef ExtestMethods[] = {
	{"ls",         ls,         METH_VARARGS, "ls(path)                  list contents of <path>, python-list returned"},
	{"mv",         mv,         METH_VARARGS, "mv(old, new)              move path from <old> to <new>, 0/errorno returned"},
	{"rm",         rm,         METH_VARARGS, "rm(path)                  rm -r <path>, 0/errorno returned"},
	{"put",        put,        METH_VARARGS, "put(local, remote)        upload file to hdfs, 0/errorno returned"},
	{"putf",       putf,       METH_VARARGS, "putf(local, remote)       force upload file to hdfs, 0/errorno returned"},
	{"mkdir",      mkdir,      METH_VARARGS, "mkdir(path)               mkdir of path, 0/errorno returned"},
	{"chmod",      chmod,      METH_VARARGS, "chmod(path, mode)         mode must be int like 655,644, 0/errorno returned"},
	{"chown",      chown,      METH_VARARGS, "chown(path, owner, group) all parameters should be string, 0/errorno returned"},
	{"exist",      exist,      METH_VARARGS, "exist(path)               whether <path> exists, True/False returned"},
	{"open",       open,       METH_VARARGS, "open(path, mode)          mode should be 'r' or 'w', 0/errorno returned, remeber to call close() at the end"},
	{"close",      close,      METH_VARARGS, "close()                   close hdfsFile which is opened by the last open(path, mode) call"},
	{"writeline",  writeline,  METH_VARARGS, "writeline(line)           line should contains '\\r\\n' or '\\n', ex: writeline('something\\n')"},
	{"readline",   readline,   METH_VARARGS, "readline()                return a line from the file last opend by open(path, mode)"},
	{"getmerge",   getmerge,   METH_VARARGS, "getmerge(remote, local)   merge hdfs file to local, 0/errorno returned"},
	{"dirinfo",    dirinfo,    METH_VARARGS, "dirinfo(path)             return the name, lastmodifytime of the path"},
	{NULL, NULL, 0, NULL},
};

PyMODINIT_FUNC initawesome_hdfs() {
	log_init("", LOG_CONSOLE);
	Py_InitModule("awesome_hdfs", ExtestMethods);
	hdfs.init(HOST, PORT);
}


#ifdef __cplus_cplus
}
#endif


