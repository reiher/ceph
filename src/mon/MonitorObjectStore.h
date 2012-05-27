// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
* Ceph - scalable distributed file system
*
* Copyright (C) 2012 Inktank, Inc.
*
* This is free software; you can redistribute it and/or
* modify it under the terms of the GNU Lesser General Public
* License version 2.1, as published by the Free Software
* Foundation. See file COPYING.
*/
#ifndef CEPH_MONITOR_OBJECT_STORE_H
#define CEPH_MONITOR_OBJECT_STORE_H

#include <stdio.h>
#include <string.h>
#include <iostream>
#include <assert.h>
#include <time.h>
#include <stdlib.h>
#include <signal.h>
#include "os/FileStore.h"

// If Paxos happens to need much more direct access to the FileStore, then we
// should just s/protected/public/ on the next line.
class MonitorObjectStore : public FileStore
{
 public:
  const static string DEFAULT_DIR;
  const static coll_t DEFAULT_COLL;

 public:
  MonitorObjectStore(const std::string& base, const std::string& jdev) 
    : FileStore(base, jdev) { }
  ~MonitorObjectStore() { }

  /**
   * @defgroup MonitorObjectStore_h_funcs Interface functions
   * @{
   */
  /**
   * @defgroup MonitorObjectStore_h_funcs_write Write
   * @{
   */
  void put(ObjectStore::Transaction *t,
	  coll_t coll, hobject_t obj, bufferlist& bl);
  void put(ObjectStore::Transaction *t,
	   string dir, string name, bufferlist& bl);
  void put(ObjectStore::Transaction *t,
	   string dir, version_t ver, bufferlist& bl);
  void put(ObjectStore::Transaction *t,
	   string dir, string name, version_t ver);

  int put(string dir, string name, bufferlist& bl);
  int put(string dir, version_t ver, bufferlist& bl);
  int put(string dir, string name, version_t ver);

  void put(ObjectStore::Transaction *t, string dir,
	   map<version_t,bufferlist>::iterator start,
	   map<version_t,bufferlist>::iterator end);

  int put(string dir, map<version_t,bufferlist>::iterator start,
	  map<version_t,bufferlist>::iterator end);

  int append(string dir, string name, bufferlist& bl);

  void erase(ObjectStore::Transaction *t, string dir, string name);
  void erase(ObjectStore::Transaction *t, string dir, version_t ver);

  int erase(string dir, string name);
  int erase(string dir, version_t ver);

  /**
   * @}
   */
  /**
   * @defgroup MonitorObjectStore_h_funcs_read Read
   * @{
   */
  int get(string dir, string name, bufferlist& bl);
  int get(string dir, version_t ver, bufferlist& bl);
  version_t get(string dir, string name);
  bool exists(string dir, string name);
  bool exists(string dir, version_t ver);
  /**
   * @}
   */
  /**
   * @}
   */

 private:
  coll_t _get_coll(string dir);
  coll_t _get_coll(ObjectStore::Transaction *t, string dir);
};

#endif /* CEPH_MONITOR_OBJECT_STORE_H */
