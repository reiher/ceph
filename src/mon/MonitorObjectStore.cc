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
#include <stdio.h>
#include <string.h>
#include <iostream>
#include <assert.h>
#include <time.h>
#include <stdlib.h>
#include <signal.h>
#include "os/FileStore.h"
#include "common/debug.h"

#include "MonitorObjectStore.h"

#define dout_subsys ceph_subsys_mon
#undef dout_prefix
#define dout_prefix _prefix(_dout, dir)
static ostream& _prefix(std::ostream *_dout, const string& dir)
{
  return *_dout << "store(" << dir << ") ";
}

const string MonitorObjectStore::DEFAULT_DIR("default");
const coll_t MonitorObjectStore::DEFAULT_COLL(DEFAULT_DIR);

coll_t MonitorObjectStore::_get_coll(ObjectStore::Transaction *t, string dir)
{
  coll_t coll((dir.empty() ? DEFAULT_DIR : dir));

  /* This is error prone. Better deal with the missing collections than dealing
   * with EEXISTS all over the place!
   *
  if (!collection_exists(coll)) {
    t->create_collection(coll);
  }
  */

  return coll;
}

coll_t MonitorObjectStore::_get_coll(string dir)
{
  ObjectStore::Transaction t;
  coll_t coll = _get_coll(&t, dir);

  if (!t.empty())
    apply_transaction(t);

  return coll;
}

void MonitorObjectStore::put(ObjectStore::Transaction *t,
			     coll_t coll, hobject_t obj, bufferlist& bl)
{
  t->write(coll, obj, 0, bl.length(), bl);
}

void MonitorObjectStore::put(ObjectStore::Transaction *t, 
			    string dir, string name, bufferlist& bl)
{
  coll_t coll = _get_coll(t, dir);
  hobject_t obj(sobject_t(name, CEPH_NOSNAP));
  put(t, coll, obj, bl);
}

void MonitorObjectStore::put(ObjectStore::Transaction *t,
			     string dir, version_t ver, bufferlist& bl)
{
  stringstream ss;
  ss << ver;
  put(t, dir, ss.str(), bl);
}

void MonitorObjectStore::put(ObjectStore::Transaction *t,
			     string dir, string name, version_t ver)
{
  bufferlist bl;
  ::encode(ver, bl);
  put(t, dir, name, bl);
}

int MonitorObjectStore::put(string dir, string name, bufferlist& bl)
{

  ObjectStore::Transaction t;
  put(&t, dir, name, bl);
  apply_transaction(t);

  return 0;
}

int MonitorObjectStore::put(string dir, version_t ver, bufferlist& bl)
{
  ObjectStore::Transaction t;
  put(&t, dir, ver, bl);
  apply_transaction(t);
  return 0;
}

int MonitorObjectStore::put(string dir, string name, version_t ver)
{
  ObjectStore::Transaction t;
  put(&t, dir, name, ver);
  apply_transaction(t);
  return 0;
}

void MonitorObjectStore::put(ObjectStore::Transaction *t, string dir,
			    map<version_t,bufferlist>::iterator start,
			    map<version_t,bufferlist>::iterator end)
{
  map<version_t,bufferlist>::iterator p;
  for (p = start; p != end; ++p) {
    put(t, dir, p->first, p->second);
  }
}

int MonitorObjectStore::put(string dir, 
			    map<version_t,bufferlist>::iterator start,
			    map<version_t,bufferlist>::iterator end)
{
  ObjectStore::Transaction t;
  put(&t, dir, start, end);
  apply_transaction(t);

  return 0;
}

int MonitorObjectStore::append(string dir, string name, bufferlist& bl)
{
  coll_t coll = _get_coll(dir);
  hobject_t obj(sobject_t(name, CEPH_NOSNAP));

  struct stat st;
  int err = stat(coll, obj, &st);
  if (err < 0) {
    dout(5) << __func__ << " err = " << err << " stating "
	    << coll << "/" << obj << dendl;
    return err;
  }

  ObjectStore::Transaction t;
  t.write(coll, obj, st.st_size, bl.length(), bl);
  apply_transaction(t);

  return 0;
}

void MonitorObjectStore::erase(ObjectStore::Transaction *t,
			       string dir, string name)
{
  coll_t coll(dir);
  hobject_t obj(sobject_t(name, CEPH_NOSNAP));
  t->remove(coll, obj);
}

void MonitorObjectStore::erase(ObjectStore::Transaction *t,
			       string dir, version_t ver)
{
  stringstream ss;
  ss << ver;
  erase(t, dir, ss.str());
}

int MonitorObjectStore::erase(string dir, string name)
{
  ObjectStore::Transaction t;
  erase(&t, dir, name);
  apply_transaction(t);

  return 0;
}

int MonitorObjectStore::erase(string dir, version_t ver)
{
  ObjectStore::Transaction t;
  erase(&t, dir, ver);
  apply_transaction(t);
  return 0;
}


int MonitorObjectStore::get(string dir, string name, bufferlist& bl)
{
  coll_t coll = _get_coll(dir);
  hobject_t obj(sobject_t(name, CEPH_NOSNAP));

  struct stat st;
  int err = stat(coll, obj, &st);
  if (err < 0) {
    dout(5) << __func__ << " error stating " << coll << "/" << obj << dendl;
    return err;
  }

  int len = read(coll, obj, 0, st.st_size, bl);
  if (len < 0) {
    dout(5) << __func__ << " error (" << len << ") reading " 
	    << coll << "/" << obj << " (0~" << st.st_size << ")" << dendl;
  }
  return len;
}

int MonitorObjectStore::get(string dir, version_t ver, bufferlist& bl)
{
  stringstream ss;
  ss << ver;
  return get(dir, ss.str(), bl);
}

version_t MonitorObjectStore::get(string dir, string name)
{
  bufferlist bl;
  int err = get(dir, name, bl);
  if (err < 0) {
    // non-existent files are treated as containing zero
    if (errno == ENOENT)
      return 0;
  }
  assert(err >= 0);

  version_t v;
  ::decode(v, bl);
  return v;
}

bool MonitorObjectStore::exists(string dir, string name)
{
  coll_t coll((dir.empty() ? DEFAULT_DIR : dir));
  if (!collection_exists(coll))
    return false;

  hobject_t obj(sobject_t(name, CEPH_NOSNAP));
  return FileStore::exists(coll, obj);
}

bool MonitorObjectStore::exists(string dir, version_t ver)
{
  stringstream ss;
  ss << ver;
  hobject_t obj(sobject_t(ss.str(), CEPH_NOSNAP));
  return FileStore::exists(coll_t(dir), obj);
}
