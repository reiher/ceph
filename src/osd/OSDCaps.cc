// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2009-2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "OSDCaps.h"
#include "common/config.h"
#include "common/debug.h"

CapMap::~CapMap()
{
}

void ObjectCapMap::apply_caps(const string& oid, int& cap) const
{
  map<string, OSDCap>::const_iterator iter;
  for (iter = object_prefix_map.begin();
      iter != object_prefix_map.end();
      ++iter) {
    const string prefix = iter->first;
    const OSDCap& c = iter->second;
    if ((oid.compare(0, prefix.length(), prefix)) == 0) {
      cap |= c.allow;
      cap &= ~c.deny;
      break;
    }
  }
}

void PoolsMap::dump() const
{
  map<string, std::pair<OSDCap, ObjectCapMap*> >::const_iterator it;
  for (it = pools_map.begin(); it != pools_map.end(); ++it) {
    generic_dout(0) << it->first << " -> (" << (int)it->second.first.allow
                    << "." << (int)it->second.first.deny << ")" << dendl;
  }
}

ObjectCapMap& PoolsMap::get_ocap(const string& name)
{
  ObjectCapMap *existing = pools_map[name].second;
  if (!existing) {
    existing = new ObjectCapMap();
    pools_map[name].second = existing;
  }
  return *existing;
}

void PoolsMap::apply_pool_caps(const string& name, int& cap) const
{
  map<string, std::pair<OSDCap, ObjectCapMap*> >::const_iterator iter;

  if ((iter = pools_map.find(name)) != pools_map.end()) {
    const OSDCap& c = iter->second.first;
    cap |= c.allow;
    cap &= ~c.deny;
  }
}

PoolsMap::~PoolsMap()
{
  // we just need to clean up the ObjectCapMaps we created
  map<string, std::pair<OSDCap, ObjectCapMap* > >::iterator iter;
  for (iter = pools_map.begin(); iter != pools_map.end(); ++iter) {
    if (iter->second.second) delete iter->second.second;
  }
}

void AuidMap::apply_caps(const uint64_t uid, int& cap) const
{
  map<uint64_t, OSDCap>::const_iterator iter;

  if ((iter = auid_map.find(uid)) != auid_map.end()) {
    const OSDCap& auid_cap = iter->second;
    cap |= auid_cap.allow;
    cap &= ~auid_cap.deny;
  }
}

bool OSDCaps::get_next_token(const string s, size_t& pos, string& token) const
{
  int start = s.find_first_not_of(" \t", pos);
  int end;

  if (start < 0) {
    return false; 
  }

  if (s[start] == '=' || s[start] == ',' || s[start] == ';') {
    end = start + 1;
  } else {
    end = s.find_first_of(";,= \t", start+1);
  }

  if (end < 0) {
    end=s.size();
  }

  token = s.substr(start, end - start);

  pos = end;

  return true;
}

bool OSDCaps::is_rwx(string& token, rwx_t& cap_val) const
{
  const char *t = token.c_str();
  int val = 0;

  while (*t) {
    switch (*t) {
    case 'r':
      val |= OSD_CAP_R;
      break;
    case 'w':
      val |= OSD_CAP_W;
      break;
    case 'x':
      val |= OSD_CAP_X;
      break;
    default:
      return false;
    }
    t++;
  }

  cap_val = val;
  return true;
}

bool OSDCaps::parse(bufferlist::iterator& iter)
{
  string s;

  try {
    ::decode(s, iter);

    generic_dout(10) << "decoded caps: " << s << dendl;

    size_t pos = 0;
    string token;
    bool init = true;

    bool op_allow = false;
    bool op_deny = false;
    bool cmd_pool = false;
    bool cmd_uid = false;
    bool any_cmd = false;
    bool got_eq = false;
    bool prefix = false;
    list<string> name_list;
    list<string> prefix_list;
    bool last_is_comma = false;
    rwx_t cap_val = 0;

    while (pos < s.size()) {
      if (init) {
        op_allow = false;
        op_deny = false;
        cmd_pool = false;
	cmd_uid = false;
        any_cmd = false;
        got_eq = false;
        last_is_comma = false;
        cap_val = 0;
        init = false;
        name_list.clear();
      }

#define ASSERT_STATE(x) \
do { \
  if (!(x)) { \
       derr << "error parsing caps at pos=" << pos << " (" #x ")" << dendl; \
  } \
} while (0)

      if (get_next_token(s, pos, token)) {
	if (token.compare("*") == 0) { //allow all operations 
	  ASSERT_STATE(op_allow);
	  allow_all = true;
	} else if (token.compare("=") == 0) {
          ASSERT_STATE(any_cmd);
          got_eq = true;
        } else if (token.compare("allow") == 0) {
          ASSERT_STATE((!op_allow) && (!op_deny));
          op_allow = true;
        } else if (token.compare("deny") == 0) {
          ASSERT_STATE((!op_allow) && (!op_deny));
          op_deny = true;
        } else if ((token.compare("pools") == 0) ||
                   (token.compare("pool") == 0)) {
          ASSERT_STATE(!cmd_uid && (op_allow || op_deny));
          cmd_pool = true;
          any_cmd = true;
	} else if (token.compare("uid") == 0) {
	  ASSERT_STATE(!cmd_pool && (op_allow || op_deny));
	  any_cmd = true;
	  cmd_uid = true;
        } else if (is_rwx(token, cap_val)) {
          ASSERT_STATE(op_allow || op_deny);
        } else if (token.compare("prefix") == 0) {
          ASSERT_STATE(cmd_pool);
          prefix = true;
        } else if (token.compare(";") != 0) {
	  ASSERT_STATE(got_eq);
          if (token.compare(",") == 0) {
            ASSERT_STATE(!last_is_comma);
	    last_is_comma = true;
          } else {
            last_is_comma = false;
            if (prefix) {
              prefix_list.push_back(token);
            } else {
              name_list.push_back(token);
            }
          }
        }

	if (token.compare(";") == 0 || pos >= s.size()) {
	  if (got_eq) {
	    ASSERT_STATE(name_list.size() > 0);
	    if (prefix) {
	      for (list<string>::iterator pool_iter = name_list.begin();
	          pool_iter != name_list.end();
	          ++pool_iter) {
	        ObjectCapMap& omap = pools_map.get_ocap(*pool_iter);
	        for (list<string>::iterator prefix_iter = prefix_list.begin();
	            prefix_iter != prefix_list.end();
	            ++prefix_iter) {
	          OSDCap& prefix_cap = omap.get_cap(*prefix_iter);
	          if (op_allow) {
	            prefix_cap.allow |= cap_val;
	          } else {
	            prefix_cap.deny |= cap_val;
	          }
	        }
	      }
	    } else {
	      CapMap *working_map = &pools_map;
	      if (cmd_uid)
	        working_map = &auid_map;
	      for (list<string>::iterator iter2 = name_list.begin();
	          iter2 != name_list.end();
	          ++iter2) {
	        OSDCap& cap = working_map->get_cap(*iter2);
	        if (op_allow) {
	          cap.allow |= cap_val;
	        } else {
	          cap.deny |= cap_val;
	        }
	      }
	    }
          } else {
            if (op_allow) {
              default_allow |= cap_val;
            } else {
              default_deny |= cap_val;
            }
          }
          init = true;
        }
        
      }
    }
  } catch (const buffer::error &err) {
    return false;
  }

  generic_dout(10) << "default allow=" << (int)default_allow << " default_deny=" << (int) default_deny << dendl;
  pools_map.dump();
  return true;
}

/**
 * Get the caps given this OSDCaps object for a given pool id
 * and uid (the pool's owner).
 *
 * Basic strategy: chain of permissions: default allow -> auid
 * -> pool -> default_deny.
 *
 * Starting with default allowed caps. Next check if you have caps on
 * the auid and apply, then apply any caps granted on the pool
 * (this lets users give out caps to their auid but keep one pool
 * private, for instance).
 * If these two steps haven't given you explicit caps
 * on the pool, check if you're the pool owner and grant full.
 */
int OSDCaps::get_pool_cap(const string& pool_name,
                          uint64_t uid) const
{
  if (allow_all)
    return OSD_CAP_ALL;

  int explicit_cap = default_allow; //explicitly granted caps
  
  //if the owner is granted permissions on the pool owner's auid, grant them
  auid_map.apply_caps(uid, explicit_cap);

  //check for explicitly granted caps and apply if needed
  pools_map.apply_pool_caps(pool_name, explicit_cap);

  //owner gets full perms by default:
  if (uid == auid
      && explicit_cap == 0) {
    explicit_cap = OSD_CAP_ALL;
  }

  explicit_cap &= ~default_deny;

  return explicit_cap;
}

/**
 * Get the object cap for the given object, pool, and (object) uid.
 *
 * This grabs the pool cap for the pool and uid, then sticks any
 * prefix-based object caps that might exist on top of that.
 */
int OSDCaps::get_object_cap(const string& object_name,
                                  const string& pool_name,
                                  uint64_t uid) const
{
  int cap = get_pool_cap(pool_name, uid);
  ObjectCapMap *capmap = pools_map.get_ocap_pointer(pool_name);
  if (capmap) {
    capmap->apply_caps(object_name, cap);
  }
  return cap;
}
