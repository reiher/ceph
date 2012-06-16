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
#include <iostream>
#include <string>
#include <sstream>

#include "os/LevelDBStore.h"

int main(int argc, char *argv[])
{
	if (argc < 3) {
		std::cerr << "Usage: " << argv[0] << " <store path> <prefix>" << std::endl;
		return 1;
	}

	string path(argv[1]);
	string prefix(argv[2]);
	std::cout << "path: " << path << " ; prefix: " << prefix << std::endl;

	LevelDBStore ldb(path);
        assert(!ldb.init(std::cerr));

	KeyValueDB::Iterator it = ldb.get_iterator(prefix);
	it->seek_to_first();
	while (it->valid()) {
		std::cout << "key = " << it->key() << std::endl;
		ostringstream os;
		it->value().hexdump(os);
		string hex = os.str();
		std::cout << hex << std::endl;
		std::cout << "--------------------------------------------" << std::endl;

		it->next();
	}

	return 0;
}
