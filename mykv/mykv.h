#ifndef MYBITCASK_MYKV_H_
#define MYBITCASK_MYKV_H_

#include "mybitcask/mybitcask.h"

#include <iostream>

std::ostream& error();

void RunREPL(mybitcask::MyBitcask* db);

#endif  // MYBITCASK_MYKV_H_
