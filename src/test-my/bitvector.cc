/*
 * bitvector.cc
 *
 *  Created on: Jan 11, 2018
 *      Author: runsisi
 */

#include "common/bit_vector.hpp"
#include <time.h>
#include <iostream>
using namespace std;

int main ()
{
  time_t start,end;
  double seconds;

  time(&start);  /* get current time; same as: timer = time(NULL)  */
  unsigned int size=10000;

  typedef BitVector<2> bv_t;

  bv_t bv;

  bv.resize(10000);

  for (int i = 0; i < bv.size(); i++)
  {
     if (bv[i] == 1) continue;
  }

  time(&end);

  seconds = difftime(start, end);

  printf("%.f seconds", seconds);

  return 0;
}


// build command line:
// g++ -o bv bv.cc -I.. -I../../build/include -std=c++17 -L../../build/lib -lceph-common

// run command line:
// LD_LIBRARY_PATH=../../build/lib ./bv
