/*
 * objectkey.cc
 *
 *  Created on: Mar 27, 2017
 *      Author: runsisi
 */

#define CEPH_NOSNAP -2
#define CEPH_SNAPDIR -1
#include "object.h"
#include "common/hobject.h"
#include <iostream>
#include <functional>

const shard_id_t shard_id_t::NO_SHARD(-1);

string header_key(uint64_t seq)
{
  char buf[100];
  snprintf(buf, sizeof(buf), "%.*" PRId64, (int)(2*sizeof(seq)), seq);
  return string(buf);
}

#define mix(a, b, c)                                            \
        do {                                                    \
                a = a - b;  a = a - c;  a = a ^ (c >> 13);      \
                b = b - c;  b = b - a;  b = b ^ (a << 8);       \
                c = c - a;  c = c - b;  c = c ^ (b >> 13);      \
                a = a - b;  a = a - c;  a = a ^ (c >> 12);      \
                b = b - c;  b = b - a;  b = b ^ (a << 16);      \
                c = c - a;  c = c - b;  c = c ^ (b >> 5);       \
                a = a - b;  a = a - c;  a = a ^ (c >> 3);       \
                b = b - c;  b = b - a;  b = b ^ (a << 10);      \
                c = c - a;  c = c - b;  c = c ^ (b >> 15);      \
        } while (0)

unsigned ceph_str_hash_rjenkins(const char *str, unsigned length)
{
        const unsigned char *k = (const unsigned char *)str;
        __u32 a, b, c;  /* the internal state */
        __u32 len;      /* how many key bytes still need mixing */

        /* Set up the internal state */
        len = length;
        a = 0x9e3779b9;      /* the golden ratio; an arbitrary value */
        b = a;
        c = 0;               /* variable initialization of internal state */

        /* handle most of the key */
        while (len >= 12) {
                a = a + (k[0] + ((__u32)k[1] << 8) + ((__u32)k[2] << 16) +
                         ((__u32)k[3] << 24));
                b = b + (k[4] + ((__u32)k[5] << 8) + ((__u32)k[6] << 16) +
                         ((__u32)k[7] << 24));
                c = c + (k[8] + ((__u32)k[9] << 8) + ((__u32)k[10] << 16) +
                         ((__u32)k[11] << 24));
                mix(a, b, c);
                k = k + 12;
                len = len - 12;
        }

        /* handle the last 11 bytes */
        c = c + length;
        switch (len) {            /* all the case statements fall through */
        case 11:
                c = c + ((__u32)k[10] << 24);
        case 10:
                c = c + ((__u32)k[9] << 16);
        case 9:
                c = c + ((__u32)k[8] << 8);
                /* the first byte of c is reserved for the length */
        case 8:
                b = b + ((__u32)k[7] << 24);
        case 7:
                b = b + ((__u32)k[6] << 16);
        case 6:
                b = b + ((__u32)k[5] << 8);
        case 5:
                b = b + k[4];
        case 4:
                a = a + ((__u32)k[3] << 24);
        case 3:
                a = a + ((__u32)k[2] << 16);
        case 2:
                a = a + ((__u32)k[1] << 8);
        case 1:
                a = a + k[0];
                /* case 0: nothing left to add */
        }
        mix(a, b, c);

        return c;
}

unsigned ceph_str_hash_linux(const char *str, unsigned length)
{
        unsigned hash = 0;

        while (length--) {
                unsigned char c = *str++;
                hash = (hash + (c << 4) + (c >> 4)) * 11;
        }
        return hash;
}

static void append_escaped(const string &in, string *out)
{
  for (string::const_iterator i = in.begin(); i != in.end(); ++i) {
    if (*i == '%') {
      out->push_back('%');
      out->push_back('p');
    } else if (*i == '.') {
      out->push_back('%');
      out->push_back('e');
    } else if (*i == '_') {
      out->push_back('%');
      out->push_back('u');
    } else {
      out->push_back(*i);
    }
  }
}

string ghobject_key(const ghobject_t &oid)
{
  string out;
  append_escaped(oid.hobj.oid.name, &out);
  out.push_back('.');
  append_escaped(oid.hobj.get_key(), &out);
  out.push_back('.');
  append_escaped(oid.hobj.nspace, &out);
  out.push_back('.');

  char snap_with_hash[1000];
  char *t = snap_with_hash;
  char *end = t + sizeof(snap_with_hash);
  if (oid.hobj.snap == -2)
    t += snprintf(t, end - t, "head");
  else if (oid.hobj.snap == -1)
    t += snprintf(t, end - t, "snapdir");
  else
    t += snprintf(t, end - t, "%llx", (long long unsigned)oid.hobj.snap);

  if (oid.hobj.pool == -1)
    t += snprintf(t, end - t, ".none");
  else
    t += snprintf(t, end - t, ".%llx", (long long unsigned)oid.hobj.pool);
  t += snprintf(t, end - t, ".%.*X", (int)(sizeof(uint32_t)*2), oid.hobj.get_hash());

  if (oid.generation != ghobject_t::NO_GEN ||
      oid.shard_id != shard_id_t::NO_SHARD) {
    t += snprintf(t, end - t, ".%llx", (long long unsigned)oid.generation);
    t += snprintf(t, end - t, ".%x", (int)oid.shard_id);
  }
  out += string(snap_with_hash);
  return out;
}

set<string> get_prefixes(
  uint32_t bits,
  uint32_t mask,
  int64_t pool)
{
  uint32_t len = bits;
  while (len % 4 /* nibbles */) len++;

  set<uint32_t> from;
  if (bits < 32)
    from.insert(mask & ~((uint32_t)(~0) << bits));
  else if (bits == 32)
    from.insert(mask);
  else
    ceph_abort();


  set<uint32_t> to;
  for (uint32_t i = bits; i < len; ++i) {
    for (set<uint32_t>::iterator j = from.begin();
         j != from.end();
         ++j) {
      to.insert(*j | (1U << i));
      to.insert(*j);
    }

    to.swap(from);
    to.clear();
  }

  char buf[20];
  char *t = buf;
  uint64_t poolid(pool);
  t += snprintf(t, sizeof(buf), "%.*llX", 16, (long long unsigned)poolid);
  *(t++) = '.';
  string poolstr(buf, t - buf);
  set<string> ret;
  for (set<uint32_t>::iterator i = from.begin();
       i != from.end();
       ++i) {
    uint32_t revhash(hobject_t::_reverse_nibbles(*i));

    snprintf(buf, sizeof(buf), "%.*X", (int)(sizeof(revhash))*2, revhash);

    ret.insert(poolstr + string(buf, len/4));
  }
  return ret;
}

int main() {
  ghobject_t o(hobject_t("snapmapper", "", 0, 0xA468EC03,
      -1, ""));
  std::cout << ghobject_key(o) << std::endl;

  unsigned so_hash = std::hash<sobject_t>()(sobject_t("snapmapper", 0));

  char t[100];
  snprintf(t, 100, ".%.*X", (int)(sizeof(uint32_t)*2), so_hash);
  std::cout << t << std::endl;

  std::cout << header_key(83) << std::endl;

  snapid_t snap = 42;
  int len = snprintf(
      t, sizeof(t),
      "%.*X_", (int)(sizeof(snap)*2),
      static_cast<unsigned>(snap));
  std::cout << t << std::endl;

  std::cout << get_prefixes(
      6,
      5,
      0) << std::endl;

  return 0;
}
