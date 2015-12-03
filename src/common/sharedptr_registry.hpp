// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_SHAREDPTR_REGISTRY_H
#define CEPH_SHAREDPTR_REGISTRY_H

#include <map>
#include <memory>
#include "common/Mutex.h"
#include "common/Cond.h"

/**
 * Provides a registry of shared_ptr<V> indexed by K while
 * the references are alive.
 */
template <class K, class V, class C = std::less<K> >
class SharedPtrRegistry {
public:
  typedef ceph::shared_ptr<V> VPtr;
  typedef ceph::weak_ptr<V> WeakVPtr;
  int waiting;
private:
  Mutex lock;
  Cond cond;
  map<K, pair<WeakVPtr, V*>, C> contents;

  class OnRemoval {
    SharedPtrRegistry<K,V,C> *parent;
    K key;
  public:
    OnRemoval(SharedPtrRegistry<K,V,C> *parent, K key) :
      parent(parent), key(key) {}
    void operator()(V *to_remove) {
      {
	Mutex::Locker l(parent->lock);
	typename map<K, pair<WeakVPtr, V*>, C>::iterator i =
	  parent->contents.find(key);
	if (i != parent->contents.end() &&
	    i->second.second == to_remove) {
	  parent->contents.erase(i);

          // notify those are waiting for us to be removed from contents
	  parent->cond.Signal(); 
	}
      }
      delete to_remove;
    }
  };
  friend class OnRemoval;

public:
  SharedPtrRegistry() :
    waiting(0),
    lock("SharedPtrRegistry::lock")
  {}

  bool empty() {
    Mutex::Locker l(lock);
    return contents.empty();
  }

  bool get_next(const K &key, pair<K, VPtr> *next) {
    pair<K, VPtr> r;
    {
      Mutex::Locker l(lock);
      VPtr next_val;
      typename map<K, pair<WeakVPtr, V*>, C>::iterator i =
	contents.upper_bound(key);
      while (i != contents.end() &&
	     !(next_val = i->second.first.lock()))
	++i;
      if (i == contents.end())
	return false;
      if (next)
	r = make_pair(i->first, next_val);
    }
    if (next)
      *next = r;
    return true;
  }

  
  bool get_next(const K &key, pair<K, V> *next) {
    VPtr next_val;
    Mutex::Locker l(lock);
    typename map<K, pair<WeakVPtr, V*>, C>::iterator i =
      contents.upper_bound(key);
    while (i != contents.end() &&
	   !(next_val = i->second.first.lock()))
      ++i;
    if (i == contents.end())
      return false;
    if (next)
      *next = make_pair(i->first, *next_val);
    return true;
  }

  VPtr lookup(const K &key) {
    Mutex::Locker l(lock);
    waiting++;
    while (1) {
      typename map<K, pair<WeakVPtr, V*>, C>::iterator i = contents.find(key);
      if (i != contents.end()) { // ok, find the key
	VPtr retval = i->second.first.lock(); // promote this weak_ptr to shared_ptr
	if (retval) { // promotion ok, raw pointer has not been released
	  waiting--;
	  return retval;
	}

        // raw pointer has been released, but the weak_ptr has not been 
        // removed from contents, we will wait someone to remove it
      } else { // key does not exist
	break;
      }
      cond.Wait(lock); // wait someone to remove the weak_ptr from contents
    }
    waiting--;
    return VPtr(); // ok, raw pointer has been released
  }

  VPtr lookup_or_create(const K &key) {
    Mutex::Locker l(lock);
    waiting++;
    while (1) {
      typename map<K, pair<WeakVPtr, V*>, C>::iterator i =
	contents.find(key);
      if (i != contents.end()) { // ok, find the key
	VPtr retval = i->second.first.lock(); // promote this weak_ptr to shared_ptr
	if (retval) { // promotion ok, raw pointer has not been released
	  waiting--;
	  return retval;
	}

        // raw pointer has been released, but the weak_ptr has not been 
        // removed from contents, we will wait someone to remove it       
      } else { // key does not exist
	break;
      }
      cond.Wait(lock); // wait someone to remove the weak_ptr from contents
    }

    // ok, the specified key does not exist, so we alloc an new item
    V *ptr = new V();
    VPtr retval(ptr, OnRemoval(this, key));
    // weak_ptr is always constructed from shared_ptr or other weak_ptr, it is an
    // observer of a exsiting shared_ptr
    
    /* refer to: http://en.cppreference.com/w/cpp/memory/weak_ptr
     * 
     * std::weak_ptr is a smart pointer that holds a non-owning ("weak") reference 
     * to an object that is managed by std::shared_ptr. It must be converted to 
     * std::shared_ptr in order to access the referenced object.
     * std::weak_ptr models temporary ownership: when an object needs to be 
     * accessed only if it exists, and it may be deleted at any time by someone 
     * else, std::weak_ptr is used to track the object, and it is converted to 
     * std::shared_ptr to assume temporary ownership. If the original std::shared_ptr 
     * is destroyed at this time, the object's lifetime is extended until the 
     * temporary std::shared_ptr is destroyed as well.
     * In addition, std::weak_ptr is used to break circular references of std::shared_ptr. 
     */
    contents.insert(make_pair(key, make_pair(retval, ptr)));
    waiting--;
    return retval;
  }

  unsigned size() {
    Mutex::Locker l(lock);
    return contents.size();
  }

  void remove(const K &key) {
    Mutex::Locker l(lock);
    contents.erase(key);
    cond.Signal();
  }

  template<class A>
  VPtr lookup_or_create(const K &key, const A &arg) {
    Mutex::Locker l(lock);
    waiting++;
    while (1) {
      typename map<K, pair<WeakVPtr, V*>, C>::iterator i =
	contents.find(key);
      if (i != contents.end()) {
	VPtr retval = i->second.first.lock();
	if (retval) {
	  waiting--;
	  return retval;
	}
      } else {
	break;
      }
      cond.Wait(lock);
    }
    V *ptr = new V(arg);
    VPtr retval(ptr, OnRemoval(this, key));
    contents.insert(make_pair(key, make_pair(retval, ptr)));
    waiting--;
    return retval;
  }

  friend class SharedPtrRegistryTest;
};

#endif
