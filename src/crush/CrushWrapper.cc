// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "osd/osd_types.h"
#include "common/debug.h"
#include "common/Formatter.h"
#include "common/errno.h"

#include "CrushWrapper.h"
#include "CrushTreeDumper.h"

#define dout_subsys ceph_subsys_crush

bool CrushWrapper::has_v2_rules() const
{
  for (unsigned i=0; i<crush->max_rules; i++) {
    if (is_v2_rule(i)) {
      return true;
    }
  }
  return false;
}

bool CrushWrapper::is_v2_rule(unsigned ruleid) const
{
  // check rule for use of indep or new SET_* rule steps
  if (ruleid >= crush->max_rules)
    return false;
  crush_rule *r = crush->rules[ruleid];
  if (!r)
    return false;
  for (unsigned j=0; j<r->len; j++) {
    if (r->steps[j].op == CRUSH_RULE_CHOOSE_INDEP ||
	r->steps[j].op == CRUSH_RULE_CHOOSELEAF_INDEP ||
	r->steps[j].op == CRUSH_RULE_SET_CHOOSE_TRIES ||
	r->steps[j].op == CRUSH_RULE_SET_CHOOSELEAF_TRIES) {
      return true;
    }
  }
  return false;
}

bool CrushWrapper::has_v3_rules() const
{
  for (unsigned i=0; i<crush->max_rules; i++) {
    if (is_v3_rule(i)) {
      return true;
    }
  }
  return false;
}

bool CrushWrapper::is_v3_rule(unsigned ruleid) const
{
  // check rule for use of SET_CHOOSELEAF_VARY_R step
  if (ruleid >= crush->max_rules)
    return false;
  crush_rule *r = crush->rules[ruleid];
  if (!r)
    return false;
  for (unsigned j=0; j<r->len; j++) {
    if (r->steps[j].op == CRUSH_RULE_SET_CHOOSELEAF_VARY_R) {
      return true;
    }
  }
  return false;
}

bool CrushWrapper::has_v4_buckets() const
{
  for (int i=0; i<crush->max_buckets; ++i) {
    crush_bucket *b = crush->buckets[i];
    if (!b)
      continue;
    if (b->alg == CRUSH_BUCKET_STRAW2)
      return true;
  }
  return false;
}

int CrushWrapper::can_rename_item(const string& srcname,
                                  const string& dstname,
                                  ostream *ss) const
{
  if (name_exists(srcname)) {
    if (name_exists(dstname)) {
      *ss << "dstname = '" << dstname << "' already exists";
      return -EEXIST;
    }
    if (is_valid_crush_name(dstname)) {
      return 0;
    } else {
      *ss << "srcname = '" << srcname << "' does not match [-_.0-9a-zA-Z]+";
      return -EINVAL;
    }
  } else {
    if (name_exists(dstname)) {
      *ss << "srcname = '" << srcname << "' does not exist "
          << "and dstname = '" << dstname << "' already exists";
      return -EALREADY;
    } else {
      *ss << "srcname = '" << srcname << "' does not exist";
      return -ENOENT;
    }
  }
}

int CrushWrapper::rename_item(const string& srcname,
                              const string& dstname,
                              ostream *ss)
{
  int ret = can_rename_item(srcname, dstname, ss);
  if (ret < 0)
    return ret;
  int oldid = get_item_id(srcname);
  return set_item_name(oldid, dstname);
}

int CrushWrapper::can_rename_bucket(const string& srcname,
                                    const string& dstname,
                                    ostream *ss) const
{
  int ret = can_rename_item(srcname, dstname, ss);
  if (ret)
    return ret;
  int srcid = get_item_id(srcname);
  if (srcid >= 0) {
    *ss << "srcname = '" << srcname << "' is not a bucket "
        << "because its id = " << srcid << " is >= 0";
    return -ENOTDIR;
  }
  return 0;
}

int CrushWrapper::rename_bucket(const string& srcname,
                                const string& dstname,
                                ostream *ss)
{
  int ret = can_rename_bucket(srcname, dstname, ss);
  if (ret < 0)
    return ret;
  int oldid = get_item_id(srcname);
  return set_item_name(oldid, dstname);
}

void CrushWrapper::find_takes(set<int>& roots) const
{
  for (unsigned i=0; i<crush->max_rules; i++) {
    crush_rule *r = crush->rules[i];
    if (!r)
      continue;
    for (unsigned j=0; j<r->len; j++) {
      if (r->steps[j].op == CRUSH_RULE_TAKE)
	roots.insert(r->steps[j].arg1);
    }
  }
}

void CrushWrapper::find_roots(set<int>& roots) const
{
  for (int i = 0; i < crush->max_buckets; i++) {
    if (!crush->buckets[i])
      continue;
    crush_bucket *b = crush->buckets[i];
    if (!_search_item_exists(b->id))
      roots.insert(b->id);
  }
}

bool CrushWrapper::subtree_contains(int root, int item) const
{
  if (root == item)
    return true;

  if (root >= 0)
    return false;  // root is a leaf

  const crush_bucket *b = get_bucket(root);
  if (!b)
    return false;

  for (unsigned j=0; j<b->size; j++) {
    if (subtree_contains(b->items[j], item))
      return true;
  }
  return false;
}

bool CrushWrapper::_maybe_remove_last_instance(CephContext *cct, int item, bool unlink_only)
{
  // last instance?
  if (_search_item_exists(item)) {
    return false;
  }
  if (item < 0 && _bucket_is_in_use(cct, item)) {
    return false;
  }

  if (item < 0 && !unlink_only) {
    crush_bucket *t = get_bucket(item);
    ldout(cct, 5) << "_maybe_remove_last_instance removing bucket " << item << dendl;
    crush_remove_bucket(crush, t);
  }
  if ((item >= 0 || !unlink_only) && name_map.count(item)) {
    ldout(cct, 5) << "_maybe_remove_last_instance removing name for item " << item << dendl;
    name_map.erase(item);
    have_rmaps = false;
  }
  return true;
}

int CrushWrapper::remove_item(CephContext *cct, int item, bool unlink_only)
{
  ldout(cct, 5) << "remove_item " << item << (unlink_only ? " unlink_only":"") << dendl;

  int ret = -ENOENT;

  if (item < 0 && !unlink_only) {
    crush_bucket *t = get_bucket(item);
    if (t && t->size) {
      ldout(cct, 1) << "remove_item bucket " << item << " has " << t->size
		    << " items, not empty" << dendl;
      return -ENOTEMPTY;
    }
    if (_bucket_is_in_use(cct, item)) {
      return -EBUSY;
    }
  }

  for (int i = 0; i < crush->max_buckets; i++) {
    if (!crush->buckets[i])
      continue;
    crush_bucket *b = crush->buckets[i];

    for (unsigned i=0; i<b->size; ++i) {
      int id = b->items[i];
      if (id == item) {
	ldout(cct, 5) << "remove_item removing item " << item
		      << " from bucket " << b->id << dendl;
	crush_bucket_remove_item(crush, b, item);
	adjust_item_weight(cct, b->id, b->weight);
	ret = 0;
      }
    }
  }

  if (_maybe_remove_last_instance(cct, item, unlink_only))
    ret = 0;
  
  return ret;
}

bool CrushWrapper::_search_item_exists(int item) const
{
  for (int i = 0; i < crush->max_buckets; i++) {
    if (!crush->buckets[i])
      continue;
    crush_bucket *b = crush->buckets[i];
    for (unsigned j=0; j<b->size; ++j) {
      if (b->items[j] == item)
	return true;
    }
  }
  return false;
}

bool CrushWrapper::_bucket_is_in_use(CephContext *cct, int item)
{
  for (unsigned i = 0; i < crush->max_rules; ++i) {
    crush_rule *r = crush->rules[i];
    if (!r)
      continue;
    for (unsigned j = 0; j < r->len; ++j) {
      if (r->steps[j].op == CRUSH_RULE_TAKE &&
	  r->steps[j].arg1 == item) {
	return true;
      }
    }
  }
  return false;
}

int CrushWrapper::_remove_item_under(CephContext *cct, int item, int ancestor, bool unlink_only)
{
  ldout(cct, 5) << "_remove_item_under " << item << " under " << ancestor
		<< (unlink_only ? " unlink_only":"") << dendl;

  if (ancestor >= 0) {
    return -EINVAL;
  }

  if (!bucket_exists(ancestor))
    return -EINVAL;

  int ret = -ENOENT;

  crush_bucket *b = get_bucket(ancestor);
  for (unsigned i=0; i<b->size; ++i) {
    int id = b->items[i];
    if (id == item) {
      ldout(cct, 5) << "_remove_item_under removing item " << item << " from bucket " << b->id << dendl;
      crush_bucket_remove_item(crush, b, item);
      adjust_item_weight(cct, b->id, b->weight);
      ret = 0;
    } else if (id < 0) {
      int r = remove_item_under(cct, item, id, unlink_only);
      if (r == 0)
	ret = 0;
    }
  }
  return ret;
}

int CrushWrapper::remove_item_under(CephContext *cct, int item, int ancestor, bool unlink_only)
{
  ldout(cct, 5) << "remove_item_under " << item << " under " << ancestor
		<< (unlink_only ? " unlink_only":"") << dendl;

  if (!unlink_only && _bucket_is_in_use(cct, item)) {
    return -EBUSY;
  }

  int ret = _remove_item_under(cct, item, ancestor, unlink_only);
  if (ret < 0)
    return ret;

  if (item < 0 && !unlink_only) {
    crush_bucket *t = get_bucket(item);
    if (t && t->size) {
      ldout(cct, 1) << "remove_item_undef bucket " << item << " has " << t->size
		    << " items, not empty" << dendl;
      return -ENOTEMPTY;
    }
  }

  if (_maybe_remove_last_instance(cct, item, unlink_only))
    ret = 0;

  return ret;
}

int CrushWrapper::get_common_ancestor_distance(CephContext *cct, int id,
			       const std::multimap<string,string>& loc)
{
  ldout(cct, 5) << __func__ << " " << id << " " << loc << dendl;
  if (!item_exists(id))
    return -ENOENT;
  map<string,string> id_loc = get_full_location(id);
  ldout(cct, 20) << " id is at " << id_loc << dendl;

  for (map<int,string>::const_iterator p = type_map.begin();
       p != type_map.end();
       ++p) {
    map<string,string>::iterator ip = id_loc.find(p->second);
    if (ip == id_loc.end())
      continue;
    for (std::multimap<string,string>::const_iterator q = loc.find(p->second);
	 q != loc.end();
	 ++q) {
      if (q->first != p->second)
	break;
      if (q->second == ip->second)
	return p->first;
    }
  }
  return -ERANGE;
}

int CrushWrapper::parse_loc_map(const std::vector<string>& args,
				std::map<string,string> *ploc)
{
  ploc->clear();
  for (unsigned i = 0; i < args.size(); ++i) {
    const char *s = args[i].c_str();
    const char *pos = strchr(s, '=');
    if (!pos)
      return -EINVAL;
    string key(s, 0, pos-s);
    string value(pos+1);
    if (value.length())
      (*ploc)[key] = value;
    else
      return -EINVAL;
  }
  return 0;
}

int CrushWrapper::parse_loc_multimap(const std::vector<string>& args,
					    std::multimap<string,string> *ploc)
{
  ploc->clear();
  for (unsigned i = 0; i < args.size(); ++i) {
    const char *s = args[i].c_str();
    const char *pos = strchr(s, '=');
    if (!pos)
      return -EINVAL;
    string key(s, 0, pos-s);
    string value(pos+1);
    if (value.length())
      ploc->insert(make_pair(key, value));
    else
      return -EINVAL;
  }
  return 0;
}

bool CrushWrapper::check_item_loc(CephContext *cct, int item, const map<string,string>& loc,
				  int *weight)
{
  ldout(cct, 5) << "check_item_loc item " << item << " loc " << loc << dendl;

  for (map<int,string>::const_iterator p = type_map.begin(); p != type_map.end(); ++p) {
    // ignore device
    if (p->first == 0)
      continue;

    // ignore types that aren't specified in loc
    map<string,string>::const_iterator q = loc.find(p->second);
    if (q == loc.end()) {
      ldout(cct, 2) << "warning: did not specify location for '" << p->second << "' level (levels are "
		    << type_map << ")" << dendl;
      continue;
    }

    if (!name_exists(q->second)) {
      ldout(cct, 5) << "check_item_loc bucket " << q->second << " dne" << dendl;
      return false;
    }

    int id = get_item_id(q->second);
    if (id >= 0) {
      ldout(cct, 5) << "check_item_loc requested " << q->second << " for type " << p->second
		    << " is a device, not bucket" << dendl;
      return false;
    }

    crush_bucket *b = get_bucket(id);
    assert(b);

    // see if item exists in this bucket
    for (unsigned j=0; j<b->size; j++) {
      if (b->items[j] == item) {
	ldout(cct, 2) << "check_item_loc " << item << " exists in bucket " << b->id << dendl;
	if (weight)
	  *weight = crush_get_bucket_item_weight(b, j);
	return true;
      }
    }
    return false;
  }
  
  ldout(cct, 1) << "check_item_loc item " << item << " loc " << loc << dendl;
  return false;
}

map<string, string> CrushWrapper::get_full_location(int id)
{
  vector<pair<string, string> > full_location_ordered;
  map<string,string> full_location;

  get_full_location_ordered(id, full_location_ordered);

  std::copy(full_location_ordered.begin(),
      full_location_ordered.end(),
      std::inserter(full_location, full_location.begin()));

  return full_location;
}

int CrushWrapper::get_full_location_ordered(int id, vector<pair<string, string> >& path)
{
  if (!item_exists(id))
    return -ENOENT;
  int cur = id;
  int ret;
  while (true) {
    pair<string, string> parent_coord = get_immediate_parent(cur, &ret);
    if (ret != 0)
      break;
    path.push_back(parent_coord);
    cur = get_item_id(parent_coord.second);
  }
  return 0;
}


map<int, string> CrushWrapper::get_parent_hierarchy(int id)
{
  map<int,string> parent_hierarchy;
  pair<string, string> parent_coord = get_immediate_parent(id);
  int parent_id;

  // get the integer type for id and create a counter from there
  int type_counter = get_bucket_type(id);

  // if we get a negative type then we can assume that we have an OSD
  // change behavior in get_item_type FIXME
  if (type_counter < 0)
    type_counter = 0;

  // read the type map and get the name of the type with the largest ID
  int high_type = 0;
  for (map<int, string>::iterator it = type_map.begin(); it != type_map.end(); ++it){
    if ( (*it).first > high_type )
      high_type = (*it).first;
  }

  parent_id = get_item_id(parent_coord.second);

  while (type_counter < high_type) {
    type_counter++;
    parent_hierarchy[ type_counter ] = parent_coord.first;

    if (type_counter < high_type){
      // get the coordinate information for the next parent
      parent_coord = get_immediate_parent(parent_id);
      parent_id = get_item_id(parent_coord.second);
    }
  }

  return parent_hierarchy;
}

int CrushWrapper::get_children(int id, list<int> *children)
{
  // leaf?
  if (id >= 0) {
    return 0;
  }

  crush_bucket *b = get_bucket(id);
  if (!b) {
    return -ENOENT;
  }

  for (unsigned n=0; n<b->size; n++) {
    children->push_back(b->items[n]);
  }
  return b->size;
}


int CrushWrapper::insert_item(CephContext *cct, int item, float weight, string name,
			      const map<string,string>& loc)  // typename -> bucketname
{

  ldout(cct, 5) << "insert_item item " << item << " weight " << weight
		<< " name " << name << " loc " << loc << dendl;

  // -_0-9A-Za-z
  if (!is_valid_crush_name(name)) // item name, e.g. osd.1
    return -EINVAL;

  if (!is_valid_crush_loc(cct, loc))
    return -EINVAL;

  if (name_exists(name)) {
    if (get_item_id(name) != item) {
      ldout(cct, 10) << "device name '" << name << "' already exists as id "
		     << get_item_id(name) << dendl;
      return -EEXIST;
    }
  } else {
    set_item_name(item, name); // insert into CrushWrapper::name_map and CrushWrapper::name_rmap
  }

  int cur = item; // osd id

  // create locations if locations don't exist and add child in location with 0 weight
  // the more detail in the insert_item method declaration in CrushWrapper.h
  for (map<int,string>::iterator p = type_map.begin(); p != type_map.end(); ++p) { // <bucket type id, bucket type name>
    // ignore device type
    if (p->first == 0) // bucket type id of 0 is osd
      continue;

    // iterate every bucket type, in increasing order, i.e. from host -> root

    // loc, e.g.
    // host: ceph0
    // rack: rack0
    // root: default

    // skip types that are unspecified
    map<string,string>::const_iterator q = loc.find(p->second); // <bucket type name, bucket name>
    if (q == loc.end()) {
      // this is totally normal, e.g. we add osd(s) with only bucket type of host specified
      ldout(cct, 2) << "warning: did not specify location for '" << p->second << "' level (levels are "
		    << type_map << ")" << dendl;
      continue;
    }

    if (!name_exists(q->second)) { // the specified bucket name does not exist
      ldout(cct, 5) << "insert_item creating bucket " << q->second << dendl;
      int empty = 0, newid;
      // create a bucket of type id p->first, and insert the item into the bucket
      // with 0 weight
      int r = add_bucket(0, 0,
			 CRUSH_HASH_DEFAULT, p->first, 1, &cur, &empty, &newid);
      if (r < 0) {
        ldout(cct, 1) << "add_bucket failure error: " << cpp_strerror(r) << dendl;
        return r;
      }

      // insert the new bucket into CrushWrapper::name_map and CrushWrapper::name_rmap
      set_item_name(newid, q->second); 
      
      cur = newid; // bucket id, a negative integer
      continue;
    }

    // ok, a bucket with the specified name exists

    // add to an existing bucket
    int id = get_item_id(q->second); // from bucket name to get bucket id, this info is stored in CrushWrapper
    if (!bucket_exists(id)) {
      // the specified bucket id does not exist in crush map, but the CrushWrapper
      // tells us the bucket with the specified name does exist, this must be
      // a bug
      ldout(cct, 1) << "insert_item doesn't have bucket " << id << dendl;
      return -EINVAL;
    }

    // check that we aren't creating a cycle.
    if (subtree_contains(id, cur)) {
      ldout(cct, 1) << "insert_item item " << cur << " already exists beneath " << id << dendl;
      return -EINVAL;
    }

    crush_bucket *b = get_bucket(id);
    assert(b);

    if (p->first != b->type) { // do not name buckets with the same name and different bucket type
      ldout(cct, 1) << "insert_item existing bucket has type "
	<< "'" << type_map[b->type] << "' != "
	<< "'" << type_map[p->first] << "'" << dendl;
      return -EINVAL;
    }

    // are we forming a loop?
    if (subtree_contains(cur, b->id)) {
      ldout(cct, 1) << "insert_item " << cur << " already contains " << b->id
		    << "; cannot form loop" << dendl;
      return -ELOOP;
    }

    ldout(cct, 5) << "insert_item adding " << cur << " weight " << weight
		  << " to bucket " << id << dendl;

    // insert the item into bucket with 0 weight
    int r = crush_bucket_add_item(crush, b, cur, 0);
    assert (!r);
    break;
  }

  // adjust the item's weight in location
  if(adjust_item_weightf_in_loc(cct, item, weight, loc) > 0) {
    if (item >= crush->max_devices) {
      crush->max_devices = item + 1;
      ldout(cct, 5) << "insert_item max_devices now " << crush->max_devices << dendl;
    }
    return 0;
  }

  ldout(cct, 1) << "error: didn't find anywhere to add item " << item << " in " << loc << dendl;
  return -EINVAL;
}

int CrushWrapper::move_bucket(CephContext *cct, int id, const map<string,string>& loc)
{
  // sorry this only works for buckets
  if (id >= 0)
    return -EINVAL;

  if (!item_exists(id))
    return -ENOENT;

  // get the name of the bucket we are trying to move for later
  string id_name = get_item_name(id);

  // detach the bucket
  int bucket_weight = detach_bucket(cct, id);

  // insert the bucket back into the hierarchy
  return insert_item(cct, id, bucket_weight / (float)0x10000, id_name, loc);
}

int CrushWrapper::link_bucket(CephContext *cct, int id, const map<string,string>& loc)
{
  // sorry this only works for buckets
  if (id >= 0)
    return -EINVAL;

  if (!item_exists(id))
    return -ENOENT;

  // get the name of the bucket we are trying to move for later
  string id_name = get_item_name(id);

  crush_bucket *b = get_bucket(id);
  unsigned bucket_weight = b->weight;

  return insert_item(cct, id, bucket_weight / (float)0x10000, id_name, loc);
}

int CrushWrapper::create_or_move_item(CephContext *cct, int item, float weight, string name,
				      const map<string,string>& loc)  // typename -> bucketname
{
  int ret = 0;
  int old_iweight;

  if (!is_valid_crush_name(name))
    return -EINVAL;

  if (check_item_loc(cct, item, loc, &old_iweight)) {
    ldout(cct, 5) << "create_or_move_item " << item << " already at " << loc << dendl;
  } else {
    if (_search_item_exists(item)) {
      weight = get_item_weightf(item);
      ldout(cct, 10) << "create_or_move_item " << item << " exists with weight " << weight << dendl;
      remove_item(cct, item, true);
    }
    ldout(cct, 5) << "create_or_move_item adding " << item << " weight " << weight
		  << " at " << loc << dendl;
    ret = insert_item(cct, item, weight, name, loc);
    if (ret == 0)
      ret = 1;  // changed
  }
  return ret;
}

int CrushWrapper::update_item(CephContext *cct, int item, float weight, string name,
			      const map<string,string>& loc)  // typename -> bucketname
{
  ldout(cct, 5) << "update_item item " << item << " weight " << weight
		<< " name " << name << " loc " << loc << dendl;
  int ret = 0;

  if (!is_valid_crush_name(name))
    return -EINVAL;

  if (!is_valid_crush_loc(cct, loc))
    return -EINVAL;

  // compare quantized (fixed-point integer) weights!  
  int iweight = (int)(weight * (float)0x10000);
  int old_iweight;
  if (check_item_loc(cct, item, loc, &old_iweight)) {
    ldout(cct, 5) << "update_item " << item << " already at " << loc << dendl;
    if (old_iweight != iweight) {
      ldout(cct, 5) << "update_item " << item << " adjusting weight "
		    << ((float)old_iweight/(float)0x10000) << " -> " << weight << dendl;
      adjust_item_weight_in_loc(cct, item, iweight, loc);
      ret = 1;
    }
    if (get_item_name(item) != name) {
      ldout(cct, 5) << "update_item setting " << item << " name to " << name << dendl;
      set_item_name(item, name);
      ret = 1;
    }
  } else {
    if (item_exists(item)) {
      remove_item(cct, item, true);
    }
    ldout(cct, 5) << "update_item adding " << item << " weight " << weight
		  << " at " << loc << dendl;
    ret = insert_item(cct, item, weight, name, loc);
    if (ret == 0)
      ret = 1;  // changed
  }
  return ret;
}

int CrushWrapper::get_item_weight(int id) const
{
  for (int bidx = 0; bidx < crush->max_buckets; bidx++) {
    crush_bucket *b = crush->buckets[bidx];
    if (b == NULL)
      continue;
    for (unsigned i = 0; i < b->size; i++)
      if (b->items[i] == id)
	return crush_get_bucket_item_weight(b, i);
  }
  return -ENOENT;
}

int CrushWrapper::get_item_weight_in_loc(int id, const map<string,string> &loc)
{
  for (map<string,string>::const_iterator l = loc.begin(); l != loc.end(); ++l) {

    int bid = get_item_id(l->second);
    if (!bucket_exists(bid))
      continue;
    crush_bucket *b = get_bucket(bid);
    if ( b == NULL)
      continue;
    for (unsigned int i = 0; i < b->size; i++) {
      if (b->items[i] == id) {
	return crush_get_bucket_item_weight(b, i);
      }
    }
  }
  return -ENOENT;
}

int CrushWrapper::adjust_item_weight(CephContext *cct, int id, int weight)
{
  ldout(cct, 5) << "adjust_item_weight " << id << " weight " << weight << dendl;
  int changed = 0;
  for (int bidx = 0; bidx < crush->max_buckets; bidx++) {
    crush_bucket *b = crush->buckets[bidx];
    if (b == 0)
      continue;
    for (unsigned i = 0; i < b->size; i++) {
      if (b->items[i] == id) {
	int diff = crush_bucket_adjust_item_weight(crush, b, id, weight);
	ldout(cct, 5) << "adjust_item_weight " << id << " diff " << diff << " in bucket " << bidx << dendl;
	adjust_item_weight(cct, -1 - bidx, b->weight);
	changed++;
      }
    }
  }
  if (!changed)
    return -ENOENT;
  return changed;
}

int CrushWrapper::adjust_item_weight_in_loc(CephContext *cct, int id, int weight, const map<string,string>& loc)
{
  ldout(cct, 5) << "adjust_item_weight_in_loc " << id << " weight " << weight << " in " << loc << dendl;
  int changed = 0;

  for (map<string,string>::const_iterator l = loc.begin(); l != loc.end(); ++l) {
    int bid = get_item_id(l->second);
    if (!bucket_exists(bid))
      continue;
    crush_bucket *b = get_bucket(bid);
    if ( b == NULL)
      continue;
    for (unsigned int i = 0; i < b->size; i++) {
      if (b->items[i] == id) {
	int diff = crush_bucket_adjust_item_weight(crush, b, id, weight);
	ldout(cct, 5) << "adjust_item_weight_in_loc " << id << " diff " << diff << " in bucket " << bid << dendl;
	adjust_item_weight(cct, bid, b->weight);
	changed++;
      }
    }
  }
  if (!changed)
    return -ENOENT;
  return changed;
}

int CrushWrapper::adjust_subtree_weight(CephContext *cct, int id, int weight)
{
  ldout(cct, 5) << __func__ << " " << id << " weight " << weight << dendl;
  crush_bucket *b = get_bucket(id);
  if (IS_ERR(b))
    return PTR_ERR(b);
  int changed = 0;
  list<crush_bucket*> q;
  q.push_back(b);
  while (!q.empty()) {
    b = q.front();
    q.pop_front();
    int local_changed = 0;
    for (unsigned i=0; i<b->size; ++i) {
      int n = b->items[i];
      if (n >= 0) {
	crush_bucket_adjust_item_weight(crush, b, n, weight);
	++changed;
	++local_changed;
      } else {
	crush_bucket *sub = get_bucket(n);
	if (IS_ERR(sub))
	  continue;
	q.push_back(sub);
      }
    }
    if (local_changed) {
      adjust_item_weight(cct, b->id, b->weight);
    }
  }
  return changed;
}

bool CrushWrapper::check_item_present(int id) const
{
  bool found = false;

  for (int bidx = 0; bidx < crush->max_buckets; bidx++) {
    crush_bucket *b = crush->buckets[bidx];
    if (b == 0)
      continue;
    for (unsigned i = 0; i < b->size; i++)
      if (b->items[i] == id)
	found = true;
  }
  return found;
}


pair<string,string> CrushWrapper::get_immediate_parent(int id, int *_ret)
{
  pair <string, string> loc;
  int ret = -ENOENT;

  for (int bidx = 0; bidx < crush->max_buckets; bidx++) {
    crush_bucket *b = crush->buckets[bidx];
    if (b == 0)
      continue;
    for (unsigned i = 0; i < b->size; i++)
      if (b->items[i] == id) {
        string parent_id = name_map[b->id];
        string parent_bucket_type = type_map[b->type];
        loc = make_pair(parent_bucket_type, parent_id);
        ret = 0;
      }
  }

  if (_ret)
    *_ret = ret;

  return loc;
}

int CrushWrapper::get_immediate_parent_id(int id, int *parent)
{
  for (int bidx = 0; bidx < crush->max_buckets; bidx++) {
    crush_bucket *b = crush->buckets[bidx];
    if (b == 0)
      continue;
    for (unsigned i = 0; i < b->size; i++) {
      if (b->items[i] == id) {
	*parent = b->id;
	return 0;
      }
    }
  }
  return -ENOENT;
}

void CrushWrapper::reweight(CephContext *cct)
{
  set<int> roots;
  find_roots(roots);
  for (set<int>::iterator p = roots.begin(); p != roots.end(); ++p) {
    if (*p >= 0)
      continue;
    crush_bucket *b = get_bucket(*p);
    ldout(cct, 5) << "reweight bucket " << *p << dendl;
    int r = crush_reweight_bucket(crush, b);
    assert(r == 0);
  }
}

int CrushWrapper::add_simple_ruleset_at(string name, string root_name,
                                        string failure_domain_name,
                                        string mode, int rule_type,
                                        int rno, ostream *err)
{
  if (rule_exists(name)) {
    // rule name exists in CrushWrapper::rule_name_rmap
    if (err)
      *err << "rule " << name << " exists";
    return -EEXIST;
  }

  // we are creating a simple rule which has its rule id == ruleset id
  if (rno >= 0) {
    if (rule_exists(rno)) {
      // CrushWrapper::crush->rules[rno] is not NULL
      if (err)
        *err << "rule with ruleno " << rno << " exists";
      return -EEXIST;
    }
    if (ruleset_exists(rno)) {
      // there is a rule with ruleset id set to rno
      if (err)
        *err << "ruleset " << rno << " exists";
      return -EEXIST;
    }
  } else {
    // to find a ruleno that has not been used
    for (rno = 0; rno < get_max_rules(); rno++) {
      if (!rule_exists(rno) && !ruleset_exists(rno))
        break;
    }
  }
  if (!name_exists(root_name)) {
    if (err)
      *err << "root item " << root_name << " does not exist";
    return -ENOENT;
  }

  // begins iterating down the tree from the specified bucket
  int root = get_item_id(root_name);
  
  int type = 0;
  if (failure_domain_name.length()) {
    // get bucket type id of failure domain
    type = get_type_id(failure_domain_name);
    if (type < 0) {
      if (err)
	*err << "unknown type " << failure_domain_name;
      return -EINVAL;
    }
  }
  if (mode != "firstn" && mode != "indep") {
    if (err)
      *err << "unknown mode " << mode;
    return -EINVAL;
  }

  int steps = 3;
  if (mode == "indep")
    steps = 5;
  int min_rep = mode == "firstn" ? 1 : 3;
  int max_rep = mode == "firstn" ? 10 : 20;
  
  //set the ruleset the same as rule_id(rno)
  // create an instance of crush_rule and initialized it, we are creating a simple 
  // rule that rule id == ruleset id, in CrushWrapper::add_rule we will see that
  // rule id != ruleset id
  crush_rule *rule = crush_make_rule(steps, rno, rule_type, min_rep, max_rep);
  assert(rule);

  // setup rule->steps[step]

  int step = 0;
  if (mode == "indep") {
    crush_rule_set_step(rule, step++, CRUSH_RULE_SET_CHOOSELEAF_TRIES, 5, 0);
    crush_rule_set_step(rule, step++, CRUSH_RULE_SET_CHOOSE_TRIES, 100, 0);
  }

  // step take
  crush_rule_set_step(rule, step++, CRUSH_RULE_TAKE, root, 0);

  // step choose firstn {num} type {bucket-type}
  // selects a set of buckets of {bucket-type}.
  //    If {num} == 0, choose pool-num-replicas buckets (all available).
  //    If {num} > 0 && < pool-num-replicas, choose that many buckets.
  //    If {num} < 0, it means pool-num-replicas - |{num}|.

  // step chooseleaf firstn {num} type {bucket-type}
  // selects a set of buckets of {bucket-type} and chooses a leaf node from the 
  // subtree of each bucket in the set of buckets.
  //    If {num} == 0, choose pool-num-replicas buckets (all available).
  //    If {num} > 0 && < pool-num-replicas, choose that many buckets.
  //    If {num} < 0, it means pool-num-replicas - |{num}|.

  // step choose firstn {num} type {bucket-type} / step chooseleaf firstn {num} type {bucket-type}
  if (type)
    crush_rule_set_step(rule, step++,
			mode == "firstn" ? CRUSH_RULE_CHOOSELEAF_FIRSTN :
			CRUSH_RULE_CHOOSELEAF_INDEP,
			CRUSH_CHOOSE_N,
			type);
  else
    crush_rule_set_step(rule, step++,
			mode == "firstn" ? CRUSH_RULE_CHOOSE_FIRSTN :
			CRUSH_RULE_CHOOSE_INDEP,
			CRUSH_CHOOSE_N,
			0);

  // step emit
  crush_rule_set_step(rule, step++, CRUSH_RULE_EMIT, 0, 0);

  // insert this rule into crush->rules[]
  int ret = crush_add_rule(crush, rule, rno);
  if(ret < 0) {
    *err << "failed to add rule " << rno << " because " << cpp_strerror(ret);
    return ret;
  }

  // note down the <rule id, rule name> map
  set_rule_name(rno, name);
  have_rmaps = false;
  return rno;
}

int CrushWrapper::add_simple_ruleset(string name, string root_name,
                                     string failure_domain_name,
                                     string mode, int rule_type,
                                     ostream *err)
{
  return add_simple_ruleset_at(name, root_name, failure_domain_name, mode,
                               rule_type, -1, err);
}

int CrushWrapper::get_rule_weight_osd_map(unsigned ruleno, map<int,float> *pmap)
{
  if (ruleno >= crush->max_rules)
    return -ENOENT;
  if (crush->rules[ruleno] == NULL)
    return -ENOENT;
  crush_rule *rule = crush->rules[ruleno];

  // build a weight map for each TAKE in the rule, and then merge them
  for (unsigned i=0; i<rule->len; ++i) {
    map<int,float> m;
    float sum = 0;
    if (rule->steps[i].op == CRUSH_RULE_TAKE) {
      int n = rule->steps[i].arg1;
      if (n >= 0) {
	m[n] = 1.0;
	sum = 1.0;
      } else {
	list<int> q;
	q.push_back(n);
	//breadth first iterate the OSD tree
	while (!q.empty()) {
	  int bno = q.front();
	  q.pop_front();
	  crush_bucket *b = crush->buckets[-1-bno];
	  assert(b);
	  for (unsigned j=0; j<b->size; ++j) {
	    int item_id = b->items[j];
	    if (item_id >= 0) { //it's an OSD
	      float w = crush_get_bucket_item_weight(b, j);
	      m[item_id] = w;
	      sum += w;
	    } else { //not an OSD, expand the child later
	      q.push_back(item_id);
	    }
	  }
	}
      }
    }
    for (map<int,float>::iterator p = m.begin(); p != m.end(); ++p) {
      map<int,float>::iterator q = pmap->find(p->first);
      if (q == pmap->end()) {
	(*pmap)[p->first] = p->second / sum;
      } else {
	q->second += p->second / sum;
      }
    }
  }

  return 0;
}

int CrushWrapper::remove_rule(int ruleno)
{
  if (ruleno >= (int)crush->max_rules)
    return -ENOENT;
  if (crush->rules[ruleno] == NULL)
    return -ENOENT;
  crush_destroy_rule(crush->rules[ruleno]);
  crush->rules[ruleno] = NULL;
  rule_name_map.erase(ruleno);
  have_rmaps = false;
  return 0;
}

void CrushWrapper::encode(bufferlist& bl, bool lean) const
{
  assert(crush);

  __u32 magic = CRUSH_MAGIC;
  ::encode(magic, bl);

  ::encode(crush->max_buckets, bl);
  ::encode(crush->max_rules, bl);
  ::encode(crush->max_devices, bl);

  // buckets
  for (int i=0; i<crush->max_buckets; i++) {
    __u32 alg = 0;
    if (crush->buckets[i]) alg = crush->buckets[i]->alg;
    ::encode(alg, bl);
    if (!alg)
      continue;

    ::encode(crush->buckets[i]->id, bl);
    ::encode(crush->buckets[i]->type, bl);
    ::encode(crush->buckets[i]->alg, bl);
    ::encode(crush->buckets[i]->hash, bl);
    ::encode(crush->buckets[i]->weight, bl);
    ::encode(crush->buckets[i]->size, bl);
    for (unsigned j=0; j<crush->buckets[i]->size; j++)
      ::encode(crush->buckets[i]->items[j], bl);

    switch (crush->buckets[i]->alg) {
    case CRUSH_BUCKET_UNIFORM:
      ::encode((reinterpret_cast<crush_bucket_uniform*>(crush->buckets[i]))->item_weight, bl);
      break;

    case CRUSH_BUCKET_LIST:
      for (unsigned j=0; j<crush->buckets[i]->size; j++) {
	::encode((reinterpret_cast<crush_bucket_list*>(crush->buckets[i]))->item_weights[j], bl);
	::encode((reinterpret_cast<crush_bucket_list*>(crush->buckets[i]))->sum_weights[j], bl);
      }
      break;

    case CRUSH_BUCKET_TREE:
      ::encode((reinterpret_cast<crush_bucket_tree*>(crush->buckets[i]))->num_nodes, bl);
      for (unsigned j=0; j<(reinterpret_cast<crush_bucket_tree*>(crush->buckets[i]))->num_nodes; j++)
	::encode((reinterpret_cast<crush_bucket_tree*>(crush->buckets[i]))->node_weights[j], bl);
      break;

    case CRUSH_BUCKET_STRAW:
      for (unsigned j=0; j<crush->buckets[i]->size; j++) {
	::encode((reinterpret_cast<crush_bucket_straw*>(crush->buckets[i]))->item_weights[j], bl);
	::encode((reinterpret_cast<crush_bucket_straw*>(crush->buckets[i]))->straws[j], bl);
      }
      break;

    case CRUSH_BUCKET_STRAW2:
      for (unsigned j=0; j<crush->buckets[i]->size; j++) {
	::encode((reinterpret_cast<crush_bucket_straw2*>(crush->buckets[i]))->item_weights[j], bl);
      }
      break;

    default:
      assert(0);
      break;
    }
  }

  // rules
  for (unsigned i=0; i<crush->max_rules; i++) {
    __u32 yes = crush->rules[i] ? 1:0;
    ::encode(yes, bl);
    if (!yes)
      continue;

    ::encode(crush->rules[i]->len, bl);
    ::encode(crush->rules[i]->mask, bl);
    for (unsigned j=0; j<crush->rules[i]->len; j++)
      ::encode(crush->rules[i]->steps[j], bl);
  }

  // name info
  ::encode(type_map, bl);
  ::encode(name_map, bl);
  ::encode(rule_name_map, bl);

  // tunables
  ::encode(crush->choose_local_tries, bl);
  ::encode(crush->choose_local_fallback_tries, bl);
  ::encode(crush->choose_total_tries, bl);
  ::encode(crush->chooseleaf_descend_once, bl);
  ::encode(crush->chooseleaf_vary_r, bl);
  ::encode(crush->straw_calc_version, bl);
  ::encode(crush->allowed_bucket_algs, bl);
}

static void decode_32_or_64_string_map(map<int32_t,string>& m, bufferlist::iterator& blp)
{
  m.clear();
  __u32 n;
  ::decode(n, blp);
  while (n--) {
    __s32 key;
    ::decode(key, blp);

    __u32 strlen;
    ::decode(strlen, blp);
    if (strlen == 0) {
      // der, key was actually 64-bits!
      ::decode(strlen, blp);
    }
    ::decode_nohead(strlen, m[key], blp);
  }
}

void CrushWrapper::decode(bufferlist::iterator& blp)
{
  create();

  __u32 magic;
  ::decode(magic, blp);
  if (magic != CRUSH_MAGIC)
    throw buffer::malformed_input("bad magic number");

  ::decode(crush->max_buckets, blp);
  ::decode(crush->max_rules, blp);
  ::decode(crush->max_devices, blp);

  // legacy tunables, unless we decode something newer
  set_tunables_legacy();

  try {
    // buckets
    crush->buckets = (crush_bucket**)calloc(1, crush->max_buckets * sizeof(crush_bucket*));
    for (int i=0; i<crush->max_buckets; i++) {
      decode_crush_bucket(&crush->buckets[i], blp);
    }

    // rules
    crush->rules = (crush_rule**)calloc(1, crush->max_rules * sizeof(crush_rule*));
    for (unsigned i = 0; i < crush->max_rules; ++i) {
      __u32 yes;
      ::decode(yes, blp);
      if (!yes) {
	crush->rules[i] = NULL;
	continue;
      }

      __u32 len;
      ::decode(len, blp);
      crush->rules[i] = reinterpret_cast<crush_rule*>(calloc(1, crush_rule_size(len)));
      crush->rules[i]->len = len;
      ::decode(crush->rules[i]->mask, blp);
      for (unsigned j=0; j<crush->rules[i]->len; j++)
	::decode(crush->rules[i]->steps[j], blp);
    }

    // name info
    // NOTE: we had a bug where we were incoding int instead of int32, which means the
    // 'key' field for these maps may be either 32 or 64 bits, depending.  tolerate
    // both by assuming the string is always non-empty.
    decode_32_or_64_string_map(type_map, blp);
    decode_32_or_64_string_map(name_map, blp);
    decode_32_or_64_string_map(rule_name_map, blp);

    // tunables
    if (!blp.end()) {
      ::decode(crush->choose_local_tries, blp);
      ::decode(crush->choose_local_fallback_tries, blp);
      ::decode(crush->choose_total_tries, blp);
    }
    if (!blp.end()) {
      ::decode(crush->chooseleaf_descend_once, blp);
    }
    if (!blp.end()) {
      ::decode(crush->chooseleaf_vary_r, blp);
    }
    if (!blp.end()) {
      ::decode(crush->straw_calc_version, blp);
    }
    if (!blp.end()) {
      ::decode(crush->allowed_bucket_algs, blp);
    }
    finalize();
  }
  catch (...) {
    crush_destroy(crush);
    throw;
  }
}

void CrushWrapper::decode_crush_bucket(crush_bucket** bptr, bufferlist::iterator &blp)
{
  __u32 alg;
  ::decode(alg, blp);
  if (!alg) {
    *bptr = NULL;
    return;
  }

  int size = 0;
  switch (alg) {
  case CRUSH_BUCKET_UNIFORM:
    size = sizeof(crush_bucket_uniform);
    break;
  case CRUSH_BUCKET_LIST:
    size = sizeof(crush_bucket_list);
    break;
  case CRUSH_BUCKET_TREE:
    size = sizeof(crush_bucket_tree);
    break;
  case CRUSH_BUCKET_STRAW:
    size = sizeof(crush_bucket_straw);
    break;
  case CRUSH_BUCKET_STRAW2:
    size = sizeof(crush_bucket_straw2);
    break;
  default:
    {
      char str[128];
      snprintf(str, sizeof(str), "unsupported bucket algorithm: %d", alg);
      throw buffer::malformed_input(str);
    }
  }
  crush_bucket *bucket = reinterpret_cast<crush_bucket*>(calloc(1, size));
  *bptr = bucket;
    
  ::decode(bucket->id, blp);
  ::decode(bucket->type, blp);
  ::decode(bucket->alg, blp);
  ::decode(bucket->hash, blp);
  ::decode(bucket->weight, blp);
  ::decode(bucket->size, blp);

  bucket->items = (__s32*)calloc(1, bucket->size * sizeof(__s32));
  for (unsigned j = 0; j < bucket->size; ++j) {
    ::decode(bucket->items[j], blp);
  }

  bucket->perm = (__u32*)calloc(1, bucket->size * sizeof(__u32));
  bucket->perm_n = 0;

  switch (bucket->alg) {
  case CRUSH_BUCKET_UNIFORM:
    ::decode((reinterpret_cast<crush_bucket_uniform*>(bucket))->item_weight, blp);
    break;

  case CRUSH_BUCKET_LIST: {
    crush_bucket_list* cbl = reinterpret_cast<crush_bucket_list*>(bucket);
    cbl->item_weights = (__u32*)calloc(1, bucket->size * sizeof(__u32));
    cbl->sum_weights = (__u32*)calloc(1, bucket->size * sizeof(__u32));

    for (unsigned j = 0; j < bucket->size; ++j) {
      ::decode(cbl->item_weights[j], blp);
      ::decode(cbl->sum_weights[j], blp);
    }
    break;
  }

  case CRUSH_BUCKET_TREE: {
    crush_bucket_tree* cbt = reinterpret_cast<crush_bucket_tree*>(bucket);
    ::decode(cbt->num_nodes, blp);
    cbt->node_weights = (__u32*)calloc(1, cbt->num_nodes * sizeof(__u32));
    for (unsigned j=0; j<cbt->num_nodes; j++) {
      ::decode(cbt->node_weights[j], blp);
    }
    break;
  }

  case CRUSH_BUCKET_STRAW: {
    crush_bucket_straw* cbs = reinterpret_cast<crush_bucket_straw*>(bucket);
    cbs->straws = (__u32*)calloc(1, bucket->size * sizeof(__u32));
    cbs->item_weights = (__u32*)calloc(1, bucket->size * sizeof(__u32));
    for (unsigned j = 0; j < bucket->size; ++j) {
      ::decode(cbs->item_weights[j], blp);
      ::decode(cbs->straws[j], blp);
    }
    break;
  }

  case CRUSH_BUCKET_STRAW2: {
    crush_bucket_straw2* cbs = reinterpret_cast<crush_bucket_straw2*>(bucket);
    cbs->item_weights = (__u32*)calloc(1, bucket->size * sizeof(__u32));
    for (unsigned j = 0; j < bucket->size; ++j) {
      ::decode(cbs->item_weights[j], blp);
    }
    break;
  }

  default:
    // We should have handled this case in the first switch statement
    assert(0);
    break;
  }
}

  
void CrushWrapper::dump(Formatter *f) const
{
  f->open_array_section("devices");
  for (int i=0; i<get_max_devices(); i++) {
    f->open_object_section("device");
    f->dump_int("id", i);
    const char *n = get_item_name(i);
    if (n) {
      f->dump_string("name", n);
    } else {
      char name[20];
      sprintf(name, "device%d", i);
      f->dump_string("name", name);
    }
    f->close_section();
  }
  f->close_section();

  f->open_array_section("types");
  int n = get_num_type_names();
  for (int i=0; n; i++) {
    const char *name = get_type_name(i);
    if (!name) {
      if (i == 0) {
	f->open_object_section("type");
	f->dump_int("type_id", 0);
	f->dump_string("name", "device");
	f->close_section();
      }
      continue;
    }
    n--;
    f->open_object_section("type");
    f->dump_int("type_id", i);
    f->dump_string("name", name);
    f->close_section();
  }
  f->close_section();

  f->open_array_section("buckets");
  for (int bucket = -1; bucket > -1-get_max_buckets(); --bucket) {
    if (!bucket_exists(bucket))
      continue;
    f->open_object_section("bucket");
    f->dump_int("id", bucket);
    if (get_item_name(bucket))
      f->dump_string("name", get_item_name(bucket));
    f->dump_int("type_id", get_bucket_type(bucket));
    if (get_type_name(get_bucket_type(bucket)))
      f->dump_string("type_name", get_type_name(get_bucket_type(bucket)));
    f->dump_int("weight", get_bucket_weight(bucket));
    f->dump_string("alg", crush_bucket_alg_name(get_bucket_alg(bucket)));
    f->dump_string("hash", crush_hash_name(get_bucket_hash(bucket)));
    f->open_array_section("items");
    for (int j=0; j<get_bucket_size(bucket); j++) {
      f->open_object_section("item");
      f->dump_int("id", get_bucket_item(bucket, j));
      f->dump_int("weight", get_bucket_item_weight(bucket, j));
      f->dump_int("pos", j);
      f->close_section();
    }
    f->close_section();
    f->close_section();
  }
  f->close_section();

  f->open_array_section("rules");
  dump_rules(f);
  f->close_section();

  f->open_object_section("tunables");
  dump_tunables(f);
  f->close_section();
}

namespace {
  // depth first walker
  class TreeDumper {
    typedef CrushTreeDumper::Item Item;
    const CrushWrapper *crush;
  public:
    TreeDumper(const CrushWrapper *crush)
      : crush(crush) {}

    void dump(Formatter *f) {
      set<int> roots;
      crush->find_roots(roots);
      for (set<int>::iterator root = roots.begin(); root != roots.end(); ++root) {
	dump_item(Item(*root, 0, crush->get_bucket_weightf(*root)), f);
      }
    }

  private:
    void dump_item(const Item& qi, Formatter* f) {
      if (qi.is_bucket()) {
	f->open_object_section("bucket");
	CrushTreeDumper::dump_item_fields(crush, qi, f);
	dump_bucket_children(qi, f);
	f->close_section();
      } else {
	f->open_object_section("device");
	CrushTreeDumper::dump_item_fields(crush, qi, f);
	f->close_section();
      }
    }

    void dump_bucket_children(const Item& parent, Formatter* f) {
      f->open_array_section("items");
      const int max_pos = crush->get_bucket_size(parent.id);
      for (int pos = 0; pos < max_pos; pos++) {
	int id = crush->get_bucket_item(parent.id, pos);
	float weight = crush->get_bucket_item_weightf(parent.id, pos);
	dump_item(Item(id, parent.depth + 1, weight), f);
      }
      f->close_section();
    }
  };
}

void CrushWrapper::dump_tree(Formatter *f) const
{
  assert(f);
  TreeDumper(this).dump(f);
}

void CrushWrapper::dump_tunables(Formatter *f) const
{
  f->dump_int("choose_local_tries", get_choose_local_tries());
  f->dump_int("choose_local_fallback_tries", get_choose_local_fallback_tries());
  f->dump_int("choose_total_tries", get_choose_total_tries());
  f->dump_int("chooseleaf_descend_once", get_chooseleaf_descend_once());
  f->dump_int("chooseleaf_vary_r", get_chooseleaf_vary_r());
  f->dump_int("straw_calc_version", get_straw_calc_version());
  f->dump_int("allowed_bucket_algs", get_allowed_bucket_algs());

  // be helpful about it
  if (has_hammer_tunables())
    f->dump_string("profile", "hammer");
  else if (has_firefly_tunables())
    f->dump_string("profile", "firefly");
  else if (has_bobtail_tunables())
    f->dump_string("profile", "bobtail");
  else if (has_argonaut_tunables())
    f->dump_string("profile", "argonaut");
  else
    f->dump_string("profile", "unknown");
  f->dump_int("optimal_tunables", (int)has_optimal_tunables());
  f->dump_int("legacy_tunables", (int)has_legacy_tunables());

  f->dump_int("require_feature_tunables", (int)has_nondefault_tunables());
  f->dump_int("require_feature_tunables2", (int)has_nondefault_tunables2());
  f->dump_int("require_feature_tunables3", (int)has_nondefault_tunables3());
  f->dump_int("has_v2_rules", (int)has_v2_rules());
  f->dump_int("has_v3_rules", (int)has_v3_rules());
  f->dump_int("has_v4_buckets", (int)has_v4_buckets());
}

void CrushWrapper::dump_rules(Formatter *f) const
{
  for (int i=0; i<get_max_rules(); i++) {
    if (!rule_exists(i))
      continue;
    dump_rule(i, f);
  }
}

void CrushWrapper::dump_rule(int ruleset, Formatter *f) const
{
  f->open_object_section("rule");
  f->dump_int("rule_id", ruleset);
  if (get_rule_name(ruleset))
    f->dump_string("rule_name", get_rule_name(ruleset));
  f->dump_int("ruleset", get_rule_mask_ruleset(ruleset));
  f->dump_int("type", get_rule_mask_type(ruleset));
  f->dump_int("min_size", get_rule_mask_min_size(ruleset));
  f->dump_int("max_size", get_rule_mask_max_size(ruleset));
  f->open_array_section("steps");
  for (int j=0; j<get_rule_len(ruleset); j++) {
    f->open_object_section("step");
    switch (get_rule_op(ruleset, j)) {
    case CRUSH_RULE_NOOP:
      f->dump_string("op", "noop");
      break;
    case CRUSH_RULE_TAKE:
      f->dump_string("op", "take");
      {
        int item = get_rule_arg1(ruleset, j);
        f->dump_int("item", item);

        const char *name = get_item_name(item);
        f->dump_string("item_name", name ? name : "");
      }
      break;
    case CRUSH_RULE_EMIT:
      f->dump_string("op", "emit");
      break;
    case CRUSH_RULE_CHOOSE_FIRSTN:
      f->dump_string("op", "choose_firstn");
      f->dump_int("num", get_rule_arg1(ruleset, j));
      f->dump_string("type", get_type_name(get_rule_arg2(ruleset, j)));
      break;
    case CRUSH_RULE_CHOOSE_INDEP:
      f->dump_string("op", "choose_indep");
      f->dump_int("num", get_rule_arg1(ruleset, j));
      f->dump_string("type", get_type_name(get_rule_arg2(ruleset, j)));
      break;
    case CRUSH_RULE_CHOOSELEAF_FIRSTN:
      f->dump_string("op", "chooseleaf_firstn");
      f->dump_int("num", get_rule_arg1(ruleset, j));
      f->dump_string("type", get_type_name(get_rule_arg2(ruleset, j)));
      break;
    case CRUSH_RULE_CHOOSELEAF_INDEP:
      f->dump_string("op", "chooseleaf_indep");
      f->dump_int("num", get_rule_arg1(ruleset, j));
      f->dump_string("type", get_type_name(get_rule_arg2(ruleset, j)));
      break;
    case CRUSH_RULE_SET_CHOOSE_TRIES:
      f->dump_string("op", "set_choose_tries");
      f->dump_int("num", get_rule_arg1(ruleset, j));
      break;
    case CRUSH_RULE_SET_CHOOSELEAF_TRIES:
      f->dump_string("op", "set_chooseleaf_tries");
      f->dump_int("num", get_rule_arg1(ruleset, j));
      break;
    default:
      f->dump_int("opcode", get_rule_op(ruleset, j));
      f->dump_int("arg1", get_rule_arg1(ruleset, j));
      f->dump_int("arg2", get_rule_arg2(ruleset, j));
    }
    f->close_section();
  }
  f->close_section();
  f->close_section();
}

void CrushWrapper::list_rules(Formatter *f) const
{
  for (int rule = 0; rule < get_max_rules(); rule++) {
    if (!rule_exists(rule))
      continue;
    f->dump_string("name", get_rule_name(rule));
  }
}

class CrushTreePlainDumper : public CrushTreeDumper::Dumper<ostream> {
public:
  typedef CrushTreeDumper::Dumper<ostream> Parent;

  CrushTreePlainDumper(const CrushWrapper *crush)
    : Parent(crush) {}

  void dump(ostream *out) {
    *out << "ID\tWEIGHT\tTYPE NAME\n";
    Parent::dump(out);
  }

protected:
  virtual void dump_item(const CrushTreeDumper::Item &qi, ostream *out) {
    *out << qi.id << "\t"
	 << weightf_t(qi.weight) << "\t";

    for (int k=0; k < qi.depth; k++)
      *out << "\t";

    if (qi.is_bucket())
    {
      *out << crush->get_type_name(crush->get_bucket_type(qi.id)) << " "
	   << crush->get_item_name(qi.id);
    }
    else
    {
      *out << "osd." << qi.id;
    }
    *out << "\n";
  }
};


class CrushTreeFormattingDumper : public CrushTreeDumper::FormattingDumper {
public:
  typedef CrushTreeDumper::FormattingDumper Parent;

  CrushTreeFormattingDumper(const CrushWrapper *crush)
    : Parent(crush) {}

  void dump(Formatter *f) {
    f->open_array_section("nodes");
    Parent::dump(f);
    f->close_section();
    f->open_array_section("stray");
    f->close_section();
  }
};


void CrushWrapper::dump_tree(ostream *out, Formatter *f) const
{
  if (out)
    CrushTreePlainDumper(this).dump(out);
  if (f)
    CrushTreeFormattingDumper(this).dump(f);
}

void CrushWrapper::generate_test_instances(list<CrushWrapper*>& o)
{
  o.push_back(new CrushWrapper);
  // fixme
}

int CrushWrapper::_get_osd_pool_default_crush_replicated_ruleset(CephContext *cct,
                                                                 bool quiet)
{
  // osd_pool_default_crush_rule is deprecated, default is -1
  int crush_ruleset = cct->_conf->osd_pool_default_crush_rule;
  if (crush_ruleset == -1) {
    // osd_pool_default_crush_replicated_ruleset default is 
    // CEPH_DEFAULT_CRUSH_REPLICATED_RULESET
    crush_ruleset = cct->_conf->osd_pool_default_crush_replicated_ruleset;
  } else if (!quiet) {
    ldout(cct, 0) << "osd_pool_default_crush_rule is deprecated "
                  << "use osd_pool_default_crush_replicated_ruleset instead"
                  << dendl;
    ldout(cct, 0) << "osd_pool_default_crush_rule = "
                  << cct->_conf-> osd_pool_default_crush_rule << " overrides "
                  << "osd_pool_default_crush_replicated_ruleset = "
                  << cct->_conf->osd_pool_default_crush_replicated_ruleset
                  << dendl;
  }

  return crush_ruleset;
}

/**
 * Determine the default CRUSH ruleset ID to be used with
 * newly created replicated pools.
 *
 * @returns a ruleset ID (>=0) or -1 if no suitable ruleset found
 */
int CrushWrapper::get_osd_pool_default_crush_replicated_ruleset(CephContext *cct)
{
  int crush_ruleset = _get_osd_pool_default_crush_replicated_ruleset(cct,
                                                                     false);
  if (crush_ruleset == CEPH_DEFAULT_CRUSH_REPLICATED_RULESET) {
    crush_ruleset = find_first_ruleset(pg_pool_t::TYPE_REPLICATED);
  } else if (!ruleset_exists(crush_ruleset)) {
    crush_ruleset = -1; // match find_first_ruleset() retval
  }

  return crush_ruleset;
}

bool CrushWrapper::is_valid_crush_name(const string& s)
{
  if (s.empty())
    return false;
  for (string::const_iterator p = s.begin(); p != s.end(); ++p) {
    if (!(*p == '-') &&
	!(*p == '_') &&
	!(*p == '.') &&
	!(*p >= '0' && *p <= '9') &&
	!(*p >= 'A' && *p <= 'Z') &&
	!(*p >= 'a' && *p <= 'z'))
      return false;
  }
  return true;
}

bool CrushWrapper::is_valid_crush_loc(CephContext *cct,
                                      const map<string,string>& loc)
{
  for (map<string,string>::const_iterator l = loc.begin(); l != loc.end(); ++l) {
    if (!is_valid_crush_name(l->first) ||
        !is_valid_crush_name(l->second)) {
      ldout(cct, 1) << "loc["
                    << l->first << "] = '"
                    << l->second << "' not a valid crush name ([A-Za-z0-9_-.]+)"
                    << dendl;
      return false;
    }
  }
  return true;
}
