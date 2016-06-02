// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <vector>

#include "snap_set_diff.h"
#include "common/ceph_context.h"
#include "include/rados/librados.hpp"
#include "include/interval_set.h"
#include "common/debug.h"

#define dout_subsys ceph_subsys_rados

/**
 * calculate intervals/extents that vary between two snapshots
 */

// called by C_DiffObject::compute_diffs, ObjectCopyRequest<I>::compute_diffs
void calc_snap_set_diff(CephContext *cct, const librados::snap_set_t& snap_set,
			librados::snap_t start, librados::snap_t end,
			interval_set<uint64_t> *diff, uint64_t *end_size,
                        bool *end_exists)
{
  ldout(cct, 10) << "calc_snap_set_diff start " << start << " end " << end
		 << ", snap_set seq " << snap_set.seq << dendl;

  bool saw_start = false;
  uint64_t start_size = 0;

  diff->clear();
  *end_size = 0;
  *end_exists = false;

  // snap_set::clones in ascending order
  for (vector<librados::clone_info_t>::const_iterator r = snap_set.clones.begin();
       r != snap_set.clones.end();
       ) {

    // iterate each clone object

    // make an interval, and hide the fact that the HEAD doesn't
    // include itself in the snaps list

    librados::snap_t a, b;

    if (r->cloneid == librados::SNAP_HEAD) {

      // i.e., CEPH_NOSNAP

      // head is valid starting from right after the last seen seq
      a = snap_set.seq + 1;

      b = librados::SNAP_HEAD;
    } else {
      // obc->obs.oi.snaps is in descending order, but got the ascending
      // order returned in handling CEPH_OSD_OP_LIST_SNAPS

      a = r->snaps[0];

      // note: b might be < r->cloneid if a snap has been trimmed.
      b = r->snaps[r->snaps.size()-1];
    }

    // [a, b]: at snapid a we created a clone object, the HEAD were changing until
    // snapid b, at snapid b a new clone object was created

    ldout(cct, 20) << " clone " << r->cloneid << " snaps " << r->snaps
		   << " -> [" << a << "," << b << "]"
		   << " size " << r->size << " overlap to next " << r->overlap << dendl;

    // filter those clone objects that their snapshots, i.e., [a, b], have
    // overlap with the [start, end]

    if (b < start) {
      // this is before start, skip those snapshots before start

      ++r;
      continue;
    }

    // [start, b]

    if (!saw_start) {

      if (start < a) {

        // [start, a, b]

	ldout(cct, 20) << "  start, after " << start << dendl;

	// this means the object didn't exist at start
	if (r->size)
	  diff->insert(0, r->size);

	start_size = 0;
      } else {

        // [a, start, b], snapid start in [a, b], so the clone object exists at snapid start

	ldout(cct, 20) << "  start" << dendl;

	start_size = r->size;
      }

      saw_start = true;
    }

    *end_size = r->size;

    if (end < a) {

      // [start, end, a, b]

      ldout(cct, 20) << " past end " << end << ", end object does not exist" << dendl;

      *end_exists = false;
      diff->clear();

      if (start_size) {

        // the clone object exists at snapid start

	diff->insert(0, start_size);
      }

      break;
    }

    if (end <= b) {

      // [a, end, b], snapid end in [a, b], so the clone object exists at snapid end

      ldout(cct, 20) << " end" << dendl;

      *end_exists = true;

      break;
    }

    // [start, a, b, end] or [a, start, b, end] or [start, a, b, end]

    // start with the max(this size, next size), and subtract off any
    // overlap between the current clone object and the next clone object

    const vector<pair<uint64_t, uint64_t> > *overlap = &r->overlap;
    interval_set<uint64_t> diff_to_next;
    uint64_t max_size = r->size;

    // point to the next clone object
    ++r;
    if (r != snap_set.clones.end()) {

      // the next clone object exists

      if (r->size > max_size)
	max_size = r->size;
    }

    if (max_size)
      diff_to_next.insert(0, max_size);

    for (vector<pair<uint64_t, uint64_t> >::const_iterator p = overlap->begin();
	 p != overlap->end();
	 ++p) {

      // erase the overlapped area, so we get the diff area between the current
      // clone object and the next clone object

      diff_to_next.erase(p->first, p->second);
    }

    ldout(cct, 20) << "  diff_to_next " << diff_to_next << dendl;

    diff->union_of(diff_to_next);

    ldout(cct, 20) << "  diff now " << *diff << dendl;
  } // for each clone objects
}
