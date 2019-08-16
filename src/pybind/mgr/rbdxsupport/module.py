"""
RBDx support module
"""

import radosx
import rbdx

import datetime
import json
import six
import threading
import traceback

import functools
from timeit import default_timer as perf_timer
from __builtin__ import True

try:
    import queue
except ImportError:
    import Queue as queue

from mgr_module import MgrModule

TICK_INTERVAL = 1.0

TASKQ_MAX_SIZE = 32
TASKQ_ENQ_TIMEOUT = 5.0
WORKER_NUM = 3

PRIO_H = 1
PRIO_M = 2
PRIO_L = 3

ACTION_LIST_NAME = 'list name'
ACTION_LIST_CHILD = 'list child'
ACTION_LIST_INFO = 'list info'

# TICKS should be prime number and never have two ticks have the same number
# see PoolScheduler._next_tick_task
LIST_NAME_TICKS = 7
LIST_CHILD_TICKS = 11

LIST_INFO_INCOMPLETE_TICKS = 3  # info incomplete, v2
LIST_INFO_COMPLETE_TICKS = 13  # stable, v1
LIST_INFO_SYNC_TICKS = 2  # stable, sync, v2

LIST_DU_ACTIVE_TICKS = 5  # active, stable, v3
LIST_DU_INACTIVE_INCOMPLETE_TICKS = 17  # active, du incomplete, v3
LIST_DU_SYNC_TICKS = 19  # inactive, du incomplete, v3

LIST_INFO_INCOMPLETE_BATCH = 100
LIST_INFO_COMPLETE_BATCH = 20
LIST_INFO_SYNC_BATCH = 2

LIST_DU_ACTIVE_BATCH = 20
LIST_DU_INACTIVE_INCOMPLETE_BATCH = 50
LIST_DU_SYNC_BATCH = 5

FILTER_LAST_INFO_UPDATE_TICKS = 6
FILTER_LAST_INFO_V2_UPDATE_TICKS = 8
FILTER_LAST_DU_UPDATE_TICKS = 300

PRUNE_TICKS = 7

PRIME_SEQ = [11, 19, 31, 43, 59, 71, 83, 101, 109, 131, 149, 163, 179]


def time_logging(method, filter=0.005):
    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        start = perf_timer()
        ret = method(self, *args, **kwargs)
        end = perf_timer()
        elapsed = end - start
        if elapsed > filter:
            self.log.debug('{0} elapsed: {1} seconds'.format(method.__name__, elapsed))
        return ret
    return wrapper


class Task(object):

    def __init__(self, pool, action, prio):
        self.pool = pool
        self.action = action
        self.prio = prio
        self.private = None
        self.done = False
        self.r = 0
        self.event = None

    def __cmp__(self, rhs):
        return cmp(self.prio, rhs.prio)

    def wait(self):
        self.event.wait()


class Scheduler(object):

    class PoolScheduler(object):

        lock = threading.Lock()
        cur_tick_task = None
        cur_ext_task = None

        def __init__(self, module, pool, taskq):
            self.module = module
            self.pool = pool
            self.taskq = taskq
            self.ticks = PRIME_SEQ[pool % len(PRIME_SEQ)] + pool  # a little random

            self.info_incomplete_snapshot = None
            self.info_complete_snapshot = None
            self.info_sync_snapshot = None

            self.list_du_active_snapshot = None
            self.list_du_inactive_incomplete_snapshot = None
            self.list_du_sync_snapshot = None

        @property
        def log(self):
            return self.module.log

        @property
        def now(self):
            return self.module.now

        def _snapshot_images(self, filters=None):
            return self.module.snapshot_images(self.pool, filters=filters)

        @time_logging
        def _next_tick_task(self, cur_task):

            def is_info_complete(image):
                # means info v2+ complete
                if image.get('watchers') is None or \
                        image.get('snaps') is None:
                    # the second condition is not needed actually, since we
                    # always fill the complete image info
                    return False

                snaps = image['snaps']
                for _, snap in six.iteritems(snaps):
                    if snap.get('name') is None:
                        return False

                snapc = image['snapc']
                snapc_ids = sorted(snapc['snaps'])
                snaps_ids = sorted(list(snaps.keys()))
                if snapc_ids != snaps_ids:
                    return False
                return True

            def is_active(image):
                watchers = image.get('watchers')
                if watchers is None:
                    return False

                return len(watchers) > 0

            def is_inactive(image):
                watchers = image.get('watchers')
                if watchers is None:
                    return False

                return len(watchers) == 0

            def is_du_complete(image):
                du = image.get('du')
                if du is None:
                    return False

                return True

            def filter_info_incomplete_images(image):
                return not is_info_complete(image)

            def filter_info_complete_images(image):
                return is_info_complete(image)

            def filter_du_active_images(image):
                return is_active(image)

            def filter_du_inactive_incomplete_images(image):
                return is_inactive(image) and not is_du_complete(image)

            def filter_info_last_update(now, ticks):

                def filter(image):
                    if now < image.get('info_update', 0) or \
                        now - image.get('info_update', 0) > ticks:
                        return True

                return filter

            def filter_info_v2_last_update(now, ticks):

                def filter(image):
                    if now < image.get('info_v2_update', 0) or \
                        now - image.get('info_v2_update', 0) > ticks:
                        return True

                return filter

            def filter_du_last_update(now, ticks):

                def filter(image):
                    if now < image.get('du_update', 0) or \
                        now - image.get('du_update', 0) > ticks:
                        return True

                return filter

            if cur_task is None:
                # the initial start, populate the basic image data
                task_private = {
                    'v': 2,
                }
                task = Task(self.pool, ACTION_LIST_INFO, PRIO_H)
                task.private = task_private
                return task
            if not cur_task.done:
                # the current task is still running
                return None

            # start one and only one new task
            # those TICKS are chosen deliberately
            if self.ticks % LIST_NAME_TICKS == 0:
                task = Task(self.pool, ACTION_LIST_NAME, PRIO_M)
                return task

            if self.ticks % LIST_CHILD_TICKS == 0:
                task = Task(self.pool, ACTION_LIST_CHILD, PRIO_M)
                return task

            if self.ticks % LIST_INFO_INCOMPLETE_TICKS == 0:

                self.info_incomplete_snapshot = self._snapshot_images(
                    filters=[
                        filter_info_incomplete_images,
                    ]
                )
#                 if self.info_incomplete_snapshot is None:
#                     self.info_incomplete_snapshot = self._snapshot_images(
#                         filter=filter_info_incomplete_images
#                     )
                if not self.info_incomplete_snapshot:
                    return None

                # sort
                self.info_incomplete_snapshot = sorted(
                    six.iteritems(self.info_incomplete_snapshot),
                    key=lambda x: x[1].get('info_v2_update', 0)
                )

                batch = self.info_incomplete_snapshot[
                    0:LIST_INFO_INCOMPLETE_BATCH
                    ]

                task_private = {
                    'v': 2,
                    'batch': batch,
                }
                task = Task(self.pool, ACTION_LIST_INFO, PRIO_M)
                task.private = task_private
                return task

            if self.ticks % LIST_INFO_COMPLETE_TICKS == 0:

                self.info_complete_snapshot = self._snapshot_images(
                    filters=[
                        filter_info_complete_images,
                        filter_info_last_update(self.now, FILTER_LAST_INFO_UPDATE_TICKS),
                        filter_info_v2_last_update(self.now, FILTER_LAST_INFO_V2_UPDATE_TICKS),
                    ]
                )
#                 if self.info_complete_snapshot is None:
#                     self.info_complete_snapshot = self._snapshot_images(
#                         filter=filter_info_complete_images
#                     )
                if not self.info_complete_snapshot:
                    return None

                # sort
                self.info_complete_snapshot = sorted(
                    six.iteritems(self.info_complete_snapshot),
                    key=lambda x: x[1].get('info_update', 0)
                )

                batch = self.info_complete_snapshot[
                    0:LIST_INFO_COMPLETE_BATCH
                ]

                task_private = {
                    'v': 1,
                    'batch': batch,
                }
                task = Task(self.pool, ACTION_LIST_INFO, PRIO_M)
                task.private = task_private
                return task

            if self.ticks % LIST_INFO_SYNC_TICKS == 0:

                self.info_sync_snapshot = self._snapshot_images(
                    filters=[
                        filter_info_v2_last_update(self.now, FILTER_LAST_INFO_V2_UPDATE_TICKS),
                    ]
                )
#                 if self.info_sync_snapshot is None:
#                     self.info_sync_snapshot = self._snapshot_images(
#                         filter=filter_info_complete_images
#                     )
                if not self.info_sync_snapshot:
                    return None

                # sort
                self.info_sync_snapshot = sorted(
                    six.iteritems(self.info_sync_snapshot),
                    key=lambda x: x[1].get('info_v2_update', 0)
                )

                batch = self.info_sync_snapshot[
                    0:LIST_INFO_SYNC_BATCH
                ]

                task_private = {
                    'v': 2,
                    'batch': batch,
                }
                task = Task(self.pool, ACTION_LIST_INFO, PRIO_M)
                task.private = task_private
                return task

            if self.ticks % LIST_DU_ACTIVE_TICKS == 0:

                self.du_active_snapshot = self._snapshot_images(
                    filters=[
                        filter_du_active_images,
                        filter_du_last_update(self.now, FILTER_LAST_DU_UPDATE_TICKS),
                    ]
                )
#                 if self.du_active_snapshot is None:
#                     self.du_active_snapshot = self._snapshot_images(
#                         filter=filter_active_du_complete_images
#                     )
                if not self.du_active_snapshot:
                    return None

                # sort
                self.du_active_snapshot = sorted(
                    six.iteritems(self.du_active_snapshot),
                    key=lambda x: x[1].get('du_update', 0)
                )

                batch = self.du_active_snapshot[
                    0:LIST_DU_ACTIVE_BATCH
                ]

                task_private = {
                    'v': 3,
                    'batch': batch,
                }
                task = Task(self.pool, ACTION_LIST_INFO, PRIO_L)
                task.private = task_private
                return task

            if self.ticks % LIST_DU_INACTIVE_INCOMPLETE_TICKS == 0:

                self.du_inactive_incomplete_snapshot = self._snapshot_images(
                    filters=[
                        filter_du_inactive_incomplete_images,
                    ]
                )
#                 if self.du_inactive_incomplete_snapshot is None:
#                     self.du_inactive_incomplete_snapshot = self._snapshot_images(
#                         filter=filter_active_du_incomplete_images
#                     )

                if not self.du_inactive_incomplete_snapshot:
                    return None

                # sort
                self.du_inactive_incomplete_snapshot = sorted(
                    six.iteritems(self.du_inactive_incomplete_snapshot),
                    key=lambda x: x[1].get('du_update', 0)
                )

                batch = self.du_inactive_incomplete_snapshot[
                    0:LIST_DU_INACTIVE_INCOMPLETE_BATCH
                ]

                task_private = {
                    'v': 3,
                    'batch': batch,
                }
                task = Task(self.pool, ACTION_LIST_INFO, PRIO_L)
                task.private = task_private
                return task

            if self.ticks % LIST_DU_SYNC_TICKS == 0:
                
                self.du_sync_snapshot = self._snapshot_images(
                    filters=[
                        filter_du_last_update(self.now, FILTER_LAST_DU_UPDATE_TICKS),
                    ]
                )
#                 if self.du_sync_snapshot is None:
#                     self.du_sync_snapshot = self._snapshot_images(
#                         filter=filter_inactive_du_incomplete_images
#                     )
                if not self.du_sync_snapshot:
                    return None

                # sort
                self.du_sync_snapshot = sorted(
                    six.iteritems(self.du_sync_snapshot),
                    key=lambda x: x[1].get('du_update', 0)
                )

                batch = self.du_sync_snapshot[
                    0:LIST_DU_SYNC_BATCH
                ]

                task_private = {
                    'v': 3,
                    'batch': batch,
                }
                task = Task(self.pool, ACTION_LIST_INFO, PRIO_L)
                task.private = task_private
                return task

            return None

        def tick(self):
            with self.lock:
                self.ticks += 1

                self.log.debug('scheduler({0}) ticking{1}..'.format(
                    self.pool, self.ticks))

                task = self._next_tick_task(self.cur_tick_task)
                if task is not None:
                    try:
                        self.module.log.debug('taskq size: {0}'.format(self.taskq.qsize()))
                        self.taskq.put(task, timeout=TASKQ_ENQ_TIMEOUT)
                    except queue.Full:
                        self.module.log.warn('taskq is full')
                        return None
                    self.cur_tick_task = task

        def schedule(self, action, **kwargs):
            self.log.debug('scheduler({0}) scheduling..'.format(self.pool))
            with self.lock:
                task = self.cur_ext_task
                if task is not None:
                    if not task.done:
                        # we are in the context of finisher, so should never happen..
                        if self.cur_ext_task.action == action:
                            return task
                        # busy
                        return None

                # process kwargs
                task_private = None
                if action == ACTION_LIST_NAME:
                    pass
                elif action == ACTION_LIST_CHILD:
                    pass
                elif action == ACTION_LIST_INFO:
                    task_private = {
                        'v': kwargs.get('v', 1),
                    }
                else:
                    self.log.warn('unexpected action: {0}'.format(action))
                    return None

                task = Task(self.pool, action, PRIO_H)
                task.private = task_private
                task.event = threading.Event()
                try:
                    self.log.debug('taskq size: {0}'.format(self.taskq.qsize()))
                    self.taskq.put(task, timeout=TASKQ_ENQ_TIMEOUT)
                except queue.Full:
                    self.log.warn('taskq is full')
                    return None
                self.cur_ext_task = task
                return task

    lock = threading.Lock()
    schedulers = {}

    def __init__(self, module, taskq):
        self.module = module
        self.taskq = taskq

    @property
    def log(self):
        return self.module.log

    @property
    def now(self):
        return self.module.now

    def schedule(self, pool, action, **kwargs):
        scheduler = None
        with self.lock:
            scheduler = self.schedulers.get(pool)
        if scheduler is None:
            return None
        return scheduler.schedule(action, **kwargs)

    def tick(self, pools):
        schedulers = {}
        with self.lock:
            old = self.schedulers

            for p in pools:
                scheduler = old.get(p)
                if scheduler is None:
                    schedulers[p] = Scheduler.PoolScheduler(self.module, p, self.taskq)
                else:
                    schedulers[p] = scheduler

            self.schedulers = schedulers

        for _, s in six.iteritems(schedulers):
            s.tick()

class Module(MgrModule):
    COMMANDS = [
        {
            "cmd": "rbdx list-name "
                   "name=pool_id,type=CephInt,req=true "
                   "name=refresh,type=CephChoices,strings=--refresh,req=false",
            "desc": "List image names",
            "perm": "r"
        },
        {
            "cmd": "rbdx list-info "
                   "name=pool_id,type=CephInt,req=true "
                   "name=v2,type=CephChoices,strings=--v2,req=false "
                   "name=v3,type=CephChoices,strings=--v3,req=false "
                   "name=refresh,type=CephChoices,strings=--refresh,req=false",
            "desc": "List image infos",
            "perm": "r"
        },
        {
            "cmd": "rbdx list-du "
                   "name=pool_id,type=CephInt,req=true "
                   "name=refresh,type=CephChoices,strings=--refresh,req=false",
            "desc": "List image disk usages",
            "perm": "r"
        },
    ]
    MODULE_OPTIONS = []

    lock = threading.Lock()
    data = {}

    def __init__(self, *args, **kwargs):
        super(Module, self).__init__(*args, **kwargs)

        self._radosx = None
        self._rbdx = None

        self.ticks = 0

        self.taskq = queue.PriorityQueue(maxsize=TASKQ_MAX_SIZE)
        self.scheduler = Scheduler(self, self.taskq)

        self.stop = threading.Event()
        self.thread = threading.Thread(target=self.run)
        self.thread.start()

    @property
    def radosx(self):
        """
        A libradosx instance to be shared by any classes within
        this mgr module that want one.
        """
        if self._radosx:
            return self._radosx

        ctx_capsule = self.get_context()
        self._radosx = radosx.Rados()
        self._radosx.init_with_context(ctx_capsule)
        self._radosx.connect()

        return self._radosx

    @property
    def rbdx(self):
        """
        A librbdx instance to be shared by any classes within
        this mgr module that want one.
        """
        if self._rbdx:
            return self._rbdx

        self._rbdx = rbdx.xRBD()

        return self._rbdx

    @property
    def now(self):
        # w/o lock should be ok
        return self.ticks

    def _get_pools(self):
        # FIXME: efficient implementation
        osdmap = self.get_osdmap()
        d = osdmap.dump()
        return [int(p['pool']) for p in d['pools']]

    @time_logging
    def _list_name(self, pool):
        ioctx = radosx.IoCtx()
        r = self.radosx.ioctx_create2(pool, ioctx)
        if r < 0:
            return None, r

        # std::map<image id, image name>
        l = rbdx.Map_string_2_string()
        r = self.rbdx.list(ioctx, l)
        return l, r
    
    @time_logging
    def _update_name(self, pool, l):
        with self.lock:
            old = self.data.get(pool, {})
            images = {}
            for k, v in l.items():
                images[k] = old.get(k)
                if images[k] is None:
                    images[k] = dict(id=k, name=v)
                else:
                    images[k]['name'] = v

            self.data[pool] = images

    @time_logging
    def _list_info(self, pool, batch=None):
        ioctx = radosx.IoCtx()
        r = self.radosx.ioctx_create2(pool, ioctx)
        if r < 0:
            return None, r

        # std::map<image id, <image info, r>>
        l = rbdx.Map_string_2_pair_image_info_t_int()
        
        if batch is not None:
            images = rbdx.Vector_string()
            for k, _ in batch:
                # std::vector<image id>
                images.append(k)
            r = self.rbdx.list_info(ioctx, images, l)
        else:
            r = self.rbdx.list_info(ioctx, l)
        return l, r

    @time_logging
    def _update_info(self, pool, l):

        def pythonify(info):
            return {
                'id': info.id,
                'name': info.name,
                'order': info.order,
                'size': info.size,
                'stripe_unit': info.stripe_unit,
                'stripe_count': info.stripe_count,
                'features': info.features,
                'flags': info.flags,
                'snapc': {
                    'seq': info.snapc.seq,
                    'snaps': [s for s in info.snapc.snaps],
                },
                'parent': {
                    'spec': {
                        'pool_id': info.parent.spec.pool_id,
                        'image_id': info.parent.spec.image_id,
                        'snap_id': info.parent.spec.snap_id,
                    },
                    'overlap': info.parent.overlap,
                },
                'timestamp': {
                    'tv_sec': info.timestamp.tv_sec,
                    'tv_nsec': info.timestamp.tv_nsec,
                },
                'data_pool_id': info.data_pool_id,
                'watchers': [w for w in info.watchers],
                'qos': {
                    'iops': info.qos.iops,
                    'bps': info.qos.bps,
                },
            }

        def update(image, info):
            image['id'] = info.id
            image['name'] = info.name if info.name else image.get('name')
            image['order'] = info.order
            image['size'] = info.size
            image['stripe_unit'] = info.stripe_unit
            image['stripe_count'] = info.stripe_count
            image['features'] = info.features
            image['flags'] = info.flags
            image['snapc'] = {
                'seq': info.snapc.seq,
                'snaps': [s for s in info.snapc.snaps],
            }
            image['parent'] = {
                'spec': {
                    'pool_id': info.parent.spec.pool_id,
                    'image_id': info.parent.spec.image_id,
                    'snap_id': info.parent.spec.snap_id,
                },
                'overlap': info.parent.overlap,
            }
            image['timestamp'] = {
                'tv_sec': info.timestamp.tv_sec,
                'tv_nsec': info.timestamp.tv_nsec,
            }
            image['data_pool_id'] = info.data_pool_id
            image['watchers'] = [w for w in info.watchers]
            image['qos'] = {
                'iops': info.qos.iops,
                'bps': info.qos.bps,
            }

        with self.lock:
            images = self.data.get(pool, {})
            for k, v in l.items():
                info, r = v
                if r < 0:
                    continue

                if images.get(k) is None:
                    images[k] = pythonify(info)
                else:
                    update(images[k], info)

                images[k]['info_update'] = self.ticks

            self.data[pool] = images

    @time_logging
    def _list_info_v2(self, pool, batch=None):
        ioctx = radosx.IoCtx()
        r = self.radosx.ioctx_create2(pool, ioctx)
        if r < 0:
            return None, r

        # std::map<image id, std::pair<image info v2, r>>
        l = rbdx.Map_string_2_pair_image_info_v2_t_int()
        if batch is not None:
            images = rbdx.Vector_string()
            for k, _ in batch:
                # std::vector<image id>
                images.append(k)
            r = self.rbdx.list_info_v2(ioctx, images, l)
        else:
            r = self.rbdx.list_info_v2(ioctx, l)
        return l, r

    @time_logging
    def _update_info_v2(self, pool, l):

        def pythonify_snap(snap):
            return {
                'id': snap.id,
                'name': snap.name,
                'snap_ns_type': int(snap.snap_ns_type),
                'size': snap.size,
                'features': snap.features,
                'flags': snap.flags,
                'protection_status': int(snap.protection_status),
                'timestamp': {
                    'tv_sec': snap.timestamp.tv_sec,
                    'tv_nsec': snap.timestamp.tv_nsec,
                },
            }

        def pythonify(info):
            return {
                'id': info.id,
                'name': info.name,
                'order': info.order,
                'size': info.size,
                'stripe_unit': info.stripe_unit,
                'stripe_count': info.stripe_count,
                'features': info.features,
                'flags': info.flags,
                'snapc': {
                    'seq': info.snapc.seq,
                    'snaps': [s for s in info.snapc.snaps],
                },
                'parent': {
                    'spec': {
                        'pool_id': info.parent.spec.pool_id,
                        'image_id': info.parent.spec.image_id,
                        'snap_id': info.parent.spec.snap_id,
                    },
                    'overlap': info.parent.overlap,
                },
                'timestamp': {
                    'tv_sec': info.timestamp.tv_sec,
                    'tv_nsec': info.timestamp.tv_nsec,
                },
                'data_pool_id': info.data_pool_id,
                'watchers': [w for w in info.watchers],
                'qos': {
                    'iops': info.qos.iops,
                    'bps': info.qos.bps,
                },
                'snaps': {
                    k: pythonify_snap(v) for k, v in info.snaps.items()
                },
            }
            
        def update_snap(snap, sinfo):
            snap['id'] = sinfo.id
            snap['name'] = sinfo.name
            snap['snap_ns_type'] = int(sinfo.snap_ns_type)
            snap['size'] = sinfo.size
            snap['features'] = sinfo.features
            snap['flags'] = sinfo.flags
            snap['protection_status'] = int(sinfo.protection_status)
            snap['timestamp'] = {
                'tv_sec': sinfo.timestamp.tv_sec,
                'tv_nsec': sinfo.timestamp.tv_nsec,
            }

        def update(image, info):
            image['id'] = info.id
            image['name'] = info.name if info.name else image.get('name')
            image['order'] = info.order
            image['size'] = info.size
            image['stripe_unit'] = info.stripe_unit
            image['stripe_count'] = info.stripe_count
            image['features'] = info.features
            image['flags'] = info.flags
            image['snapc'] = {
                'seq': info.snapc.seq,
                'snaps': [s for s in info.snapc.snaps],
            }
            image['parent'] = {
                'spec': {
                    'pool_id': info.parent.spec.pool_id,
                    'image_id': info.parent.spec.image_id,
                    'snap_id': info.parent.spec.snap_id,
                },
                'overlap': info.parent.overlap,
            }
            image['timestamp'] = {
                'tv_sec': info.timestamp.tv_sec,
                'tv_nsec': info.timestamp.tv_nsec,
            }
            image['data_pool_id'] = info.data_pool_id
            image['watchers'] = [w for w in info.watchers]
            image['qos'] = {
                'iops': info.qos.iops,
                'bps': info.qos.bps,
            }
            old = image.get('snaps', {})
            snaps = {}
            for k, v in info.snaps.items():
                snaps[k] = old.get(k)
                if snaps[k] is None:
                    snaps[k] = pythonify_snap(v)
                else:
                    update_snap(snaps[k], v)
            image['snaps'] = snaps

        with self.lock:
            if self.data.get(pool) is None:
                self.data[pool] = {}

            images = self.data[pool]
            for k, v in l.items():
                info, r = v
                if r < 0:
                    continue

                if images.get(k) is None:
                    images[k] = pythonify(info)
                else:
                    update(images[k], info)

                images[k]['info_v2_update'] = self.ticks

            self.data[pool] = images

    @time_logging
    def _list_info_v3(self, pool, batch=None):
        ioctx = radosx.IoCtx()
        r = self.radosx.ioctx_create2(pool, ioctx)
        if r < 0:
            return None, r

        # std::map<image id, std::pair<image info v3, r>>
        l = rbdx.Map_string_2_pair_image_info_v3_t_int()
        if batch is not None:
            images = rbdx.Vector_string()
            for k, _ in batch:
                # std::vector<image id>
                images.append(k)
            r = self.rbdx.list_info_v3(ioctx, images, l)
        else:
            r = self.rbdx.list_info_v3(ioctx, l)
        return l, r

    @time_logging
    def _update_info_v3(self, pool, l):

        def pythonify(info):
            return {
                'id': info.id,
                'name': info.name,
                'order': info.order,
                'size': info.size,
                'stripe_unit': info.stripe_unit,
                'stripe_count': info.stripe_count,
                'features': info.features,
                'flags': info.flags,
                'snapc': {
                    'seq': info.snapc.seq,
                    'snaps': [s for s in info.snapc.snaps],
                },
                'parent': {
                    'spec': {
                        'pool_id': info.parent.spec.pool_id,
                        'image_id': info.parent.spec.image_id,
                        'snap_id': info.parent.spec.snap_id,
                    },
                    'overlap': info.parent.overlap,
                },
                'timestamp': {
                    'tv_sec': info.timestamp.tv_sec,
                    'tv_nsec': info.timestamp.tv_nsec,
                },
                'data_pool_id': info.data_pool_id,
                'watchers': [w for w in info.watchers],
                'qos': {
                    'iops': info.qos.iops,
                    'bps': info.qos.bps,
                },
                'du': info.du,
            }

        def update(image, info):
            image['id'] = info.id
            image['name'] = info.name if info.name else image.get('name')
            image['order'] = info.order
            image['size'] = info.size
            image['stripe_unit'] = info.stripe_unit
            image['stripe_count'] = info.stripe_count
            image['features'] = info.features
            image['flags'] = info.flags
            image['snapc'] = {
                'seq': info.snapc.seq,
                'snaps': [s for s in info.snapc.snaps],
            }
            image['parent'] = {
                'spec': {
                    'pool_id': info.parent.spec.pool_id,
                    'image_id': info.parent.spec.image_id,
                    'snap_id': info.parent.spec.snap_id,
                },
                'overlap': info.parent.overlap,
            }
            image['timestamp'] = {
                'tv_sec': info.timestamp.tv_sec,
                'tv_nsec': info.timestamp.tv_nsec,
            }
            image['data_pool_id'] = info.data_pool_id
            image['watchers'] = [w for w in info.watchers]
            image['qos'] = {
                'iops': info.qos.iops,
                'bps': info.qos.bps,
            }
            image['du'] = info.du

        with self.lock:
            if self.data.get(pool) is None:
                self.data[pool] = {}

            images = self.data[pool]
            for k, v in l.items():
                info, r = v
                if r < 0:
                    continue

                if images.get(k) is None:
                    images[k] = pythonify(info)
                else:
                    update(images[k], info)

                images[k]['info_v2_update'] = self.ticks
                images[k]['du_update'] = self.ticks

            self.data[pool] = images

    @time_logging
    def _list_child(self, pool):
        ioctx = radosx.IoCtx()
        r = self.radosx.ioctx_create2(pool, ioctx)
        if r < 0:
            return None, r

        # std::map<parent spec, std::vector<image id>>
        l = rbdx.Map_parent_spec_t_2_vector_string()
        r = self.rbdx.child_list(ioctx, l)
        return l, r

    @time_logging
    def _update_child(self, pool, l):
        with self.lock:
            for k, v in l.items():
                images = self.data.get(k.pool_id)
                if images is None:
                    continue
                image = images.get(k.image_id)
                if image is None:
                    continue
                if image.get('snaps') is None:
                    continue
                snaps = image.get('snaps')
                if snaps is None:
                    continue
                snap = snaps.get(k.snap_id)
                if snap is None:
                    continue

                children = snap.get('children', {})
                children[pool] = [c for c in v]
                snap['children'] = children

    def _worker_run(self, i, taskq):
        while True:
            self.log.debug('worker({0}) waiting..'.format(i))

            task = taskq.get()

            pool = task.pool
            action = task.action
            task_private = task.private

            self.log.debug('worker({0}) working on task({1}/{2}/{3})..'.format(
                i, pool, action, task_private))

            if action == ACTION_LIST_NAME:
                l, r = self._list_name(pool)
                if r < 0:
                    task.r = r
                else:
                    self._update_name(pool, l)
            elif action == ACTION_LIST_CHILD:
                l, r = self._list_child(pool)
                if r < 0:
                    task.r = r
                else:
                    self._update_child(pool, l)
            elif action == ACTION_LIST_INFO:
                v = task_private.get('v', 1)
                batch = task_private.get('batch')
                if v == 1:
                    l, r = self._list_info(pool, batch=batch)
                elif v == 2:
                    l, r = self._list_info_v2(pool, batch=batch)
                else:
                    l, r = self._list_info_v3(pool, batch=batch)
                if r < 0:
                    task.r = r
                else:
                    if v == 1:
                        self._update_info(pool, l)
                    elif v == 2:
                        self._update_info_v2(pool, l)
                    else:
                        self._update_info_v3(pool, l)

            task.done = True
            if task.event is not None:
                task.event.set()

            taskq.task_done()

            self.log.debug('worker({0}) task({1}/{2}) done'.format(i, pool, action))

    @time_logging
    def _prune(self, pools):
        with self.lock:
            if not self.ticks % PRUNE_TICKS == 0:
                return

            self.log.debug('pruning..')

            data = {}
            for p in pools:
                data[p] = self.data.get(p)
                if data[p] is None:
                    continue
                for _, image in six.iteritems(data[p]):
                    snaps = image.get('snaps', {})
                    for _, snap in six.iteritems(snaps):
                        old = snap.get('children')
                        if old is None:
                            continue
                        children = {}
                        for p in pools:
                            if old.get(p) is not None:
                                children[p] = old.get(p)
                        snap['children'] = children

            self.data = data

    def _tick(self, pools):
        with self.lock:
            self.ticks += 1

        self.scheduler.tick(pools)
        self._prune(pools)

    def run(self):
        try:
            self.log.info('starting..')

            for i in range(WORKER_NUM):
                self.log.info('worker({0}) spawning..'.format(i))
                worker = threading.Thread(target=self._worker_run, args=(i, self.taskq))
                worker.setDaemon(True)
                worker.start()
                self.log.info('worker({0}) spawned..'.format(i))

            self.log.info('started')

            while not self.stop.wait(TICK_INTERVAL):
                self.log.debug('ticking..')

                pools = self._get_pools()
                self._tick(pools)

            self.log.info('stopping..')
            self.taskq.join()
            self.log.info('stopped')

        except Exception as ex:
            self.log.fatal('Fatal runtime error: {}\n{}'.format(
                ex, traceback.format_exc()))

    def snapshot_images(self, pool, filters=None):

        def filtered(image, filters):
            for filter in filters:
                if not filter(image):
                    return False
            return True

        with self.lock:
            images = self.data.get(pool)
            if images is None:
                return None

            filtered_images = {}
            if filters is not None:
                for k, v in six.iteritems(images):
                    if filtered(v, filters):
                        filtered_images[k] = v
            else:
                filtered_images = images

            return filtered_images

    @time_logging
    def list_name(self, pool):
        with self.lock:
            images = self.data.get(pool)
            if images is None:
                return {}

            data = {}
            for image_id, image in six.iteritems(images):
                data[image_id] = image['name']
            return data

    @time_logging
    def list_info(self, pool, v):

        def extract_snap(snap):
            return {
                'id': snap['id'],
                'name': snap['name'],
                'snap_ns_type': snap['snap_ns_type'],
                'size': snap['size'],
                'features': snap['features'],
                'flags': snap['flags'],
                'protection_status': snap['protection_status'],
                'timestamp': {
                    'tv_sec': snap['timestamp']['tv_sec'],
                    'tv_nsec': snap['timestamp']['tv_nsec'],
                },
                'du': snap.get('du'),
                'children': snap.get('children'),
            }
        
        def extract_info(image, v):
            info = {
                'id': image.get('id'),
                'name': image.get('name'),
                'order': image.get('order'),
                'size': image.get('size'),
                'stripe_unit': image.get('stripe_unit'),
                'stripe_count': image.get('stripe_count'),
                'features': image.get('features'),
                'flags': image.get('flags'),
                'snapc': {
                    'seq': image['snapc']['seq'],
                    'snaps': [s for s in image['snapc']['snaps']],
                } if image.get('snapc') is not None else None,
                'parent': {
                    'spec': {
                        'pool_id': image['parent']['spec']['pool_id'],
                        'image_id': image['parent']['spec']['image_id'],
                        'snap_id': image['parent']['spec']['snap_id'],
                    },
                    'overlap': image['parent']['overlap'],
                } if image.get('parent') is not None else None,
                'timestamp': {
                    'tv_sec': image['timestamp']['tv_sec'],
                    'tv_nsec': image['timestamp']['tv_nsec'],
                } if image.get('timestamp') is not None else None,
                'data_pool_id': image.get('data_pool_id'),
                'watchers': [
                    w for w in image['watchers']
                ] if image.get('watchers') is not None else None,
                'qos': {
                    'iops': image['qos']['iops'],
                    'bps': image['qos']['bps'],
                } if image.get('qos') is not None else None,
                'du': image.get('du'),
            }

            if v == 2:
                info['snaps'] = {
                    k: extract_snap(v) for k, v in six.iteritems(image['snaps'])
                } if image.get('snaps') is not None else None
            return info

        with self.lock:
            images = self.data.get(pool)
            if images is None:
                return {}

            data = {}
            for image_id, image in six.iteritems(images):
                data[image_id] = extract_info(image, v)
            return data

    @time_logging
    def list_du(self, pool):

        def extract_du(image):
            du = {
                'du': image.get('du'),
            }

            return du

        images = self.data.get(pool)
        if images is None:
            return {}

        data = {}
        for image_id, image in six.iteritems(images):
            data[image_id] = extract_du(image)
        return data

    def handle_command(self, cmd):
        prefix = cmd['prefix']

        try:
            if prefix == 'rbdx list-name':
                pool_id = cmd['pool_id']
                refresh = True if 'refresh' in cmd else False

                pools = self._get_pools()

                if pool_id not in pools:
                    return 0, json.dumps({}), ''

                if refresh:
                    action = ACTION_LIST_NAME
                    task = self.scheduler.schedule(pool_id, action)
                    if task is not None:
                        task.wait()
                        if task.r < 0:
                            self.log.warn('{0}/{1} failed: {2}'.format(
                                pool_id, action, r)
                            )

                names = self.list_name(pool_id)
                return 0, json.dumps(names), ''

            if prefix == 'rbdx list-info' or prefix == 'rbdx list-du':
                pool_id = cmd['pool_id']
                refresh = True if 'refresh' in cmd else False
                v = 2 if 'v2' in cmd else 1
                v = 3 if 'v3' in cmd else v

                if prefix == 'rbdx list-du':
                    v = 3

                pools = self._get_pools()

                if pool_id not in pools:
                    return 0, json.dumps({}), ''

                if refresh:
                    action = ACTION_LIST_INFO
                    task = self.scheduler.schedule(pool_id, action, v=v)
                    if task is not None:
                        task.wait()
                        if task.r < 0:
                            self.log.warn('{0}/{1} failed: {2}'.format(
                                pool_id, action, r)
                            )

                if prefix == 'rbdx list-info':
                    infos = self.list_info(pool_id, v)
                else:
                    infos = self.list_du(pool_id)
                return 0, json.dumps(infos), ''

        except Exception as ex:
            # log the full traceback but don't send it to the CLI user
            self.log.fatal('Fatal runtime error: {}\n{}'.format(
                ex, traceback.format_exc()))
            raise
        
        raise NotImplementedError(prefix)
