.. _mds-reactor:

=========================
MDS Reactor Dispatch Mode
=========================

By default an MDS rank uses **classic** dispatch: inbound messages and many
completion callbacks run under ``mds_lock`` with the historical finisher /
multi-thread handoff behavior. The optional **reactor** dispatch engine
replaces that with a single op thread that drains a prioritized multi-lane
work queue. Classic remains the default; reactor mode is selected only when
explicitly configured.

.. note::
   Reactor dispatch is intended for evaluation and development. Treat it as
   experimental until it has been validated for your workload. Prefer classic
   for production unless you are deliberately testing reactor mode.

How it works
============

When ``mds_dispatch_engine=reactor``, producers (messenger dispatch,
Objecter / journaler completions, trim ticks, and some control callbacks)
wrap work as ``OpWorkItem`` instances and enqueue them onto
``MDSOpWorkQueue``. One op thread drains the queue and runs
``execute_item``.

After boot completes, that op thread is **rank-exclusive**: it does not take
``mds_lock`` for ordinary execute. Paths that still need the mutex (SafeTimer,
lifecycle, classic mode) continue to use it. During boot / replay, the reactor
drains only Control and IOComplete, still under a real ``mds_lock`` guard, so
those items serialize with journal replay threads that hold the lock. Client
and Maintenance work stay queued until boot exclusivity is cleared.

Dispatch lanes
==============

Every work item is assigned a ``DispatchLane``. Lanes are ordered high to
low priority:

===========  ===============================================================
Lane         Typical contents
===========  ===============================================================
Control      Maps, commands, session/reconnect/reclaim, resolve/rejoin,
             heartbeat, table requests, finished-queue advancement, and
             other rank control-plane callbacks
IOComplete   Objecter and MDLog journaler ``MDSIOContextBase`` completions
Maintenance  Cache trim quanta (``TrimQuantum``), journal trim ticks
             (``LogTrim``), scrub / cache-expire messages
Client       Client requests/caps/leases/metrics and steady-state peer
             metadata traffic (discover, locks, dirfrag export, …)
===========  ===============================================================

Each scheduling round visits lanes in that order. For each lane the op thread
runs items until the lane is empty or the lane's **wall-clock slice** expires,
then advances to the next lane.

Synthetic (non-message) work is hard-wired to a lane when created:

- ``AdvanceQueues`` → Control
- ``IOCompletion`` → IOComplete
- ``TrimQuantum`` / ``LogTrim`` → Maintenance
- ``Callable`` → caller-selected lane

Inbound messages are classified at enqueue time by
``classify_inbound_message()`` in ``src/mds/dispatch/classify.cc`` (see
`Assigning new messages`_ below).

Scheduling and adaptation
=========================

Base slice budgets come from ``mds_reactor_lane_slice_*``. A value of ``0``
means “drain until empty” for that lane visit (no wall deadline).

Among **IOComplete**, **Maintenance**, and **Client**, the reactor estimates
per-lane backlog as ``depth × rolling average execute time``. When the highest
of those backlogs reaches ``mds_reactor_slice_backlog_target``, the scheduler
boosts the dominant lane toward its ``*_max`` slice and cuts the other
adaptable lanes toward their ``*_min`` so none are starved entirely. Control
keeps its base slice and is not adapted. Full adaptation is reached at four
times the backlog target. Set ``mds_reactor_slice_backlog_target`` to ``0`` to
disable adaptation.

``TrimQuantum`` and ``LogTrim`` are single-flight and cooperatively
time-sliced: each item stops after ``mds_cache_trim_max_duration`` or
``mds_log_trim_max_duration`` and re-enqueues another quantum if more work
remains.

Enqueue backpressure
====================

Unbounded Client or IOComplete floods can grow the reactor queue until memory
or ``mds_reactor_queue_len_abort`` trips. Hysteresis watermarks stall
producers instead:

- Client lane: ``mds_reactor_client_enqueue_{high,low}``
- IOComplete lane: ``mds_reactor_io_enqueue_{high,low}``

When depth reaches the high watermark, enqueuers block until the op thread
drains to the low watermark. While any waiter is draining, new enqueuers for
that lane join the wait so they cannot refill the mid band. **Control is never
stalled.** Client throttle is skipped while boot exclusivity holds Client
undrained. The op thread never blocks on these waits. Set a high watermark to
``0`` to disable that throttle.

IO completions that arise on the op thread during ``execute_item`` (for
example nested journaler finish paths) are deferred and drained after the
current item returns — they are not executed inline under re-entered locks and
are not re-enqueued in a way that bypasses throttle / abort accounting.

Enabling reactor mode
=====================

``mds_dispatch_engine`` is a **startup** option. Set it before the MDS starts
(or restart after changing it):

.. prompt:: bash #

   ceph config set mds mds_dispatch_engine reactor

To return to classic:

.. prompt:: bash #

   ceph config set mds mds_dispatch_engine classic

Most lane-slice and enqueue options are runtime-changeable; see
:ref:`MDS Config Reference`.

Configuration reference
=======================

Engine selection
----------------

:confval:`mds_dispatch_engine`
  ``classic`` (default) or ``reactor``. Startup only.

Queue safety and backpressure
-----------------------------

:confval:`mds_reactor_queue_len_abort`
  If non-zero, dump recent logs and abort when total reactor queue depth
  reaches this value. Keep it above Client + IOComplete high watermarks (plus
  headroom for Control / Maintenance) when those are set. ``0`` disables.

:confval:`mds_reactor_client_enqueue_high` /
:confval:`mds_reactor_client_enqueue_low`
  Client-lane enqueue hysteresis. ``0`` high disables Client stall.

:confval:`mds_reactor_io_enqueue_high` /
:confval:`mds_reactor_io_enqueue_low`
  IOComplete-lane enqueue hysteresis. ``0`` high disables IOComplete stall.

Lane slices (base budgets)
--------------------------

:confval:`mds_reactor_lane_slice_control`
:confval:`mds_reactor_lane_slice_io`
:confval:`mds_reactor_lane_slice_maintenance`
:confval:`mds_reactor_lane_slice_client`

Wall-clock milliseconds per lane visit in a scheduling round. ``0`` drains
that lane until empty for the visit.

Backlog-adaptive slices
-----------------------

:confval:`mds_reactor_slice_exec_window`
  Rolling window size for per-lane average execute time (clamped to
  ``[1, 1024]``).

:confval:`mds_reactor_slice_backlog_target`
  Soft backlog (milliseconds of estimated work) that starts adaptation.
  ``0`` disables.

:confval:`mds_reactor_lane_slice_client_max` /
:confval:`mds_reactor_lane_slice_client_min`
:confval:`mds_reactor_lane_slice_io_max` /
:confval:`mds_reactor_lane_slice_io_min`
:confval:`mds_reactor_lane_slice_maintenance_max` /
:confval:`mds_reactor_lane_slice_maintenance_min`

Bounds used when adapting the corresponding lane away from its base slice.

Trim quanta (reactor)
---------------------

:confval:`mds_cache_trim_max_duration`
  Max wall time for one Maintenance ``TrimQuantum`` item. Classic mode
  ignores this.

:confval:`mds_log_trim_max_duration`
  Max wall time for one Maintenance ``LogTrim`` item. Classic mode ignores
  this.

Assigning new messages
======================

Every message type that can reach MDS rank dispatch in reactor mode **must**
have an explicit case in ``classify_inbound_message()``
(``src/mds/dispatch/classify.cc``). There is intentionally **no** ``default``
arm: unclassified types log and ``ceph_abort()``.

When you add a new MDS-bound message type:

1. **Pick a lane** using the table above:

   - Rank / cluster control, maps, session lifecycle, resolve/rejoin →
     ``DispatchLane::Control``
   - Background scrub / cache-expire style work →
     ``DispatchLane::Maintenance``
   - Steady-state client or peer metadata path →
     ``DispatchLane::Client``
   - Do **not** assign inbound messages to ``IOComplete``; that lane is for
     ``MDSIOContextBase`` completions created via ``submit_io_completion``.

2. **Add a ``case``** in the ``switch (m.get_type())`` inside
   ``classify_inbound_message()`` that returns the chosen lane. Group it with
   the existing cases for that lane.

3. **Optional execute-latency work class.** Control and Maintenance inbound
   types (and synthetic work kinds) can be broken out in
   ``DispatchWorkClass`` / ``classify_inbound_work_class()`` and
   ``dispatch_work_class_info[]``. Client-lane inbound types are intentionally
   omitted from that per-type breakdown (lane averages already cover Client).
   If you add a new Control/Maintenance type that should appear in
   ``dispatch_execute_latency_*`` counters:

   - Extend ``enum class DispatchWorkClass`` in ``classify.h`` (keep order in
     sync with the counter block in ``MDSRank.h``).
   - Add a matching ``work_class_info[]`` entry in ``classify.cc``.
   - Map the message type in ``classify_inbound_work_class()``.

4. **Build and run tests.** ``src/test/mds/TestDispatchClassify.cc`` (and
   related dispatch unit tests) should cover the new classification. A missing
   case will abort the MDS as soon as that message is enqueued in reactor mode.

Related sources: ``OpWorkItem.h`` (lane / work-kind enums),
``ReactorDispatchEngine.{h,cc}`` (scheduler), ``MDSDispatchEngine`` (classic vs
reactor selection).
