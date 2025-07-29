Ratchet
=======

Ratchet is a heavy-handed way to slow down a process and its immediate
children (except the a Postgres archiver; this oddity could be removed
to make it a general tool) by issuing repeated ``SIGSTOP`` and
``SIGCONT`` signals.  Ratchet is *not* intended to be compatible with
processes that use ``SIGSTOP``/``SIGCONT`` for other reasons: it does
not attempt to restore the state of processes that were ``SIGSTOP``-ed
to begin with.

Ratchet uses some utilities that are platform specific.  It's only
known to work on GNU systems, relying on GNU ``ps``.  Notably, it's
incompatible with the BSD-family ``ps``, which is seen on Macintosh OS
X/Darwin also.

Ratchet tries to be careful and enable reliably ensuring all processes
end up being ``SIGCONT``-ed and do not remain stuck.  Outside
exceptional conditions like an OOM-kill or Python bug, it is intended
to be correct by design.  In these exceptional situations, there is an
idempotent command, ``wake``, that can be used to recover, and if
confirmed to run successfully is designed to ensure correct
un-suspension.

Ratchet requires Python 3.

Usage
=====

The output of ``./ratchet -h``::

    The output of ``./ratchet -h``::

    usage: ratchet [-h] {wake,renice,nap,self-test} ...

    Continuously send SIGSTOP and SIGCONT to throttle process activity

    optional arguments:
      -h, --help            show this help message and exit

    actions:
      {wake,renice,nap,self-test}
        wake                wake up a process and its children
        renice              change niceness values for postmaster children
        nap                 make a process and its children pause frequently
        self-test           Do a quick self-test

Testing
=======

A simple embedded self-diagnostic is available; it just prints out
some information and must be scanned by a human.  Nevertheless, it has
been useful for development iteration on this small tool::

    $ ./ratchet self-test
    Beginning self test...
      Testing un-limited speed of a process; for comparison
    Unthrottled process: counted 3371227 times
      Testing throttled process; it should loop fewer times
    Throttled process: counted 908915 times
      Testing unconditional wake; should terminate
    Paused process: counted 3385802 times
      Woken.
      Test port-pid resolution
      ...resolved properly
      Test renicing
    Reniced process: counted 3386670 times
      Summary: succeeded=2 failed=0
    Done.
