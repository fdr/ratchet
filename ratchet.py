import argparse
import datetime
import os
import signal
import subprocess
import sys
import time

def pid_children(root_pid):
    """
    Given a pid, return a list of the child pids and their argv

    This is not recursive.  The argv is returned so that the Postgres
    Archiver process can be spared from throttling.
    
    Raises an exception if the pid could not be found.
    """
    p = subprocess.Popen(['ps' ,'--ppid=' + str(root_pid), '-o', 'pid=,args='],
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    stdout, stderr = p.communicate()

    if p.wait() != 0:
        if stderr == '':
            # ps likes to return 1 when no processes are returned.  It
            # makes no exit-code differentiation between 'erroneous
            # user input', 'no pid', or 'no children of pid', so don't
            # even bother trying.
            return []
        else:
            raise Exception('process listing did not complete successfully: ' +
                            stderr)

    parts = [line.split(' ', 1) for line in stdout.strip().split('\n')]
    return [(int(pid), args) for (pid, args) in parts]

def is_archiver(argv):
    return argv.find('archiver') >= 0

def force_naptime(parent_pid, nap_quantum):
    """
    Put a parent and its child processes to sleep for a while

    This function is intended to be called repeatedly to cause a
    degradation in performance, as each naptime is relatively small.

    """
    SIGCONT = signal.SIGCONT
    SIGSTOP = signal.SIGSTOP
    parent_pid = int(parent_pid)

    # Record stopped processes before actually sending the signal;
    # since spurious SIGCONTs are mostly harmless but dangling
    # SIGSTOPs are dangerous, optimize for sending SIGCONT even if
    # there is no preceding SIGSTOP in error cases.
    children_maybe_stopped = set()

    def stop_pid_and_record(pid):
        children_maybe_stopped.add(pid)
        os.kill(pid, SIGCONT)        

    try:
        # Stop the parent first, so it cannot create children.
        os.kill(parent_pid, SIGSTOP)
        
        children = pid_children(parent_pid)

        # Send SIGSTOP to children, except the archiver.
        for pid, argv in children:
            if not is_archiver(argv):
                stop_pid_and_record(pid)

        # Wait around a while while everyone is taking a nap
        time.sleep(nap_quantum)
    finally:
        # First wake up the children, and the parent last, again
        # because the parent (postmaster) may spawn more processes.
        for pid in children_maybe_stopped:
            os.kill(pid, SIGCONT)

        os.kill(parent_pid, SIGCONT)

def force_wake(parent_pid):
    """
    A paranoid, unconditional wake-up call

    Should be called after throttling is complete, in case this
    program was termianted by an OOM killer or even just sketchy OOM
    handling by Python.
    """
    for pid in [parent_pid] + pid_children(parent_pid):
        os.kill(parent_pid, SIGCONT)

def nap_until(parent_pid, deadline, nap_quantum):
    """
    Cause continuous napping until some future datetime.

    """
    while datetime.datetime.now() < deadline:
        force_naptime(parent_pid, nap_quantum)

def self_test():
    """
    Quick and dirty self-test

    Gin up a process to throttle, throttle and wake it.
    """

    print 'Beginning self test...'

    print '  Testing un-limited speed of a process'
    def fork_burner(title):
        deadline = datetime.datetime.now() + datetime.timedelta(seconds=5)
        pid = os.fork()
        if pid == 0:
            # is the victim/child
            i = 0
            while datetime.datetime.now() < deadline:
                i += 1

            print '{0}: counted {1} times'.format(title, i)
            sys.exit(0)
        else:
            return int(pid)
        
    pid = fork_burner('Unthrottled process')
    os.waitpid(pid, 0)

    print '  Testing throttled process, it should loop fewer times'

    # Deadline comes somewhat safely before the process finishes
    napper_deadline = datetime.datetime.now() + datetime.timedelta(seconds=4)

    pid = fork_burner('Throttled process')
    nap_until(pid, napper_deadline, 0.1)
    os.waitpid(pid, 0)

def main():
    parser = argparse.ArgumentParser(
        description=('Continuously send SIGSTOP and SIGCONT to '
                     'throttle process activity'))

    subparsers = parser.add_subparsers(title='actions', dest='action')

    # Parent parser for positional PID argument that is seen in
    # multiple commands.
    pid_arg_parent = argparse.ArgumentParser(add_help=False)
    pid_arg_parent.add_argument('pid', type=int,
                                help='parent process id to apply to')

    # Wake action
    wake_action = subparsers.add_parser('wake', parents=[pid_arg_parent])

    # Nap action
    nap_action = subparsers.add_parser('nap', parents=[pid_arg_parent])
    nap_action.add_argument('duration', type=int,
                            help='number of minutes to incur naps for')
    nap_action.add_argument('--quantum', type=float, default=0.3,
                            help=('number of seconds to pause at a time; '
                                  'floating point supported'))

    # Self-test action
    nap_action = subparsers.add_parser('self-test', help='Do a quick self-test')

    args = parser.parse_args()

    if args.action == 'wake':
        force_wake(args.pid)
    elif args.action == 'nap':
        deadline = (datetime.datetime.now() +
                    datetime.timedelta(minutes=1 * args.duration))
        nap_until(args.pid, deadline, args.quantum)
    elif args.action == 'self-test':
        self_test()
    else:
        raise AssertionError(
            "parser.parse_args should have rejected this input already")

if __name__ == '__main__':
    main()
