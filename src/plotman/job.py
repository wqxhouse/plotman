# TODO do we use all these?
import argparse
import contextlib
import functools
import logging
import os
import random
import re
import sys
import time
from datetime import datetime
from enum import Enum, auto
from subprocess import call
import glob

import attr
import click
import pendulum
import psutil

from plotman import chia


def job_phases_for_tmpdir(d, all_jobs):
    '''Return phase 2-tuples for jobs running on tmpdir d'''
    return sorted([j.progress() for j in all_jobs if j.tmpdir == d])


def job_phases_for_dstdir(d, all_jobs):
    '''Return phase 2-tuples for jobs outputting to dstdir d'''
    return sorted([j.progress() for j in all_jobs if j.dstdir == d])


def is_plotting_cmdline(cmdline, madmax_proc_name):
    if cmdline and 'python' in cmdline[0].lower():
        cmdline = cmdline[1:]

    if madmax_proc_name:
        return (
            len(cmdline) >= 1
            and cmdline[0].endswith(madmax_proc_name))
    else:
        return (
            len(cmdline) >= 3
            and cmdline[0].endswith("chia")
            and 'plots' == cmdline[1]
            and 'create' == cmdline[2]
        )


def parse_chia_plot_time(s):
    # This will grow to try ISO8601 as well for when Chia logs that way
    return pendulum.from_format(s, 'ddd MMM DD HH:mm:ss YYYY', locale='en', tz=None)


def parse_chia_plot_time_madmax(s):
    # This will grow to try ISO8601 as well for when Chia logs that way
    return pendulum.from_format(s, 'YYYY-MM-DD-HH-mm', locale='en', tz=None)


def parse_chia_plots_create_command_line(command_line):
    command_line = list(command_line)
    if len(command_line) == 0:
        return ParsedChiaPlotsCreateCommand(
            error="Empty command line",
            help={},
            parameters={}
        )

    # Parse command line args
    if 'python' in command_line[0].lower():
        command_line = command_line[1:]
    assert len(command_line) >= 3
    assert 'chia' in command_line[0]

    is_madmax = False
    if 'plots' == command_line[1]:
        assert 'create' == command_line[2]
    else:
        is_madmax = True

    all_command_arguments = command_line[1 if is_madmax else 3:]

    # nice idea, but this doesn't include -h
    # help_option_names = command.get_help_option_names(ctx=context)
    help_option_names = {'--help', '-h'}

    command_arguments = [
        argument
        for argument in all_command_arguments
        if argument not in help_option_names
    ]

    # TODO: We could at some point do chia version detection and pick the
    #       associated command.  For now we'll just use the latest one we have
    #       copied.
    command = chia.commands.latest_command()
    try:
        context = command.make_context(
            info_name='', args=list(command_arguments))
    except click.ClickException as e:
        error = e
        params = {}
    else:
        error = None
        params = context.params

    return ParsedChiaPlotsCreateCommand(
        error=error,
        help=len(all_command_arguments) > len(command_arguments),
        parameters=params,
    )


class ParsedChiaPlotsCreateCommand:
    def __init__(self, error, help, parameters):
        self.error = error
        self.help = help
        self.parameters = parameters


@functools.total_ordering
@attr.frozen(order=False)
class Phase:
    major: int = 0
    minor: int = 0
    known: bool = True

    def __lt__(self, other):
        return (
            (not self.known, self.major, self.minor)
            < (not other.known, other.major, other.minor)
        )

    @classmethod
    def from_tuple(cls, t):
        if len(t) != 2:
            raise Exception(f'phase must be created from 2-tuple: {t!r}')

        if None in t and not t[0] is t[1]:
            raise Exception(f'phase can not be partially known: {t!r}')

        if t[0] is None:
            return cls(known=False)

        return cls(major=t[0], minor=t[1])

    @classmethod
    def list_from_tuples(cls, l):
        return [cls.from_tuple(t) for t in l]

# TODO: be more principled and explicit about what we cache vs. what we look up
# dynamically from the logfile


class Job:
    'Represents a plotter job'

    logfile = ''
    jobfile = ''
    job_id = 0
    plot_id = '--------'
    proc = None   # will get a psutil.Process

    def get_running_jobs(logroot, madmax_proc_name=None, cached_jobs=()):
        '''Return a list of running plot jobs.  If a cache of preexisting jobs is provided,
           reuse those previous jobs without updating their information.  Always look for
           new jobs not already in the cache.'''
        jobs = []
        cached_jobs_by_pid = {j.proc.pid: j for j in cached_jobs}

        with contextlib.ExitStack() as exit_stack:
            processes = []

            pids = set()
            ppids = set()

            for process in psutil.process_iter():
                # Ignore processes which most likely have terminated between the time of
                # iteration and data access.
                with contextlib.suppress(psutil.NoSuchProcess, psutil.AccessDenied):
                    exit_stack.enter_context(process.oneshot())
                    if is_plotting_cmdline(process.cmdline(), madmax_proc_name):
                        ppids.add(process.ppid())
                        pids.add(process.pid)
                        processes.append(process)

            # https://github.com/ericaltendorf/plotman/pull/418
            # The experimental Chia GUI .deb installer launches plots
            # in a manner that results in a parent and child process
            # that both share the same command line and, as such, are
            # both identified as plot processes.  Only the child is
            # really plotting.  Filter out the parent.

            wanted_pids = pids - ppids

            wanted_processes = [
                process
                for process in processes
                if process.pid in wanted_pids
            ]

            for proc in wanted_processes:
                with contextlib.suppress(psutil.NoSuchProcess, psutil.AccessDenied):
                    if proc.pid in cached_jobs_by_pid.keys():
                        # Copy from cache
                        jobs.append(cached_jobs_by_pid[proc.pid])
                    else:
                        with proc.oneshot():
                            parsed_command = parse_chia_plots_create_command_line(
                                command_line=proc.cmdline(),
                            )
                            if parsed_command.error is not None:
                                continue
                            job = Job(
                                proc=proc,
                                parsed_command=parsed_command,
                                logroot=logroot,
                            )
                            if job.help:
                                continue
                            jobs.append(job)

        return jobs

    def __init__(self, proc, parsed_command, logroot):
        '''Initialize from an existing psutil.Process object.  must know logroot in order to understand open files'''
        self.proc = proc
        # These are dynamic, cached, and need to be udpated periodically
        self.phase = Phase(known=False)

        self.help = parsed_command.help
        self.args = parsed_command.parameters
        self.madmax_log = False

        # an example as of 1.0.5
        # {
        #     'size': 32,
        #     'num_threads': 4,
        #     'buckets': 128,
        #     'buffer': 6000,
        #     'tmp_dir': '/farm/yards/901',
        #     'final_dir': '/farm/wagons/801',
        #     'override_k': False,
        #     'num': 1,
        #     'alt_fingerprint': None,
        #     'pool_contract_address': None,
        #     'farmer_public_key': None,
        #     'pool_public_key': None,
        #     'tmp2_dir': None,
        #     'plotid': None,
        #     'memo': None,
        #     'nobitfield': False,
        #     'exclude_final_dir': False,
        # }

        self.k = self.args['size']
        self.r = self.args['num_threads']
        self.u = self.args['buckets']
        self.b = self.args['buffer']
        self.n = self.args['num']
        self.tmpdir = self.args['tmp_dir']
        self.tmp2dir = self.args['tmp2_dir']
        self.dstdir = self.args['final_dir']

        plot_cwd = self.proc.cwd()
        self.tmpdir = os.path.join(plot_cwd, self.tmpdir)
        if self.tmp2dir is not None:
            self.tmp2dir = os.path.join(plot_cwd, self.tmp2dir)
        self.dstdir = os.path.join(plot_cwd, self.dstdir)

        # Find logfile (whatever file is open under the log root).  The
        # file may be open more than once, e.g. for STDOUT and STDERR.
        for f in self.proc.open_files():
            if logroot in f.path:
                if self.logfile:
                    assert self.logfile == f.path
                else:
                    self.logfile = f.path
                break

        if self.logfile:
            # Initialize data that needs to be loaded from the logfile
            self.init_from_logfile()
# TODO: turn this into logging or somesuch
#         else:
#             print('Found plotting process PID {pid}, but could not find '
#                     'logfile in its open files:'.format(pid = self.proc.pid))
#             for f in self.proc.open_files():
#                 print(f.path)

    def init_from_logfile(self):
        '''Read plot ID and job start time from logfile.  Return true if we
           find all the info as expected, false otherwise'''
        assert self.logfile
        if 'madmax' in self.logfile:
            self.madmax_log = True

        if not self.madmax_log:
            # Try reading for a while; it can take a while for the job to get started as it scans
            # existing plot dirs (especially if they are NFS).
            found_id = False
            found_log = False
            for attempt_number in range(3):
                with open(self.logfile, 'r') as f:
                    for line in f:
                        m = re.match('^ID: ([0-9a-f]*)', line)
                        if m:
                            self.plot_id = m.group(1)
                            found_id = True
                        m = re.match(
                            r'^Starting phase 1/4:.*\.\.\. (.*)', line)
                        if m:
                            # Mon Nov  2 08:39:53 2020
                            self.start_time = parse_chia_plot_time(m.group(1))
                            found_log = True
                            break  # Stop reading lines in file

                if found_id and found_log:
                    break  # Stop trying
                else:
                    time.sleep(1)  # Sleep and try again
        else:
            # Try reading for a while; it can take a while for the job to get started as it scans
            # existing plot dirs (especially if they are NFS).
            found_id = False
            found_log = False
            for attempt_number in range(3):
                with open(self.logfile, 'r') as f:
                    for line in f:
                        m = re.match(
                            r'^Plot Name: plot-k32-(.*)-([0-9a-f]*)', line)
                        if m:
                            self.start_time = parse_chia_plot_time_madmax(
                                m.group(1))
                            self.plot_id = m.group(2)
                            found_id = True
                            found_log = True
                            break  # Stop reading lines in file

                if found_id and found_log:
                    break  # Stop trying
                else:
                    time.sleep(1)  # Sleep and try again

        # If we couldn't find the line in the logfile, the job is probably just getting started
        # (and being slow about it).  In this case, use the last metadata change as the start time.
        # TODO: we never come back to this; e.g. plot_id may remain uninitialized.
        # TODO: should we just use the process start time instead?
        if not found_log:
            self.start_time = datetime.fromtimestamp(
                os.path.getctime(self.logfile))

        # Load things from logfile that are dynamic
        self.update_from_logfile()

    def update_from_logfile(self):
        if not self.madmax_log:
            self.set_phase_from_logfile()
        else:
            self.set_phase_from_logfile_madmax()

    def set_phase_from_logfile(self):
        assert self.logfile

        # Map from phase number to subphase number reached in that phase.
        # Phase 1 subphases are <started>, table1, table2, ...
        # Phase 2 subphases are <started>, table7, table6, ...
        # Phase 3 subphases are <started>, tables1&2, tables2&3, ...
        # Phase 4 subphases are <started>
        phase_subphases = {}

        with open(self.logfile, 'r') as f:
            for line in f:
                # "Starting phase 1/4: Forward Propagation into tmp files... Sat Oct 31 11:27:04 2020"
                m = re.match(r'^Starting phase (\d).*', line)
                if m:
                    phase = int(m.group(1))
                    phase_subphases[phase] = 0

                # Phase 1: "Computing table 2"
                m = re.match(r'^Computing table (\d).*', line)
                if m:
                    phase_subphases[1] = max(
                        phase_subphases[1], int(m.group(1)))

                # Phase 2: "Backpropagating on table 2"
                m = re.match(r'^Backpropagating on table (\d).*', line)
                if m:
                    phase_subphases[2] = max(
                        phase_subphases[2], 7 - int(m.group(1)))

                # Phase 3: "Compressing tables 4 and 5"
                m = re.match(r'^Compressing tables (\d) and (\d).*', line)
                if m:
                    phase_subphases[3] = max(
                        phase_subphases[3], int(m.group(1)))

                # TODO also collect timing info:

                # "Time for phase 1 = 22796.7 seconds. CPU (98%) Tue Sep 29 17:57:19 2020"
                # for phase in ['1', '2', '3', '4']:
                    # m = re.match(r'^Time for phase ' + phase + ' = (\d+.\d+) seconds..*', line)
                    # data.setdefault....

                # Total time = 49487.1 seconds. CPU (97.26%) Wed Sep 30 01:22:10 2020
                # m = re.match(r'^Total time = (\d+.\d+) seconds.*', line)
                # if m:
                    # data.setdefault(key, {}).setdefault('total time', []).append(float(m.group(1)))

        if phase_subphases:
            phase = max(phase_subphases.keys())
            self.phase = Phase(major=phase, minor=phase_subphases[phase])
        else:
            self.phase = Phase(major=0, minor=0)

    def set_phase_from_logfile_madmax(self):
        assert self.logfile

        # Map from phase number to subphase number reached in that phase.
        # Phase 1 subphases are <started>, table1, table2, ...
        # Phase 2 subphases are <started>, table7, table6, ...
        # Phase 3 subphases are <started>, tables1&2, tables2&3, ...
        # Phase 4 subphases are <started>
        phase_subphases = {}
        phase_subphases[1] = 1

        with open(self.logfile, 'r') as f:
            for line in f:
                # [P1] Table 1 took 10.7436 sec
                # [P3-1] Table 2 took 26.8535 sec, wrote 3429434557 right entries
                m = re.match(r'^\[P(\d).*\] Table (\d).*', line)
                if m:
                    phase = int(m.group(1))
                    if phase == 2:
                        phase_subphases[phase] = 7 - int(m.group(2)) + (2 if "rewrite" in line else 1)
                    else :
                        phase_subphases[phase] = int(m.group(2)) + 1

                # Phase 1 took 631.199 sec
                m = re.match(r'^Phase (\d) took.*', line)
                if m:
                    phase = int(m.group(1)) + 1
                    phase_subphases[phase] = 1

                # # [P4] Starting to write C1 and C3 tables
                # m = re.match(r'^\[P4\].*', line)
                # if m:
                #     phase_subphases[4] = 1

                # TODO also collect timing info:

                # "Time for phase 1 = 22796.7 seconds. CPU (98%) Tue Sep 29 17:57:19 2020"
                # for phase in ['1', '2', '3', '4']:
                    # m = re.match(r'^Time for phase ' + phase + ' = (\d+.\d+) seconds..*', line)
                    # data.setdefault....

                # Total time = 49487.1 seconds. CPU (97.26%) Wed Sep 30 01:22:10 2020
                # m = re.match(r'^Total time = (\d+.\d+) seconds.*', line)
                # if m:
                    # data.setdefault(key, {}).setdefault('total time', []).append(float(m.group(1)))

        if phase_subphases:
            phase = max(phase_subphases.keys())
            self.phase = Phase(major=phase, minor=phase_subphases[phase])
        else:
            self.phase = Phase(major=0, minor=0)

    def progress(self):
        '''Return a 2-tuple with the job phase and subphase (by reading the logfile)'''
        return self.phase

    def plot_id_prefix(self):
        return self.plot_id[:8]

    # TODO: make this more useful and complete, and/or make it configurable
    def status_str_long(self):
        return '{plot_id}\nk={k} r={r} b={b} u={u}\npid:{pid}\ntmp:{tmp}\ntmp2:{tmp2}\ndst:{dst}\nlogfile:{logfile}'.format(
            plot_id=self.plot_id,
            k=self.k,
            r=self.r,
            b=self.b,
            u=self.u,
            pid=self.proc.pid,
            tmp=self.tmpdir,
            tmp2=self.tmp2dir,
            dst=self.dstdir,
            plotid=self.plot_id,
            logfile=self.logfile
        )

    def get_mem_usage(self):
        return self.proc.memory_info().vms  # Total, inc swapped

    def get_tmp_usage(self):
        total_bytes = 0
        try:
            with os.scandir(self.tmpdir) as it:
                for entry in it:
                    if self.plot_id in entry.name:
                        try:
                            total_bytes += entry.stat().st_size
                        except FileNotFoundError:
                            # The file might disappear; this being an estimate we don't care
                            pass
        except OSError as e:
            print(f"ERROR: {e}\n dir: {self.tmpdir}")
        return total_bytes

    def get_run_status(self):
        '''Running, suspended, etc.'''
        status = self.proc.status()
        if status == psutil.STATUS_RUNNING:
            return 'RUN'
        elif status == psutil.STATUS_SLEEPING:
            return 'SLP'
        elif status == psutil.STATUS_DISK_SLEEP:
            return 'DSK'
        elif status == psutil.STATUS_STOPPED:
            return 'STP'
        else:
            return self.proc.status()

    def get_time_wall(self):
        create_time = datetime.fromtimestamp(self.proc.create_time())
        return int((datetime.now() - create_time).total_seconds())

    def get_time_user(self):
        return int(self.proc.cpu_times().user)

    def get_time_sys(self):
        return int(self.proc.cpu_times().system)

    def get_time_iowait(self):
        cpu_times = self.proc.cpu_times()
        iowait = getattr(cpu_times, 'iowait', None)
        if iowait is None:
            return None

        return int(iowait)

    def suspend(self, reason=''):
        self.proc.suspend()
        self.status_note = reason

    def resume(self):
        self.proc.resume()

    def get_temp_files(self):
        single_file_path = None
        for f in self.proc.open_files():
            if any(
                dir in f.path
                for dir in [self.tmpdir, self.tmp2dir, self.dstdir]
                if dir is not None
            ):
                single_file_path = f.path
                break

        if not single_file_path:
            print(f"ERROR: not able to locate dir. The drive may be down.")
            return []

        # Some associated files are not opened by the process at a specific time, so a glob
        # through the directory is necessary
        m = re.match(r'.*plot-k32-.*-([0-9a-f]*)\..*tmp', single_file_path)
        if m:
            full_plot_id = m.group(1)
        else:
            full_plot_id_start = single_file_path.find(self.plot_id)
            full_plot_id_end = single_file_path.find(".plot", full_plot_id_start)
            full_plot_id = single_file_path[full_plot_id_start: full_plot_id_end]
        glob_path = os.path.join(os.path.dirname(
            single_file_path), f"*{full_plot_id}*")
        temp_files = glob.glob(glob_path)
        return temp_files

    def cancel(self):
        'Cancel an already running job'
        # We typically suspend the job as the first action in killing it, so it
        # doesn't create more tmp files during death.  However, terminate() won't
        # complete if the job is supsended, so we also need to resume it.
        # TODO: check that this is best practice for killing a job.
        self.proc.resume()
        self.proc.terminate()
