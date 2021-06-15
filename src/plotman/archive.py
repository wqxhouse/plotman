import argparse
import contextlib
import math
import os
import posixpath
import random
import re
import subprocess
import sys
from datetime import datetime
import re

import psutil
import texttable as tt

from plotman import job, manager, plot_util

# TODO : write-protect and delete-protect archived plots

def launch_rsync_async(cmd, dir_cfg) :
    plotname = cmd[cmd.rfind("/plot-") + 1 : cmd.rfind(".plot")]
    archive_file_name = os.path.join(dir_cfg.log, "archive_out_" + plotname + ".log")
    try:
        archive_file = open(archive_file_name, "x")
    except FileExistsError:
        # The desired log file name already exists.  Most likely another
        # plotman process already launched a new process in response to
        # the same scenario that triggered us.  Let's at least not
        # confuse things further by having two plotting processes
        # logging to the same file.  If we really should launch another
        # plotting process, we'll get it at the next check cycle anyways.
        message = (
            f'Archive log file already exists, skipping attempt to start a'
            f' new plot: {archive_file_name!r}'
        )
        # print(message)
        archive_file = open(archive_file_name, 'w')
    except FileNotFoundError as e:
        message = (
            f'Unable to open log file.  Verify that the directory exists'
            f' and has proper write permissions: {archive_file_name!r}'
        )
        raise Exception(message) from e

    
    with archive_file :
        p = subprocess.Popen(cmd,
            shell=True,
            stdout=archive_file,
            stderr=archive_file,
            start_new_session=True)
        return p

def get_eligible_tunnel_ips(arch_jobs) :
    out = subprocess.check_output("ifconfig", shell=True, start_new_session=True).decode("utf-8")
    ips = [ip[5:] for ip in list(re.findall("inet \d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}", out))]
    if "127.0.0.1" in ips :
        ips.remove("127.0.0.1")
    for job in arch_jobs :
        if job[2] != 'D' :
            running_tunnel_ip = job[2]
            if running_tunnel_ip not in ips :
                print("Something is wrong")
            else :
                ips.remove(running_tunnel_ip)
        # else :
            # cannot avoid potentially and ip dest

    return ips

def spawn_archive_process(dir_cfg, all_jobs):
    '''Spawns a new archive process using the command created
    in the archive() function. Returns archiving status and a log message to print.'''

    log_message = None
    archiving_status = None

    # Look for running archive jobs.  
    arch_jobs = get_running_archive_jobs(dir_cfg.archive)

    (should_start, status_or_cmd) = archive(dir_cfg, all_jobs, arch_jobs)
    if not should_start:
        archiving_status = status_or_cmd
    else:
        cmd = status_or_cmd
        # TODO: do something useful with output instead of DEVNULL
        p = launch_rsync_async(cmd, dir_cfg)
        log_message = 'Starting archive: ' + cmd
        # At least for now it seems that even if we get a new running
        # archive jobs list it doesn't contain the new rsync process.
        # My guess is that this is because the bash in the middle due to
        # shell=True is still starting up and really hasn't launched the
        # new rsync process yet.  So, just put a placeholder here.  It
        # will get filled on the next cycle.
        arch_jobs.append(('<pending>', '?.plot', 'D'))

    all_archiving_pids = []
    for arch_j in arch_jobs :
        all_archiving_pids.append(arch_j[0])
    if archiving_status is None:
        archiving_status = 'pid: ' + ', '.join(map(str, all_archiving_pids))

    return archiving_status, log_message

def compute_priority(phase, gb_free, n_plots):
    # All these values are designed around dst buffer dirs of about
    # ~2TB size and containing k32 plots.  TODO: Generalize, and
    # rewrite as a sort function.

    priority = 50

    # To avoid concurrent IO, we should not touch drives that
    # are about to receive a new plot.  If we don't know the phase,
    # ignore.
    if (phase.known):
        if (phase == job.Phase(3, 4)):
            priority -= 4
        elif (phase == job.Phase(3, 5)):
            priority -= 8
        elif (phase == job.Phase(3, 6)):
            priority -= 16
        elif (phase >= job.Phase(3, 7)):
            priority -= 32

    # If a drive is getting full, we should prioritize it
    if (gb_free < 1000):
        priority += 1 + int((1000 - gb_free) / 100)
    if (gb_free < 500):
        priority += 1 + int((500 - gb_free) / 100)
    if (gb_free < 250):
        # Needs to drive  unable to proceed the top priority
        priority += 10
    if (gb_free < 110):
        priority += 1000
    if (gb_free < 50):
        priority += 10000
    if (gb_free < 25):
        priority += 100000
    if (gb_free < 12):
        priority += 1000000
    if (gb_free < 6):
        priority += 10000000
    if (gb_free < 3):
        priority += 100000000
    if (gb_free == 0):
        priority += 1000000000

    # Finally, least importantly, pick drives with more plots
    # over those with fewer.
    priority += n_plots

    return priority

def get_archdir_freebytes(arch_cfg):
    archdir_freebytes = {}
    df_cmd = ('ssh %s@%s df -aBK | grep " %s/"' %
        (arch_cfg.rsyncd_user, arch_cfg.rsyncd_host, posixpath.normpath(arch_cfg.rsyncd_path)) )
    with subprocess.Popen(df_cmd, shell=True, stdout=subprocess.PIPE) as proc:
        for line in proc.stdout.readlines():
            fields = line.split()
            if fields[3] == b'-':
                # not actually mounted
                continue
            freebytes = int(fields[3][:-1]) * 1024  # Strip the final 'K'
            archdir = (fields[5]).decode('utf-8')
            archdir_freebytes[archdir] = freebytes
    return archdir_freebytes

def rsync_dest(arch_cfg, arch_dir):
    if arch_cfg.rsyncd_host.lower().strip() != "localhost":
        rsync_path = arch_dir.replace(arch_cfg.rsyncd_path, arch_cfg.rsyncd_module)
        if rsync_path.startswith('/'):
            rsync_path = rsync_path[1:]  # Avoid dup slashes.  TODO use path join?
        rsync_url = 'rsync://%s@%s:12000/%s' % (
                arch_cfg.rsyncd_user, arch_cfg.rsyncd_host, rsync_path)
    else :
        rsync_url = arch_dir
    return rsync_url

def get_running_archive_logs(cfg_directories) :
    arch_jobs = get_running_archive_jobs(cfg_directories.archive)
    archive_file_name_regex = os.path.join(cfg_directories.log, "archive_out_plot-.{1,}\.log")
    file_path_to_pids = {}
    for j in arch_jobs:
        pid = j[0]
        out = subprocess.check_output(f"lsof -p {pid}", shell=True, start_new_session=True).decode('utf-8')
        result = re.search(archive_file_name_regex, out)
        if result :
            file_path = out[result.start() : result.end()]
            if file_path not in file_path_to_pids:
                file_path_to_pids[file_path] = [pid]
            else :
                file_path_to_pids[file_path].append(pid)


    print(f"Archive count: {len(file_path_to_pids)}")
    for file_path, pids in file_path_to_pids.items() :
        print(f"PID {pids}: {file_path}")
        with open(file_path, 'r') as f:
            print([i for i in f.read().split('\n') if i][-1])

# TODO: maybe consolidate with similar code in job.py?
def get_running_archive_jobs(arch_cfg):
    '''Look for running rsync jobs that seem to match the pattern we use for archiving
       them.  Return a list of PIDs of matching jobs.'''
    jobs = []
    dest = rsync_dest(arch_cfg, '/')
    for proc in psutil.process_iter(['pid', 'name']):
        with contextlib.suppress(psutil.NoSuchProcess):
            if proc.name() == 'rsync':
                args = proc.cmdline()
                tunnel_ip = "D"
                found_pid = None
                plot_name = None
                for arg in args:
                    if arg.startswith(dest):
                        found_pid = proc.pid
                    if arg.endswith(".plot"):
                        plot_name = arg
                    if arg.startswith("--address"):
                        start_idx = len("--address=")
                        tunnel_ip = arg[start_idx :]
                if found_pid :
                    jobs.append((found_pid, plot_name, tunnel_ip))
    return jobs

def next_chosen_plot_cli(dir_cfg, all_jobs):
    arch_jobs = get_running_archive_jobs(dir_cfg.archive)
    return next_chosen_plot(dir_cfg, all_jobs, arch_jobs)

def next_chosen_plot(dir_cfg, all_jobs, arch_jobs):
    cur_archiving_plots = []
    for arch_job in arch_jobs:
        cur_archiving_plots.append(arch_job[1])

    dir2ph = manager.dstdirs_to_furthest_phase(all_jobs)
    best_priority = -100000000
    chosen_plot = None
    dst_dir = dir_cfg.get_dst_directories()
    for d in dst_dir:
        ph = dir2ph.get(d, job.Phase(0, 0))
        dir_plots = plot_util.list_k32_plots(d)

        gb_free = plot_util.df_b(d) / plot_util.GB
        n_plots = len(dir_plots)
        priority = compute_priority(ph, gb_free, n_plots)
        if priority >= best_priority and dir_plots:
            best_priority = priority

            for plot_candidate in dir_plots :
                if plot_candidate not in cur_archiving_plots:
                    chosen_plot = plot_candidate
                    break

    return chosen_plot

def archive(dir_cfg, all_jobs, arch_jobs):
    '''Configure one archive job.  Needs to know all jobs so it can avoid IO
    contention on the plotting dstdir drives.  Returns either (False, <reason>)
    if we should not execute an archive job or (True, <cmd>) with the archive
    command if we should.'''
    if dir_cfg.archive is None:
        return (False, "No 'archive' settings declared in plotman.yaml")

    max_arch_jobs = dir_cfg.archive.max_concurrent_transfer if dir_cfg.archive.max_concurrent_transfer else 1
    if dir_cfg.archive.rsyncd_host.lower().strip() == 'localhost':
        # * 3 because local rsync has 3 threads
        max_arch_jobs *= 3
    if len(arch_jobs) >= max_arch_jobs :
        return (False, None)

    chosen_plot = next_chosen_plot(dir_cfg, all_jobs, arch_jobs)
    if not chosen_plot:
        return (False, 'No plots found')

    # tunnel_ips = get_eligible_tunnel_ips(arch_jobs)
    # if not tunnel_ips :
    #     # return (False, 'No eligible tunnel ip left')
    #     return (False, None)
    # selected_tunnel_ip = tunnel_ips[0]

    # TODO: sanity check that archive machine is available
    # TODO: filter drives mounted RO

    #
    # Pick first archive dir with sufficient space
    #
    archdir_freebytes = get_archdir_freebytes(dir_cfg.archive)
    if not archdir_freebytes:
        return(False, 'No free archive dirs found.')

    archdir = ''
    available = [(d, space) for (d, space) in archdir_freebytes.items() if
                 space > 1.2 * plot_util.get_k32_plotsize()]
    if len(available) > 0:
        index = min(dir_cfg.archive.index, len(available) - 1)
        (archdir, freespace) = sorted(available)[index]

    if not archdir:
        return(False, 'No archive directories found with enough free space')

    msg = 'Found %s with ~%d GB free' % (archdir, freespace / plot_util.GB)

    bwlimit = dir_cfg.archive.rsyncd_bwlimit
    throttle_arg = ('--bwlimit=%d' % bwlimit) if bwlimit else ''
    # cmd = ('rsync %s -v -h --compress-level=0 --address=%s --remove-source-files -P %s %s' %
    #         (throttle_arg, selected_tunnel_ip, chosen_plot, rsync_dest(dir_cfg.archive, archdir)))
    cmd = ('rsync %s -v -h --compress-level=0 --remove-source-files -P %s %s' %
            (throttle_arg, chosen_plot, rsync_dest(dir_cfg.archive, archdir)))

    return (True, cmd)
