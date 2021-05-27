import math
import os
import re
import shutil
import subprocess

GB = 1_000_000_000

def df_b(d):
    'Return free space for directory (in bytes)'
    usage = shutil.disk_usage(d)
    return usage.free

def get_k32_plotsize():
    return 108 * GB

def human_format(num, precision, powerOfTwo=False):
    divisor = 1024 if powerOfTwo else 1000
    
    magnitude = 0
    while abs(num) >= divisor:
        magnitude += 1
        num /= divisor        
    result = (('%.' + str(precision) + 'f%s') %
            (num, ['', 'K', 'M', 'G', 'T', 'P'][magnitude]))

    if powerOfTwo and magnitude > 0:
	    result += 'i'
    
    return result

def time_format(sec):
    if sec is None:
        return '-'
    if sec < 60:
        return '%ds' % sec
    else:
        return '%d:%02d' % (int(sec / 3600), int((sec % 3600) / 60))

def tmpdir_phases_str(tmpdir_phases_pair):
    tmpdir = tmpdir_phases_pair[0]
    phases = tmpdir_phases_pair[1]
    phase_str = ', '.join(['%d:%d' % ph_subph for ph_subph in sorted(phases)])
    return ('%s (%s)' % (tmpdir, phase_str))

def split_path_prefix(items):
    if not items:
        return ('', [])

    prefix = os.path.commonpath(items)
    if prefix == '/':
        return ('', items)
    else:
        remainders = [ os.path.relpath(i, prefix) for i in items ]
        return (prefix, remainders)

def list_k32_plots(d):
    'List completed k32 plots in a directory (not recursive)'
    plots = []
    try:
        for plot in os.listdir(d):
            if re.match(r'^plot-k32-.*plot$', plot):
                plot = os.path.join(d, plot)
                try:
                    if os.stat(plot).st_size > (0.95 * get_k32_plotsize()):
                        plots.append(plot)
                except FileNotFoundError:
                    continue
    except OSError as e:
        print(f"ERROR: {e} \n dir: {d}")

    return plots

def column_wrap(items, n_cols, filler=None):
    '''Take items, distribute among n_cols columns, and return a set
       of rows containing the slices of those columns.'''
    rows = []
    n_rows = math.ceil(len(items) / n_cols)
    for row in range(n_rows):
        row_items = items[row : : n_rows]
        # Pad and truncate
        rows.append( (row_items + ([filler] * n_cols))[:n_cols] )
    return rows

def get_numa_cpu_list() :
    out = subprocess.check_output("numactl --hardware", shell=True, start_new_session=True).decode("utf-8")
    re_iter = re.finditer("node [\d] cpus: ", out)
    node_cpu_lists = []
    for match in re_iter:
        list_start_index = match.end(0)
        node_cpu_list = list(map(int, out[list_start_index : out.find("\n", list_start_index)].split()))
        node_cpu_lists.append(node_cpu_list)

    return node_cpu_lists

def get_cpu_to_numa_node() :
    node_cpu_lists = get_numa_cpu_list() 
    cpu_cnt = 0
    for cpu_list in node_cpu_lists:
        cpu_cnt += len(cpu_list)

    numa_cpus = [-1]*cpu_cnt # index = cpu, value = numa node
    for numa_node in range (len(node_cpu_lists)) :
        for cpu in node_cpu_lists[numa_node] :
            numa_cpus[cpu] = numa_node

    return numa_cpus
