import os
import sys
from os import listdir
from os.path import isfile, join
from stat import ST_CTIME
from datetime import datetime, timezone
from datetime import timezone
from subprocess import Popen, PIPE

queryStr = sys.argv[1]
temp = queryStr.split('-')
month = int(temp[0])
day = int(temp[1])

mypath = "/home/wqxplot/.chia/mainnet/plotman_logs"

onlyfiles = [join(mypath, f) for f in listdir(mypath) if isfile(join(mypath, f))]
entries = ((os.stat(path), path) for path in onlyfiles)

selected_path_arr = []
for stat, path in entries :
    timeObj = (datetime.fromtimestamp(stat[ST_CTIME]))
    if month == timeObj.month and day == timeObj.day :
        selected_path_arr.append(path)

if len(selected_path_arr) != 0:
    process = Popen(["plotman", "analyze"] + selected_path_arr, stdout=PIPE)
    (output, err) = process.communicate()
    exit_code = process.wait()
    print(output.decode("utf-8"))
    print("Exit code: " + str(exit_code))

    #print(selected_path_str)




