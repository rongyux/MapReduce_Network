"""
mapper
"""

import sys

cmatch_list = ["222", "223"]

for line in sys.stdin:
    data = line.strip("\n").split("\t")
    if len(data) < 124:
        continue
    cmatch = data[9]
    if cmatch not in cmatch_list:
        continue
    show = data[0]
    query = data[3]
    searchid = data[6]
    rank = data[12]
    bid = float(data[19])
    term = data[23]
    pid = data[33]
    cid = data[34]
    if data[124] == '-' or data[124] == '_':
        continue
    ubmq = float(data[124])

    ecpm = bid * ubmq / 100000
    bucket_max = 1
    ecpm_bucket = int(ecpm/bucket_max)

    print "\t".join([searchid, rank, str(ecpm_bucket), pid, cid, term])

