"""
reducer
"""

import sys

pre_key = ""

def post_deal(pre_key):
    """
    post deal
    """
    global tmp_list
    print "\t".join(tmp_list) #get the last cpm of searchid by rank sort

def deal(data):
    """
    deal
    """
    global tmp_list
    tmp_list = list()
    k_red = "%s_%s_%s"%(data[3], data[4], data[5])
    ecpm = data[2]
    tmp_list.append(k_red)
    tmp_list.append(ecpm)

def pre_deal():
    """
    pre deal
    """
    global tmp_list

for line in sys.stdin:
    data = line.strip().split("\t")
    if len(data) < 4:
        continue
    key = data[0]

    if key != pre_key:
        if pre_key != "":
            post_deal(pre_key)

        pre_deal()
        pre_key = key

    deal(data)

if pre_key != "":
    post_deal(pre_key)
