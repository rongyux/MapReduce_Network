"""
reducer
"""

import sys

pre_key = ""

def post_deal(pre_key):
    """
    post deal
    """
    global tmp_c
    k_ = pre_key.split('_')
    ecpm = k_[-1]
    print "%s\t%s\t%s"%("\t".join(k_[:-1]), ecpm, tmp_c)

def deal(data):
    """
    deal
    """
    global tmp_c
    tmp_c += 1

def pre_deal():
    """
    pre deal
    """
    global tmp_c
    tmp_c = 0

for line in sys.stdin:
    data = line.strip().split("\t")
    if len(data) < 2:
        continue
    key = "%s_%s"%(data[0], data[1])

    if key != pre_key:
        if pre_key != "":
            post_deal(pre_key)

        pre_deal()
        pre_key = key

    deal(data)

if pre_key != "":
    post_deal(pre_key)
