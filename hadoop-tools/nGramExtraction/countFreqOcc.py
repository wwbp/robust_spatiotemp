#!/usr/bin/env python

import csv, sys, re
from pprint import pprint
import gzip, json

tmp_file = None
MAX_LINES = 1000000
num_feat_rows = 0

freqs = {}
group_ids = set()

p_thresh = float(sys.argv[1])
feat_rows = []

def warn(text, verbose = True):
    if verbose:
        print >>sys.stderr, text

for i, l in enumerate(sys.stdin):
    line = csv.reader([l.strip()]).next()
    feat_rows.append(line)
    freqs[line[1]] = freqs.get(line[1], 0) + 1
    num_feat_rows += 1
    group_ids.add(line[0])
    if i!= 0 and i % 1000000 == 0:
        warn("read %d lines    " % i)
        if num_feat_rows > MAX_LINES:
            if not tmp_file:
                print >>sys.stderr, "Switching to tmp file (will be slower)"
                tmp_file = gzip.open("/tmp/countFreqOcc.tmp.gz","wb+")
                csv_tmp = csv.writer(tmp_file)
            csv_tmp.writerows(feat_rows)
            feat_rows = []
if feat_rows and tmp_file:
    csv_tmp.writerows(feat_rows)

warn("read %d lines    " % i)
warn("Done reading")

thresh = int(p_thresh * len(group_ids))
warn("Features must be mentioned by at least %d groups (%f*%d)" % (thresh, p_thresh, len(group_ids)))

feats = set(feat for feat, count in freqs.iteritems() if count >= thresh)
warn("Keeping %d features out of %d (CTL-C if that's not the right number)" % (len(feats), len(freqs)))

w = csv.writer(sys.stdout)

warn("Writing OOV features first")
# FEAT ROW:
# group_id, feat, value, group_norm

if tmp_file:
    tmp_file.close()
    tmp_file = gzip.open("/tmp/countFreqOcc.tmp.gz","rb")
    feat_rows = csv.reader(tmp_file)

OOV = {gid: [] for gid in group_ids}
for l in feat_rows:
    if l[1] not in feats:
        OOV[l[0]].append(l)
w.writerows(map(lambda (gid, vals): [gid,"<OOV>",sum(map(lambda x: long(x[2]),vals)),sum(map(lambda x: float(x[3]),vals))], OOV.iteritems()))

if tmp_file:
    tmp_file.seek(0)

warn("Writing frequent features now")

w.writerows(l for l in feat_rows if l[1] in feats)
