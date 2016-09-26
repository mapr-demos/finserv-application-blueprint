import pandas as pd
import datetime
import time
import numpy as np 
import sys
import random
import re
from joblib import Parallel, delayed
import multiprocessing

# Constants
# ---------
#
# Configure these to fit your environment.

# desired rate per second
#TARGET_RATE = 1000
TARGET_RATE = 300000

# small sampling
NLINES = 100000

# 1/3 of file (approx 36279156 lines total)
#NLINES = 12093052

# complete file
#NLINES = -1

# number of minutes before the trade to start
# making the bids + asks
BID_ASK_START = 3

# range of bid/ask spreads, as a percentage of the
# instrument price
BID_ASK_SPREAD_LOWER = 0.05
BID_ASK_SPREAD_UPPER = 0.1

# range of how many receivers should be for each
# bid + ask
N_RECV_LOWER = 1
N_RECV_UPPER = 10

# trades input file
INPUT_FILE = "/home/mapr/finserv-ticks-demo/taqtrade20131218"

# directory for per-second output files
OUTPUT_DIR = "/home/mapr/finserv-ticks-demo/data/"

# expected length of a single record in the input
LINELEN = 73

# receiver and sender ID pools, along with their probabilities
send_id  = range(1000, 1999)
recv_id = range(2000, 2999)

# this makes an array with random numbers that sum to 1
send_p = np.random.dirichlet(np.ones(len(send_id)), size=1)[0]
recv_p  = np.random.dirichlet(np.ones(len(recv_id)), size=1)[0]
types = [ ('B', -1.0), ('A', 1.0) ]

def make_ent(d, exc, sym, typ, scn, tvm, tpr,
        tsp, tci, tsq, tsrc, trf, snd, rcv):
    return ({
            'date' : d,
            'exchange' : exc,
            'symbol' : sym,
            'type' : typ,
            'saleCondition' : scn,
            'tradeVolume' : tvm,
            'tradePrice' : tpr,
            'tradeStopStockIndicator' : tsp,
            'tradeCorrectionIndicator' : tci,
            'tradeSequenceNumber' : tsq,
            'tradeSource' : tsrc,
            'tradeReportingFacility' : trf,
            'sendID' : snd,
            'recvID' : rcv
    })

# update the set of indices from which we are generating bid/asks
def move_window(df, curidx, curwin, curtime):
    # load the trades from the next second
    tidx = []

    print "move_index: curidx %s end %s" % (str(curidx), str(df.index[-1]))
    print "row: %s curtime %s" % (str(get_row_date_dt(df.ix[curidx])), str(curtime))

    # get the right edge of the window
    gentime = curtime + datetime.timedelta(minutes=BID_ASK_START)

    # shift in the new trades, if there are any in this
    # incoming second
    while (curidx < df.index[-1] and
            (get_row_date_dt(df.ix[curidx]) < gentime + datetime.timedelta(seconds=1))):
        tidx.append(curidx)
        curidx += 1
    if (len(tidx) > 0):
        curwin.append(tidx)

    # shift out the old ones unless we are bootstrapping
    if (curidx >= (BID_ASK_START * 60) and len(curwin) > 0):
        curwin = curwin[1:]

    return (curidx, curwin)

def get_row_date_dt(row):
    strdate = row['date']
    fullstr = "13/12/13 %s:%s:%s" % \
        (strdate[0:2], strdate[2:4], strdate[4:6])
    return (datetime.datetime.strptime(fullstr, "%d/%m/%y %H:%M:%S"))

def make_bid_ask(send_id, send_p, recv_id, recv_p, trades, types, tstr):
        # these are currently disjoint sets so we don't
        # check for the same recv/sender id
        n_recv = random.randint(N_RECV_LOWER, N_RECV_UPPER)
        sid = "%04d" % np.random.choice(send_id, size=1, p=send_p)[0]
        rids = []
        for j in range(0, n_recv):
            rids.append("%04d" % np.random.choice(recv_id, size=1, p=recv_p)[0])

        # may want some weights here too
        tt = trades.sample(n=1).iloc[0]
        ty, mult = types[random.randint(0, len(types) - 1)]
        pr = float(tt.tradePrice)

        # get the price, volume of this bid/ask
        newpr = "%012.04f" % \
            round((pr + (pr * random.uniform(BID_ASK_SPREAD_LOWER, \
                BID_ASK_SPREAD_UPPER) * mult)), 2)
        newvol = "%09d" % (random.randint(1, int(tt.tradeVolume)))

        # and remove the decimal
        newpr = re.sub('\.', '', newpr)

        # add this new bid or ask
        return (make_ent(tstr, tt.exchange,
                tt.symbol, ty, tt.saleCondition, newvol, newpr,
                tt.tradeStopStockIndicator, tt.tradeCorrectionIndicator,
                tt.tradeSequenceNumber, tt.tradeSource,
                tt.tradeReportingFacility,
                sid,
                rids))
        # print 'adding row %s' % str(nr)

# fill this second with bid/asks
def fill_bid_ask(df, indices, starttime):
    # get all the trades for which we are generating bid/asks
    trades = df.iloc[indices]
    tstr = starttime.strftime("%H%M%S%F")
    # seems to be the sweet spot on dual socket i7-920
    nc = multiprocessing.cpu_count() / 4
    print "parallelizing to %d jobs" % nc
    newents = Parallel(verbose=True, n_jobs=nc)(delayed(make_bid_ask)(send_id,
             send_p, recv_id, recv_p, trades, types, tstr) for i in range(0, TARGET_RATE))

    return (pd.DataFrame(newents))

def parse(line):
    r = make_ent(line[0:9],
            line[9:10],
            line[10:26],
            'T',
            line[26:30],
            line[30:39].lstrip('0'),
            (line[39:46] + '.' + line[46:50]).strip('0'),
            line[50:51],
            line[51:53],
            line[53:69],
            line[69:70],
            line[70:71],
            None,
            None)
    if (len(line) != LINELEN):
        raise ValueError("Expected line to be 73 characters, got " + str(len(line)))
    return r

def output_trades(df, bdf, secid, seq):
    ss = secid.strftime("%H%M%S")
    fn = "%s/%s" % (OUTPUT_DIR, ss)

    # merge and sort the bid/asks with the actual trades, by time
    combodf = bdf.append(df[df.date.str[:6] == ss]).sort_values(['date'], ascending=1)

    # assign seq numbers
    combodf.tradeSequenceNumber = [ format(x, '016d') for x in range(seq, seq + len(combodf.index)) ]
    seq += len(combodf.index)

    print "writing file %s" % fn
    with open(fn, "w") as output:
        for i, r in combodf.iterrows():
            outstr = "%9s%1s%16s%4s%9s%11s%1s%2s%16s%1s%1s" % (r.date[0:9],
                    r.exchange,
                    r.symbol,
                    r.saleCondition,
                    r.tradeVolume,
                    r.tradePrice,
                    r.tradeStopStockIndicator,
                    r.tradeCorrectionIndicator,
                    r.tradeSequenceNumber,
                    r.tradeSource,
                    r.tradeReportingFacility)
            # check for errors in length before we add the senders/receivers
            if (len(outstr) != LINELEN - 2):
                sys.stderr.write("fatal:  output line len %d, expected %d, malformed\n" % (len(outstr), LINELEN))
                sys.exit(1)
            output.write('%s' % outstr)
            if (r.type == 'B' or r.type == 'A'):
                output.write("%s" % r.sendID)
                for ent in r.recvID:
                    output.write("%s" % ent)
            output.write('\n')
    output.close()
    return (seq)

i = 0
dlines = []
print "reading file (NLINES = %d)" % NLINES
try:
    with open(INPUT_FILE, "r") as input:
        next(input)
        for x in input:
            i += 1
            if (NLINES != -1 and i > NLINES):
                break
            dlines.append(parse(x))
except KeyboardInterrupt:
    print("interrupted after " + str(i/1e6) + " M lines")

print "parsed %d lines" % len(dlines)
df = pd.DataFrame(dlines).sort_values(['date'], ascending = 1).reset_index()

# take the first event in the file and start at the beginning
# of that second
entsidx = []
windowidx = []
allbidasks = []

# grab the first time in the file
firstent = df.iloc[0]
endent = df.iloc[-1]
ftime = get_row_date_dt(firstent)
endtime = get_row_date_dt(endent)
wsec = ftime - datetime.timedelta(minutes=BID_ASK_START)
seq = curidx = 0

print "making bid/asks..."
while (wsec <= endtime):
    print "time %s seq %d" % (str(wsec), seq)
    curidx, windowidx = move_window(df, curidx, windowidx, wsec)
    # print "new window size %d" % len(windowidx)

    # if our window is empty, grab a future entry and backfill until
    # we get another one, this happens if there is a large time gap
    # in the input without any trades (for example if we did not
    # use the whole file)
    while (len(windowidx) == 0):
        print "backfilling from row %s" % df[curidx]
        seq = output_trades(df, fill_bid_ask(df, [ df[curidx] ], wsec), wsec, seq)
        wsec += datetime.timedelta(seconds=1)
        print "new time %s" % str(wsec)
        curidx, windowidx = move_window(df, curidx, windowidx, wsec)

    # write the bids, asks, and trades for this second
    seq = output_trades(df,
        fill_bid_ask(df, [i for we in windowidx for i in we], wsec), wsec, seq)

    # move to the next second
    wsec += datetime.timedelta(seconds=1)

print "done"
