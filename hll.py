"""
    Redis HLL Test
    ~~~~~~~~~~~~~~

    Some tests of the speed and correctness of the new Redis HyperLogLog
    data structure.

    Requires the redis python library. Best used with PyPy.
"""

from __future__ import division, print_function

import sys
import time
from argparse import ArgumentParser

from redis import Redis


TESTSETS = {
    'mixed': [10, 20, 100, 900,
              1000, 2000, 5000,
              10000, 50000,
              200000, 500000, 800000,
              1000000, 5000000,
              10000000,
              100000000  # Takes very long! (50 sec on my machine)
             ],
    'hundred': range(1, 101),
}


def writestatus(msg, *fargs):
    """Print current status (useful for long-taking operations)"""
    text = msg.format(*fargs)
    sys.stdout.write(text)
    sys.stdout.write("\b" * len(text))
    sys.stdout.flush()


def run_test(key, testset):
    """
    Test HyperLogLog with a different set of items and print a table
    of results to stdout.

    :param key: Redis key
    :param testset: Name of testset
    """
    print("   Expect     Count   Diff %   Size   Time_Ins  Time_Count")
    print("==========================================================")
    redis = Redis()
    test_counts = TESTSETS[testset]

    for count in test_counts:
        redis.delete(key)

        start = time.time()
        if count <= 1000000:
            redis.execute_command("PFADD", key, *range(count))
        else:
            # Redis may disconnect on huge add commands,
            # so we limit at 1 million
            chunksize = 1000000
            sendstart = 0
            sendcount = count
            _sendcount = 0
            while sendcount > 0:
                writestatus("  -> {:>5.2f} % of items sent",
                            (count - sendcount) / count * 100)
                sendstart += chunksize
                nums = range(sendstart, sendstart + chunksize)
                redis.execute_command("PFADD", key, *nums)
                _sendcount += len(nums)
                sendcount -= chunksize
            assert _sendcount == count, "Sendloop has an error"
        time_ins = time.time() - start

        start = time.time()
        hll_count = redis.execute_command("PFCOUNT", key)
        time_count = time.time() - start

        difference_percent = (hll_count - count) / count * 100
        print("{:>9} {:>9} {:>8.2f} {:>6} {:>10.5f}  {:>10.5f}".format(
            count, hll_count, difference_percent,
            redis.execute_command("DEBUG", "object", key)['serializedlength'],
            time_ins, time_count
        ))


def main():
    """Run the testtool"""
    parser = ArgumentParser()
    parser.add_argument("--key", "-k", help="Test key", default="hll_test")
    parser.add_argument("--testset", "-s", help="Set of tests",
                        choices=TESTSETS.keys(), default="mixed")
    args = parser.parse_args()

    run_test(args.key, args.testset)


if __name__ == '__main__':
    main()
