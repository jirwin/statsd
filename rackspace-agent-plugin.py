#!/usr/bin/env python

import os.path
import sys
import glob
import json


def load_state(state_file):
    try:
        fd = open(state_file, 'rb')
    except IOError as e:
        if e.errno == 2:
            return {}
        raise
    else:
        data = fd.read()
        return json.loads(data)


def update_state(watch_dir, state_file, state):
    timestamps = [int(x.split('.json')[0]) for x in state.keys()].sort()
    lastest_timestamp = timestamps.pop()

    for ts in timestamps:
        os.remove(os.path.join(watch_dir, ts + ".json"))

    with open(state_file, 'wb') as fd:
        fd.write(json.dumps(state))


def output_metrics(metrics):
    pass


def parse_file(file_path, offset=0):
    with open(file_path, 'rb') as fd:
        fd.seek(offset)
        data = fd.read()
        stats = json.loads(data)
        output_metrics(stats)
        return fd.tell()


def main():
    watch_dir = sys.args[1]
    state_file = os.path.join(watch_dir, '.state.json')
    state = load_state(state_file)
    files = glob.glob(os.path.join(watch_dir, '[0-9]*.json'))
    for file_path in files:
        relpath = os.path.relpath(file_path, watch_dir)
        state[relpath] = parse_file(file_path, state.get(relpath, 0))

    update_state(watch_dir, state_file, state)


if __name__ == "__main__":
    main()
