#!/usr/bin/env python

import statsd

statsd.init_statsd()

counter = statsd.StatsdCounter('divisible by 3')

x = 0
while True:
    x += 1
    if not x % 3:
        counter += 1
