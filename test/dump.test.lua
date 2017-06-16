#!/usr/bin/env tarantool

local dump = require('dump')
local tap = require('tap')

local test = tap.test('dump tests')

test:plan(1)

function basic(test)
    test:plan(2)
    test:is(type(dump.dump), "function", "Dump function is present")
    test:is(type(dump.restore), "function", "Restore function is present")
end

test:test('basic', basic)

os.exit(test:check() == true and 0 or -1)
