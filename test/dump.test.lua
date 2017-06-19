#!/usr/bin/env tarantool

local dump = require('dump')
local tap = require('tap')
local fio = require('fio')
local fiber = require('fiber')

local test = tap.test('dump tests')

test:plan(2)

function basic(test)
    test:plan(2)
    test:is(type(dump.dump), "function", "Dump function is present")
    test:is(type(dump.restore), "function", "Restore function is present")
end

function dump_and_restore(test)
    test:plan(1)
    local ROWS = 200
    box.schema.space.create('memtx')
    box.space.memtx:create_index('pk')
    box.schema.space.create('vinyl', {engine = 'vinyl'})
    box.space.vinyl:create_index('pk')
    box.begin()
    for i = 1, ROWS do
        box.space.memtx:insert{i, fiber.time()}
    end
    box.commit()
    box.begin()
    for i = 1, ROWS do
        box.space.vinyl:insert{i, fiber.time()}
    end
    box.commit()
    local dir = fio.tempdir() 
    dump.dump(dir)
    box.space.memtx:drop()
    box.space.vinyl:drop()
    dump.restore(dir)
    local rows = 0 
    for i = 1, ROWS do
        local i1 = box.space.memtx:get{i}
        local i2 = box.space.vinyl:get{i}
        if i1 and i2 and i1[1] == i2[1] then
           rows = rows + 1 
        end
    end
    test:is(rows, ROWS, "The number of rows is correct after restore")
    box.space.memtx:drop()
    box.space.vinyl:drop()
    for k, file in pairs(fio.glob(dir)) do
        fio.unlink(file)
    end
    fio.unlink(dir)
end

test:test('basic', basic)

box.cfg{}

test:test('dump and restore', dump_and_restore)

os.exit(test:check() == true and 0 or -1)
