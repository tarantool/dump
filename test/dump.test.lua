#!/usr/bin/env tarantool

local dump = require('dump')
local tap = require('tap')
local fio = require('fio')
local fiber = require('fiber')
local json = require('json')

local test = tap.test('dump tests')

local function rmpath(path)
    local stat = fio.stat(path)
    if not stat then
        return
    end
    if stat:is_dir() then
        for k, file in pairs(fio.glob(fio.pathjoin(path, "*"))) do
            rmpath(file)
        end
        fio.rmdir(path)
    else
        fio.unlink(path)
    end
end

function basic(test)
    test:plan(10)
    test:is(type(dump.dump), "function", "Dump function is present")
    test:is(type(dump.restore), "function", "Restore function is present")
    local status, msg = dump.dump('dir', {invalid = true})
    test:is(status, nil, "Unknown dump option - status")
    test:is(msg, "Invalid option 'invalid'", "Unknown dump option - message")
    local status, msg = dump.restore('dir', {invalid = true})
    test:is(status, nil, "Unknown restore option - status")
    test:is(msg, "Invalid option 'invalid'", "Unknown restore option - message")
    local status, msg = dump.dump('dir', {filter = true})
    test:is(status, nil, "Invalid value of dump option - status")
    test:is(msg, "Invalid value for option 'filter': expected function, got boolean", 
        "Invalid value of dump option - message")
    local status, msg = dump.restore('dir', {filter = true})
    test:is(status, nil, "Invalid value of restore option - status")
    test:is(msg, "Invalid value for option 'filter': expected function, got boolean", 
        "Invalid value of restore option - message")
end

function box_is_configured(test)
    test:plan(4)
    local status, msg = dump.dump("/tmp/1")
    test:is(status, nil, "Dump before box.cfg{} status")
    test:like(msg, "box.cfg", "Dump before box.cfg{} message")
    local status, msg = dump.restore("/tmp/1")
    test:is(status, nil, "Restore before box.cfg{} status")
    test:like(msg, "box.cfg", "Restore before box.cfg{} message")
end

function dump_and_restore(test)
    test:plan(1)
    local ROWS = 2000
    local TXN_ROWS = 100
    box.schema.space.create('memtx')
    box.space.memtx:create_index('pk')
    box.schema.space.create('vinyl', {engine = 'vinyl'})
    box.space.vinyl:create_index('pk')
    for i = 0, ROWS/TXN_ROWS - 1 do
        box.begin()
        for j = 1, TXN_ROWS do
            box.space.memtx:insert{i*TXN_ROWS + j, fiber.time()}
        end
        box.commit()
    end
    for i = 0, ROWS/TXN_ROWS - 1 do
        box.begin()
        for j = 1, TXN_ROWS do
            box.space.vinyl:insert{i*TXN_ROWS + j, fiber.time()}
        end
        box.commit()
    end
    local dir = fio.tempdir()
    dump.dump(dir)
    box.space.memtx:drop()
    box.space.vinyl:drop()
    dump.restore(dir)
    local rows = 0
    for i = 1, ROWS do
        local i1 = box.space.memtx:get{i}
        local i2 = box.space.vinyl:get{i}
        if i1 and i2 and i1[1] == i2[1] and i1[1] == i then
           rows = rows + 1
        end
    end
    test:is(rows, ROWS, "The number of rows is correct after restore")
    box.space.memtx:drop()
    box.space.vinyl:drop()
    rmpath(dir)
end

local function dump_access_denied(test)
    test:plan(2)
    local dir = fio.tempdir()
    fio.chmod(dir, 0)
    local status, msg = dump.dump(dir)
    test:is(status, nil, "Dump access denied returns error")
    test:diag(msg)
    test:is(type(msg), "string", "Dump access denied provides error message")
    rmpath(dir)
end

local function dump_after_dump(test)
    test:plan(2)
    local dir = fio.tempdir()
    box.schema.space.create('test')
    local status, msg = dump.dump(dir)
    test:is(not status, false, "First dump is successful")
    local status, msg = dump.dump(dir)
    test:is(not status, true, "Second dump fails")
    box.space.test:drop()
    rmpath(dir)
end

local function restore_no_such_path(test)
    test:plan(2)
    local dir = fio.tempdir()
    rmpath(dir)
    local status, msg = dump.restore(dir)
    test:is(type(status), "nil", "Restore of a non-existent dir returns error")
    test:is(type(msg), "string", "Restore of a non-existent dir provides error message")
end

local function dump_hash_index(test)
    test:plan(2)
    local ROWS = 2000
    local TXN_ROWS = 100
    box.schema.space.create('hash')
    box.space.hash:create_index('pk', {type='hash', parts = {1, 'str'}})
    for i = 0, ROWS/TXN_ROWS - 1 do
        box.begin()
        for j = 1, TXN_ROWS do
            box.space.hash:insert{tostring(i*TXN_ROWS + j), fiber.time()}
        end
        box.commit()
    end
    local dir = fio.tempdir()
    dump.dump(dir)
    box.space.hash:drop()
    dump.restore(dir)
    test:is(box.space.hash.index[0].type, 'HASH',
        "Restored space has HASH primary key")
    local rows = 0
    for i = 1, ROWS do
        local key = tostring(i)
        local i1 = box.space.hash:get{key}
        if i1 and i1[1] == key then
           rows = rows + 1
        end
    end
    test:is(rows, ROWS, "The number of rows is correct after restore")
    box.space.hash:drop()
    rmpath(dir)
end

local function dump_filter(test)
    test:plan(3)
    local space = box.schema.space.create('test')
    local space_id = space.id
    space:create_index('pk')
    space:insert{1, 'ignore'}
    space:insert{2, 'update'}
    space:insert{3, 'filter'}
    local dir = fio.tempdir()
    dump.dump(dir, {
        filter = function(space, tuple)
            if space.id ~= space_id then return tuple end
            if tuple[2] == 'ignore' then return tuple end
            if tuple[2] == 'filter' then return nil end
            if tuple[2] == 'update' then
                return tuple:update{{'=', 2, 'updated'}}
            end
        end,
    })
    space:drop()
    dump.restore(dir)
    space = box.space.test
    test:is(space:get(1)[2], 'ignore', "Dump filter can ignore a tuple")
    test:is(space:get(2)[2], 'updated', "Dump filter can update a tuples")
    test:is(space:get(3), nil, "Dump filter can filter out a tuple")
    space:drop()
    rmpath(dir)
end

local function restore_filter(test)
    test:plan(3)
    local space = box.schema.space.create('test')
    local space_id = space.id
    space:create_index('pk')
    space:insert{1, 'ignore'}
    space:insert{2, 'update'}
    space:insert{3, 'filter'}
    local dir = fio.tempdir()
    dump.dump(dir)
    space:drop()
    dump.restore(dir, {
        filter = function(space, tuple)
            if space.id ~= space_id then return tuple end
            if tuple[2] == 'ignore' then return tuple end
            if tuple[2] == 'filter' then return nil end
            if tuple[2] == 'update' then
                return tuple:update{{'=', 2, 'updated'}}
            end
        end,
    })
    space = box.space.test
    test:is(space:get(1)[2], 'ignore', "Restore filter can ignore a tuple")
    test:is(space:get(2)[2], 'updated', "Restore filter can update a tuple")
    test:is(space:get(3), nil, "Restore filter can filter out a tuple")
    space:drop()
    rmpath(dir)
end

test:plan(9)

test:test('Basics', basic)
test:test('Using the rock without calling box.cfg{}', box_is_configured)

box.cfg{log_level=6}

test:test('Dump and restore', dump_and_restore)
test:test('Dump into a non-writable directory', dump_access_denied)
test:test('Dump into a non-empty directory', dump_after_dump)
test:test('Restore of a non-existent path', restore_no_such_path)
test:test('Dump and restore of a space with HASH primary key', dump_hash_index)
test:test('Dump filter', dump_filter)
test:test('Restore filter', restore_filter)

os.exit(test:check() == true and 0 or -1)
