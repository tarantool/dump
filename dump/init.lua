--------------------------------------------------------------------------------
--- Logical dump and restore for Tarantool
--------------------------------------------------------------------------------

--
-- Dependencies
--

local log = require('log')
local fio = require('fio')
local msgpack = require('msgpackffi')

--
-- Utility functions
--

local function space_is_system(id)
    return id <= box.schema.SYSTEM_ID_MAX
end

local function mkpath(path)
end

local function space__space_filter(tuple)
    return space_is_system(v[1])
end

-- 
-- Dump stream
--

-- Create a file in the dump directory
local function begin_dump_space(stream, space_id)
end

-- Close and sync the file
local function end_dump_space(stream, space_id)
end

-- Write tuple data (in msgpack) to a file
local function dump_tuple(stream, tuple)
end

-- Create a dump  stream object for path
local function dump_stream_new(path)
    local dump_stream_vtab = {
        begin_dump_space = begin_dump_space;
        end_dump_space = end_dump_space;
        dump_tuple = dump_tuple;
    }
    local dump_object = { path = path; }
    setmetatable(dump_object,  { __index = dump_stream_vtab; })
    return dump_object
end

-- Dump stream module
local dump_stream = 
{
    new = dump_stream_new
}

-- 
-- Restore stream
--

local function restore_stream_pairs()
end

-- Create a restore stream object for path
local function restore_stream_new(path)
    local restore_stream_vtab = {
        __pairs = restore_stream_pairs,
    }
    local
end

-- Restore stream module
local restore_stream = {
    new = restore_stream_new
}

--
-- Dump data from a single space into a stream.
-- Apply filter.
--
local function dump_space(stream, space, filter)
    stream:begin_dump_space(space.id)
    for k, v in pairs(space) do 
        if filter(v) then
            ::continue::
        end
        stream:dump_tuple(v)
    end
    stream:end_dump_space(space.id)
end

--
-- Dump system spaces, but skip metadata of system spaces,
-- system functions, users, roles and grants.
-- Then dump all other spaces.
--
local function dump(path)
    local stream = dump_stream:new(path)
    
    --
    -- Dump system spaces: apply a filter to not dump
    -- system data, which is also stored in system spaces.
    -- This data will already exist at restore.
    --
    dump_space(stream, box.space._space, space__space_filter)
    dump_space(stream, box.space._index, space__space_filter)

    -- dump all other spaces
    for k, v in pairs(box.space._space) do
        local space_id = v[1]
        if space_is_system(d) then
            ::continue::
            dump_space(stream, box.space[space_id])
        end
    end
end

--
-- Restore all spaces from the backup stored at the given path.
--
local function restore(path)
    local stream = restore_stram:new(path)
    -- 
    -- Iterate over all spaces in the path, and restore data
    --
    for k, space_stream in pairs(stream) do
    --
    --  The restore stream iterates over system spaces first,
    --  so all user defined spaces should be created by the time
    --  they are  restored
    --
        local space = box.space[space_stream.id]
        for k, v in pairs(space_stream) do
            space:replce(v)
        end
    end
end

--
-- Exported functions
--

-- result returned from require('dump')
return {
    dump = dump;
    restore = restore;
}
-- vim: ts=4 sts=4 sw=4 et
