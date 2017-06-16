--------------------------------------------------------------------------------
--- Logical dump and restore for Tarantool
--------------------------------------------------------------------------------

--
-- Dependencies
--

local log = require('log')
local fio = require('fio')
local json = require('json')
local msgpack = require('msgpackffi')
local errno = require('errno')

--
-- Utility functions
--

local function space_is_system(space_id)
    return space_id <= box.schema.SYSTEM_ID_MAX
end

local function space__space_filter(tuple)
    return space_is_system(tuple[1])
end

local function mkpath(path, interim)
    local dir = fio.dirname(path)
    if not dir then
        return nil, string.format("Incorrect path: %s", path)
    end
    local stat = fio.stat(dir)
    if not stat then
        local status, msg = mkpath(dir, true)
        if not status then
            return nil, msg
        end
    end
    --
    -- The directory must be empty, if it exists
    --
    if not interim then
        if not stat:is_dir() then
            return nil, string.format("File %s exists and is not a directory",
                path)
        end
        local files = fio.glob(path)
        if not files then
            return nil, string.format("Failed to read directory %s, errno %d (%s)",
                path, errno(), errno.strerror())
        end
        if #files > 0 then
            return nil, string.format("Directory %s is not empty", path)
        end
    end
    if not fio.mkdir(path) then
        return nil, string.format("Failed to create directory %s, errno %d (%s)",
            path, errno(), errno.strerror());
    end
    return true
end

--
-- Dump stream
--

-- Create a file in the dump directory
local function begin_dump_space(stream, space_id)
    local path = fio.pathjoin(stream.path, string.format("%d.dump", space_id))
    local fh = fio.open(path, {'O_APPEND', 'O_CREAT'}, {"S_IRUSR", "S_IRGRP"})
    if not fh then
        return nil, string.format("Can't open file %s, errno %d (%s)",
            path, errno(), errno.strerror())
    end
    stream.files[space_id] = { path = path; fh = fh; rows = 0; }
    if not space_is_system(space_id) then
        stream.spaces = stream.spaces + 1
    end
    return true
end

-- Close and sync the file
local function end_dump_space(stream, space_id)
    local fh = stream.files[space_id].fh
    fh:fsync()
    if not fh:close() then
        return nil, string.format("Failed to close file %s, errno %d (%s)",
            stream.files[space_id].path, errno(), errno.strerror())
    end
    if stream.files[space_id].rows == 0 then
        fio.unlink(stream.files[space_id].path)
    end
    stream.rows = stream.rows + stream.files[space_id].rows
    stream.files[space_id] = nil
    return true
end

-- Write tuple data (in msgpack) to a file
local function dump_tuple(stream, space_id, tuple)
    local fh = stream.files[space_id].fh
    if fh:write(msgpack.encode(tuple)) then
        return nil, string.format("Failed to write to file %s, errno %d (%s)",
            stream.files[space_id].path, errno(), errno.strerror())
    end
    stream.files[space_id].rows = stream.files[space_id].rows + 1
    return true
end

-- Create a dump  stream object for path
local function dump_stream_new(dump_stream, path)
    local dump_stream_vtab = {
        begin_dump_space = begin_dump_space;
        end_dump_space = end_dump_space;
        dump_tuple = dump_tuple;
    }
    local status, msg = mkpath(path)
    if not status then
        return nil, msg
    end
    local dump_object = { path = path; files = {}; spaces = 0, rows = 0 }
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
    local space_id = space.id
    local status, msg = stream:begin_dump_space(space_id)
    if not status then
        return nil, msg
    end
    for k, v in space:pairs() do
        if filter(v) then
            ::continue::
        end
        status, msg = stream:dump_tuple(space_id, v)
        if not status then
            stream:end_dump_space()
            return nil, msg
        end
    end
    status, msg = stream:end_dump_space(space_id)
    if not status then
        return nil, msg
    end
    return true
end

--
-- Dump system spaces, but skip metadata of system spaces,
-- system functions, users, roles and grants.
-- Then dump all other spaces.
--
local function dump(path)
    local stream, msg = dump_stream:new(path)
    if not stream then
        return nil, msg
    end
    local status
    --
    -- Dump system spaces: apply a filter to not dump
    -- system data, which is also stored in system spaces.
    -- This data will already exist at restore.
    --
    status, msg = dump_space(stream, box.space._space, space__space_filter)
    if not status then
        return nil, msg
    end
--    dump_space(stream, box.space._index, space__space_filter)

    -- dump all other spaces
--    for k, v in pairs(box.space._space) do
--        local space_id = v[1]
--        if space_is_system(space_id) then
--            ::continue::
--            dump_space(stream, box.space[space_id])
--        end
--    end
    return { spaces = stream.spaces; rows = stream.rows; }
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
