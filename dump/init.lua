--------------------------------------------------------------------------------
--- Logical dump and restore for Tarantool
--------------------------------------------------------------------------------

--
-- Dependencies
--

local log = require('log')
local fio = require('fio')
local json = require('json')
local msgpackffi = require('msgpackffi')
local errno = require('errno')
local fun = require('fun')
local buffer = require('buffer')
local ffi = require('ffi')

-- Constants

local BUFSIZ = 1024*1024

-- Utility functions {{{
--

-- Return true if a space is a system one so we need to skip dumping
-- it
local function space_is_system(space_id)
    return space_id <= box.schema.SYSTEM_ID_MAX
end

-- Create a directory at a given path if it doesn't exist yet.
-- If it does exist, check that it's empty.
local function mkpath(path, interim)
    local stat = fio.stat(path)
    if stat then
        if not stat:is_dir() then
            return nil, string.format("File %s exists and is not a directory",
                path)
        end
        --
        -- It's OK for interim dirs to contain files.
        --
        if interim then
            return true
        end
        local files = fio.glob(fio.pathjoin(path, "*"))
        if not files then
            return nil, string.format("Failed to read directory %s, errno %d (%s)",
                path, errno(), errno.strerror())
        end
        --
        -- The leaf directory must be empty.
        --
        if #files > 0 then
            return nil, string.format("Directory %s is not empty", path)
        end
        return true
    end
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
    -- mkdir expects an octal mask  :-/, pass in 0750
    if not fio.mkdir(path, 488) then
        return nil, string.format("Failed to create directory %s, errno %d (%s)",
            path, errno(), errno.strerror());
    end
    return true
end

local croak = type(log.verbose) == 'function' and log.verbose or log.info

local function box_is_configured()
    if type(box.cfg) == "function" then
        return nil, "Please start the database with box.cfg{} first"
    end
    return true
end

ffi.cdef[[
size_t
box_tuple_bsize(const box_tuple_t *tuple);

ssize_t
box_tuple_to_buf(const box_tuple_t *tuple, char *buf, size_t size);
]]

local builtin = ffi.C

local function tuple_to_buf(tuple, buf)
    local bsize = builtin.box_tuple_bsize(tuple)
    buf:reserve(bsize)
    builtin.box_tuple_to_buf(tuple, buf.wpos, bsize)
    buf.wpos = buf.wpos + bsize
end

local function tuple_extract_key(tuple, space)
    return fun.map(function(x) return tuple[x.fieldno] end,
        space.index[0].parts):totable()
end

-- }}} Utility functions

-- Dump stream {{{
--

-- Create a file in the dump directory
local function begin_dump_space(stream, space_id)
    croak("Started dumping space %s", box.space[space_id].name)
    local path = fio.pathjoin(stream.path, string.format("%d.dump", space_id))
    local fh = fio.open(path, {'O_APPEND', 'O_CREAT', 'O_WRONLY'}, {'S_IRUSR', 'S_IRGRP'})
    if not fh then
        return nil, string.format("Can't open file %s, errno %d (%s)",
            path, errno(), errno.strerror())
    end
    stream.files[space_id] = {
        path = path;
        fh = fh;
        rows = 0;
        buf = buffer.ibuf(BUFSIZ)
    }
    return true
end

-- Flush dump buffer
local function flush_dump_stream(stream, space_id)
    local buf = stream.files[space_id].buf
    local fh = stream.files[space_id].fh
    local res = fh:write(ffi.string(buf.buf, buf.wpos - buf.buf))
    if not res then
        return nil, string.format("Failed to write to file %s, errno %d (%s)",
            stream.files[space_id].path, errno(), errno.strerror())
    end
    buf:reset()
    return true
end

-- Close and sync a space dump file
-- If the file is empty, don't clutter the dump and silently delete it
-- instead.
-- Update dump stats.
local function end_dump_space(stream, space_id)
    local status, msg = flush_dump_stream(stream, space_id)
    if not status then
        return nil, msg
    end
    croak("Ended dumping space %s", box.space[space_id].name)
    local fh = stream.files[space_id].fh
    fh:fsync()
    if not fh:close() then
        return nil, string.format("Failed to close file %s, errno %d (%s)",
            stream.files[space_id].path, errno(), errno.strerror())
    end
    local rows = stream.files[space_id].rows
    if rows == 0 then
        croak("Space %s was empty", box.space[space_id].name)
        fio.unlink(stream.files[space_id].path)
    else
        croak("Space %s had %d rows", box.space[space_id].name, rows)
        stream.spaces = stream.spaces + 1
    end
    stream.rows = stream.rows + rows
    stream.files[space_id] = nil
    return true
end

-- Write tuple data (in msgpack) to a file
local function dump_tuple(stream, space_id, tuple)
    local buf = stream.files[space_id].buf
    tuple_to_buf(tuple, buf)
    if buf:size() > BUFSIZ/2 then
        local status, msg = flush_dump_stream(stream, space_id)
        if not status then
            return nil, msg
        end
    end
    stream.files[space_id].rows = stream.files[space_id].rows + 1
    return true
end

-- Create a dump  stream object for path
-- Creates the path if it doesn't exist.
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
    local dump_object = {
        path = path;
        files = {};
        spaces = 0;
        rows = 0;
    }
    setmetatable(dump_object,  { __index = dump_stream_vtab; })
    return dump_object
end

-- Dump stream module
local dump_stream =
{
    new = dump_stream_new
}

-- }}} Dump stream

-- Restore stream {{{
--

-- Create a restore stream object for a path
-- Scans the path, finds all dump files and prepares
-- them for restore.
local function restore_stream_new(restore_stream, path)
    croak("Reading contents of %s", path)
    local stat = fio.stat(path)
    if not stat then
        return nil, string.format("Path %s does not exist", path)
    end
    if not stat:is_dir() then
        return nil, string.format("Path %s is not a directory", path)
    end
    local files = fio.glob(fio.pathjoin(path, "*.dump"))
    if not files then
        return nil, string.format("Failed to read %s, errno %d (%s)", path,
            errno(), errno.strerror())
    end
    -- Convert "280.dump" -> 280
    local function to_id(file)
        return tonumber(string.match(fio.basename(file), "^%d+"))
    end
    files = fun.map(to_id, files):totable()
    -- Ensure restore processes spaces in the same order
    -- as checkpoint recovery
    table.sort(files)
    croak("Found %d files", #files)
    local restore_object = { path = path; files = files; spaces = 0; rows = 0; }
    return restore_object
end

local function space_stream_read(stream)
    if stream.pos == string.len(stream.data) + 1 then
        return nil
    end
    local tuple, pos = msgpackffi.decode_unchecked(stream.data, stream.pos)
    if tuple == nil then
        return nil, string.format("Failed to decode tuple: trailing bytes in the input stream %s",
            stream.path)
    end
    stream.pos = pos
    return tuple
end

-- Restore data in a single space
local function space_stream_restore(space_stream)
    croak("Started restoring space %s", space_stream.space.name)
    local tuple, msg = space_stream_read(space_stream)
    while tuple do
        space_stream.space:replace(tuple)
        space_stream.rows = space_stream.rows + 1
        tuple, msg = space_stream_read(space_stream)
    end
    if msg then
        return nil, msg
    end
    croak("Loaded %d rows in space %s", space_stream.rows,
        space_stream.space.name)
    return true
end

-- Create a new stream to restore a single space
local function space_stream_new(dir, space_id)
    local space = box.space[space_id]
    if space == nil then
        return nil, string.format("The dump directory is missing metadata for space %d",
            space_id)
    end
    local path = fio.pathjoin(dir, string.format("%d.dump", space_id))
    local stat = fio.stat(path)
    if not stat then
        return nil, string.format("Failed to open file %s, errno %d, (%s)",
            path, errno(), errno.strerror());
    end
    local fh = fio.open(path, {'O_RDONLY'})
    if fh == nil then
        return nil, string.format("Can't open file '%s', errno %d (%s)",
            path, errno(), errno.strerror())
    end
    local data = fh:read(stat.size)
    fh:close()
    if not data or string.len(data) < stat.size then
        return nil, string.format("Failed to read file %s, errno %d, (%s",
            path, errno(), errno.strerror())
    end
    local space_stream = {
        space = space;
        pos = 1;
        data = data;
        rows = 0;
    }
    local space_stream_vtab = {
        restore = space_stream_restore;
    }
    setmetatable(space_stream, { __index = space_stream_vtab })
    return space_stream
end

-- Restore stream module
local restore_stream = {
    new = restore_stream_new
}

-- }}} Restore stream

-- {{{ Database-wide dump and restore

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

    -- iterate in batches using GT iterator
    local options = {iterator = 'GT', limit = 200}
    local last_key
    local batch
    if not space.index[0] then
        batch = {}
    else
        batch = space:select({}, options)
    end
    while #batch > 0 do
        last_key = tuple_extract_key(batch[#batch], space)
        for _, v in ipairs(batch) do
            if filter and filter(v) then
                goto continue
            end
            local status, msg = stream:dump_tuple(space_id, v)
            if not status then
                stream:end_dump_space(space_id)
                return nil, msg
            end
            ::continue::
        end
        batch = space:select(last_key, options)
    end

    -- end of dump
    local status, msg = stream:end_dump_space(space_id)
    if not status then
        return nil, msg
    end
    return true
end

-- Filter out all system spaces
local function space__space_filter(tuple)
    return space_is_system(tuple[1])
end

--
-- Dump system spaces, but skip metadata of system spaces,
-- system functions, users, roles and grants.
-- Then dump all other spaces.
--
local function dump(path)
    local status, msg = box_is_configured()
    if not status then
        return nil, msg
    end
    local stream, msg = dump_stream:new(path)
    if not stream then
        return nil, msg
    end
    --
    -- Dump system spaces: apply a filter to not dump
    -- system data, which is also stored in system spaces.
    -- This data will already exist at restore.
    --
    local status, msg = dump_space(stream, box.space._space, space__space_filter)
    if not status then
        return nil, msg
    end
    local status, msg = dump_space(stream, box.space._index, space__space_filter)
    if not status then
        return nil, msg
    end

    -- dump all other spaces
    for k, v in box.space._space:pairs() do
        local space_id = v[1]
        if space_is_system(space_id) then
            goto continue
        end
        local status, msg = dump_space(stream, box.space[space_id])
        if not status then
            return nil, msg
        end
        ::continue::
    end
    return { spaces = stream.spaces; rows = stream.rows; }
end

--
-- Restore all spaces from the backup stored at the given path.
--
local function restore(path)
    local status, msg = box_is_configured()
    if not status then
        return nil, msg
    end
    local stream, msg = restore_stream:new(path)
    if not stream then
        return nil, msg
    end
    --
    -- Iterate over all spaces in the path, and restore data
    for k, space_id in pairs(stream.files) do
        --
        --  The restore stream iterates over system spaces first,
        --  so all user defined spaces should be created by the time
        --  they are  restored
        --
        local space_stream, msg = space_stream_new(stream.path, space_id)
        if not space_stream then
            return nil, msg
        end
        local status, msg = space_stream:restore()
        if not status then
            return nil, msg
        end
        stream.spaces = stream.spaces + 1
        stream.rows = stream.rows + space_stream.rows
    end
    return { spaces = stream.spaces; rows = stream.rows }
end

-- }}} Database-wide dump and restore

-- Exported functions
--

-- result returned from require('dump')
return {
    dump = dump;
    restore = restore;
}
-- vim: ts=4 sts=4 sw=4 et
