--------------------------------------------------------------------------------
--- Logical dump and restore for Tarantool
--------------------------------------------------------------------------------

--
-- Dependencies
--

local log = require('log') -- some other Tarantool module

--
-- Constants
--

--
-- Internal functions
--

local function dump(path)
end

local function restore(path)
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
