-- name of the package to be published
package = 'dump'

-- version of the package; it's mandatory, but we don't use it in Tarantool;
-- instead, provide below a specific branch in the package's repository at
-- GitHub and set version to some stub value, e.g. 'scm-1'
version = 'scm-1'

-- url and branch of the package's repository at GitHub
source  = {
    url    = 'git+https://github.com/tarantool/dump.git';
    branch = 'master';
}

-- general information about the package;
-- for a Tarantool package, we require three fields (summary, homepage, license)
-- and more package information is always welcome
description = {
    summary  = "Logical dump and restore for Tarantool";
    detailed = [[
    Logical backups are the only true backups.
    ]];
    homepage = 'https://github.com/tarantool/dump.git';
    maintainer = "Konstantin Osipov <kostja@tarantool.org>";
    license  = 'BSD2';
}

-- Lua version and other packages on which this one depends;
-- Tarantool currently supports strictly Lua 5.1
dependencies = {
    'lua == 5.1';
}

-- build options and paths for the package;
-- this package distributes modules in pure Lua, so the build type = 'builtin';
-- also, specify here paths to all Lua modules within the package
-- (this package contains just one Lua module named 'dump')
build = {
    type = 'builtin';
    modules = {
        ['dump'] = 'dump/init.lua';
    }
}
-- vim: syntax=lua ts=4 sts=4 sw=4 et
