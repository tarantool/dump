<a href="https://travis-ci.org/tarantool/dump"><img src="https://travis-ci.org/tarantool/dump.png?branch=master" align="right"></a><br/>

# dump <img src="https://github.com/tarantool/dump/blob/master/docs/truck.png?raw=true" align="right">

Logical backup and restore of a Tarantool instance.

## Why you need a logical backup

Tarantool provides a physical backup with
[box.backup](https://tarantool.org/en/doc/1.7/book/admin/backups.html#hot-backup-vinyl-memtx)
module. But you may still want a logical one.

Here's why:

* A logical backup contains tuples, not files. By making a logical backup, you
  ensure that the database server can read the contents of the database.
  A logical backup sees the database through the same lense as your application.
* If Tarantool binary file layout changes, you still can restore data from a
  logical backup.
* You could use a logical backup to export data into another database (although
  we recommend using Tarantool's [MySQL](http://github.com/tarantool/mysql) or
  [PostgreSQL](http://github.com/tarantool/pg) connectors for this).

## How to use

### Preparation

Ensure that the database is not being changed while dump or restore is in progress.

### Execution

```local status, error = require('dump').dump('/path/to/logical/backup')```

The path should not exist, or be an empty directory. It is created if it does
not exist. The command then dumps all space and index definitions, users, roles
and privileges, and space data. Each space is dumped into a file in the path
named `<space-id>.dump`.

```local status, error = require('dump').restore('/path/to/logical/backup')```

Please note that this module does not throw exceptions, and uses Lua conventions for
errors: check the return value explicitly to see if your dump or restore has succeeded.

This command restores a logical dump.

### Advanced usage

You can use a filter function as an additional argument to dump and restore.
A filter is a function that takes a space and a tuple and returns a tuple. 
This function is called for each dumped/restored tuple. It can be used to overwrite
what is written to the dump file or restored. If it returns nil the tuple is skipped.

For example, 'filter' option can be used to convert memtx spaces to vinyl as
shown below:
```
dump.restore('dump', {
    filter = function(space, tuple)
        if space.id == box.schema.SPACE_ID then
            return tuple:update{{'=', 4, 'vinyl'}}
        else
            return tuple
        end
    end
})
```

### Details

The backup utility creates a file for each dumped space, using space id for file name. If you want to restore only a single space, restore from a directory which contains its dump file and nothing else. The dump skips spaces with id < 512 (the system spaces), with the exception of tuples which contain metadata of user-defined spaces, to ensure smooth restore on an empty instance. If you want to restore data into an existing space, delete files with ids < 512 from the dump directory.
