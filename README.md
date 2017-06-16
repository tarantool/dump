
# dump <img src="https://github.com/tarantool/dump/blob/master/docs/truck.png?raw=true" align="right"/> 

Logical backup and restore of a tarantool instance.

## Why you need a logical backup

Tarantool provides a physical backup with box.backup module. But you may still want a logical one.

Here's why:

* a logical backup contains tuples, not files. By making a logical backup, you ensure that the database server can read the contents of the database. A logical backup seems the database through the same lense as your application. 
* if Tarantool binary file layout changes, you still can restore from a logical backup
* you could use a logical backup to export data into another database (although it'd be better to use http://github.com/tarantool/mysql or http://github.com/tarantool/pg for this.

## How to use

### Preparation

Ensure the database is not being changed while dump or restore is in progress.

### Execution

```require('dump').dump('/path/to/logical/backup')```

dumps all space and index definitions, users, roles and privilege, and space datas. Each space is dumped into a subdirectory of the path provided in the first argument.

```require('dump').restore('/path/to/logical/backup')```

restores a logical dump.

