# key-val
[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Fshaman007%2Fkey-val.svg?type=shield)](https://app.fossa.com/projects/git%2Bgithub.com%2Fshaman007%2Fkey-val?ref=badge_shield)


## About

Simple server in C that sits at 8080 port, serves like in-memory key-value db. It has folowing commands:

* "write key value ttl" writes or overwrites key with value, saving timestamp, ttl optional
* "add key value ttl"  writes key with value, saving timestamp, but not overwrites it, ttl optional
* "update key value ttl" overwrites key with value, saving timestamp, but not add it, ttl optional
* "search key" searches value by the key
* "dump index offset" dupms what is in table from index to offset, if there are buckets there. Just "dump" returns first INITIAL_CAPACITY indexes.
* "wipe" drops everyting
* "size" show amount of key-values pairs and current hash table reserved
* "delete key" deletes the key
* "quit" close the connection.

## Data model

Hash table, that stores key, it's hash, value and creation timestamp in a buckets linked to the table row.

## ToDo

* Configfile, cli keys
* Persistence (yet need to choose strategy, Periodic Snapshots + WAL maybe?)
* Simple master-master replication with Last-Write-Wins (LWW) strategy
* Dokerise
* Test automations
* Consistent logging.

## Known problems

* if client don't read socket but spams commands, servers segfaults. Yeah, that's the big one problem.


## License
[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Fshaman007%2Fkey-val.svg?type=large)](https://app.fossa.com/projects/git%2Bgithub.com%2Fshaman007%2Fkey-val?ref=badge_large)