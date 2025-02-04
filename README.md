# key-val

## About

Simple server in C that sits at 8080 port, serves like in-memory key-value db. It has folowing commands:

* "write key value" writes or overwrites key with value, saving timestamp
* "add key value"  writes key with value, saving timestamp, but not overwrites it
* "update key value" overwrites key with value, saving timestamp, but not add it
* "search key" searches value by the key
* "dump index offset" dupms what is in table from index to offset, if there are buckets there. Just "dump" returns first INITIAL_CAPACITY indexes.
* "wipe" drops everyting
* "size" show amount of key-values pairs and current hash table reserved
* "delete key" deletes the key
* "quit" close the connection.

## Data model

Hash table, that stores key, it's hash, value and creation timestamp in a buckets linked to the table row.

## Known problems

* if client don't read socket but spams commands, servers segfaults
