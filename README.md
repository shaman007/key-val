# key-val

## About

Simple server in C that sits at 8080 port, serves like in-memory key-value db. It has 5 commands:

* "write key value" writes or overwrites key with value, saving timestamp
* "search key" searches value by the key
* "dump" dupms what is in memory (at the moment does not work properly on 50 000+ key-values)
* "wipe" drops everyting
* "size" show amount of key-values pairs and current hash table reserved
* "delete key" deletes the key
* "quit" close the connection.

## Data model

Hash table, that stores key, value and creation timestamp