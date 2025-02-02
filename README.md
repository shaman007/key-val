# key-val

## About

Simple server in C that sits at 8080 port, serves like in-memory key-value db. It has 5 commands:

* write key-value
* search value by the key
* dump what is in memory (at the moment does not work properly on 500 000+ key-values)
* wipe drops everyting
* size show amount of key-values pairs
* close the connection.

## Data model

Hash table, that stores key, value and creation timestamp