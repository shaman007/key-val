#ifndef KEY_VAL_STORE_H
#define KEY_VAL_STORE_H

#include <stddef.h>
#include <time.h>

#define STORE_DEFAULT_MAX_TTL 31536000

typedef struct Store Store;

typedef enum {
    STORE_WRITE = 0,
    STORE_UPDATE = 1,
    STORE_ADD = 2
} StoreWriteMode;

typedef enum {
    STORE_OK = 0,
    STORE_NOT_FOUND = 1,
    STORE_EXISTS = 2,
    STORE_INVALID = 3,
    STORE_NOMEM = 4
} StoreResult;

typedef struct {
    char *value;
    time_t created_at;
} StoreEntry;

Store *store_create(size_t capacity, time_t default_ttl);
void store_destroy(Store *store);
StoreResult store_put(Store *store, const char *key, const char *value, time_t ttl, StoreWriteMode mode);
StoreResult store_get(Store *store, const char *key, StoreEntry *entry);
StoreResult store_delete(Store *store, const char *key);
void store_collect_expired(Store *store);
void store_clear(Store *store);
size_t store_count(Store *store);
size_t store_capacity(Store *store);
char *store_dump(Store *store, size_t index, size_t offset);
void store_entry_free(StoreEntry *entry);

#endif
