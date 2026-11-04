#include "store.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <limits.h>

#define LOAD_FACTOR_THRESHOLD 0.75

typedef struct StoreNode {
    char *key;
    char *value;
    size_t value_capacity;
    time_t created_at;
    time_t ttl;
    unsigned long hash;
    struct StoreNode *next;
} StoreNode;

typedef struct Shard {
    StoreNode **buckets;
    size_t capacity;
    size_t count;
    time_t default_ttl;
    pthread_mutex_t mutex;
} Shard;

static unsigned long hash_key(const char *str) {
    uint64_t hash = UINT64_C(14695981039346656037);
    while (*str) {
        hash ^= (unsigned char)*str++;
        hash *= UINT64_C(1099511628211);
    }
    hash ^= hash >> 33;
    hash *= UINT64_C(0xff51afd7ed558ccd);
    hash ^= hash >> 33;
    return (unsigned long)hash;
}

static int is_expired(StoreNode *node, time_t now) {
    return node->ttl > 0 && now - node->created_at >= node->ttl;
}

static void free_node(StoreNode *node) {
    if (!node) {
        return;
    }

    free(node->key);
    free(node->value);
    free(node);
}

static void free_list(StoreNode *node) {
    while (node) {
        StoreNode *next = node->next;
        free_node(node);
        node = next;
    }
}

static StoreNode *create_node(const char *key, const char *value, unsigned long hash, time_t ttl) {
    StoreNode *node = calloc(1, sizeof(*node));
    if (!node) {
        return NULL;
    }

    node->key = strdup(key);
    node->value = strdup(value);
    if (!node->key || !node->value) {
        free_node(node);
        return NULL;
    }

    node->value_capacity = strlen(value) + 1;
    node->hash = hash;
    node->ttl = ttl;
    node->created_at = time(NULL);
    return node;
}

static StoreResult resize_locked(Shard *store) {
    if (store->capacity > SIZE_MAX / 3 / sizeof(StoreNode *)) return STORE_NOMEM;
    size_t new_capacity = store->capacity * 3;
    StoreNode **new_buckets = calloc(new_capacity, sizeof(*new_buckets));
    if (!new_buckets) {
        return STORE_NOMEM;
    }

    for (size_t i = 0; i < store->capacity; i++) {
        StoreNode *node = store->buckets[i];
        while (node) {
            StoreNode *next = node->next;
            size_t new_index = (node->hash >> 6) % new_capacity;
            node->next = new_buckets[new_index];
            new_buckets[new_index] = node;
            node = next;
        }
    }

    free(store->buckets);
    store->buckets = new_buckets;
    store->capacity = new_capacity;
    return STORE_OK;
}

static void collect_expired_locked(Shard *store) {
    time_t now = time(NULL);

    for (size_t i = 0; i < store->capacity; i++) {
        StoreNode *node = store->buckets[i];
        StoreNode *prev = NULL;

        while (node) {
            StoreNode *next = node->next;
            if (is_expired(node, now)) {
                if (prev) {
                    prev->next = next;
                } else {
                    store->buckets[i] = next;
                }
                free_node(node);
                store->count--;
            } else {
                prev = node;
            }
            node = next;
        }
    }
}

static Shard *shard_create(size_t capacity, time_t default_ttl) {
    if (capacity == 0) {
        return NULL;
    }

    Shard *store = calloc(1, sizeof(*store));
    if (!store) {
        return NULL;
    }

    store->buckets = calloc(capacity, sizeof(*store->buckets));
    if (!store->buckets) {
        free(store);
        return NULL;
    }

    store->capacity = capacity;
    store->default_ttl = default_ttl > 0 ? default_ttl : STORE_DEFAULT_MAX_TTL;
    if (pthread_mutex_init(&store->mutex, NULL)) {
        free(store->buckets);
        free(store);
        return NULL;
    }
    return store;
}

static void shard_destroy(Shard *store) {
    if (!store) {
        return;
    }

    pthread_mutex_lock(&store->mutex);
    for (size_t i = 0; i < store->capacity; i++) {
        free_list(store->buckets[i]);
    }
    free(store->buckets);
    store->buckets = NULL;
    pthread_mutex_unlock(&store->mutex);
    pthread_mutex_destroy(&store->mutex);
    free(store);
}

static StoreResult shard_put(Shard *store, const char *key, const char *value, time_t ttl, StoreWriteMode mode, unsigned long hash) {
    if (!store || !key || !value || key[0] == '\0') {
        return STORE_INVALID;
    }

    if (ttl <= 0) {
        ttl = store->default_ttl;
    }

    pthread_mutex_lock(&store->mutex);

    size_t index = (hash >> 6) % store->capacity;
    StoreNode *head = store->buckets[index];
    StoreNode *prev = NULL;
    time_t now = time(NULL);

    for (StoreNode *node = head; node; node = node->next) {
        if (node->hash == hash && strcmp(node->key, key) == 0) {
            if (is_expired(node, now)) {
                if (prev) {
                    prev->next = node->next;
                } else {
                    store->buckets[index] = node->next;
                    head = node->next;
                }
                free_node(node);
                store->count--;
                break;
            }
            if (mode == STORE_ADD) {
                pthread_mutex_unlock(&store->mutex);
                return STORE_EXISTS;
            }

            size_t needed = strlen(value) + 1;
            if (needed > node->value_capacity) {
                char *new_value = realloc(node->value, needed);
                if (!new_value) {
                    pthread_mutex_unlock(&store->mutex);
                    return STORE_NOMEM;
                }
                node->value = new_value;
                node->value_capacity = needed;
            }
            memcpy(node->value, value, needed);
            node->ttl = ttl;
            node->created_at = now;
            pthread_mutex_unlock(&store->mutex);
            return STORE_OK;
        }
        prev = node;
    }

    if (mode == STORE_UPDATE) {
        pthread_mutex_unlock(&store->mutex);
        return STORE_NOT_FOUND;
    }

    if ((double)store->count / store->capacity > LOAD_FACTOR_THRESHOLD) {
        StoreResult resized = resize_locked(store);
        if (resized != STORE_OK) {
            pthread_mutex_unlock(&store->mutex);
            return resized;
        }
        index = (hash >> 6) % store->capacity;
        head = store->buckets[index];
    }

    StoreNode *node = create_node(key, value, hash, ttl);
    if (!node) {
        pthread_mutex_unlock(&store->mutex);
        return STORE_NOMEM;
    }

    node->next = head;
    store->buckets[index] = node;
    store->count++;
    pthread_mutex_unlock(&store->mutex);
    return STORE_OK;
}

static StoreResult shard_get(Shard *store, const char *key, StoreEntry *entry, unsigned long hash) {
    if (!store || !key || !entry) {
        return STORE_INVALID;
    }

    entry->value = NULL;
    entry->created_at = 0;

    pthread_mutex_lock(&store->mutex);
    size_t index = (hash >> 6) % store->capacity;
    StoreNode *node = store->buckets[index];
    StoreNode *prev = NULL;
    time_t now = time(NULL);

    while (node) {
        StoreNode *next = node->next;
        if (node->hash == hash && strcmp(node->key, key) == 0) {
            if (is_expired(node, now)) {
                if (prev) {
                    prev->next = next;
                } else {
                    store->buckets[index] = next;
                }
                free_node(node);
                store->count--;
                pthread_mutex_unlock(&store->mutex);
                return STORE_NOT_FOUND;
            }

            entry->value = strdup(node->value);
            if (!entry->value) {
                pthread_mutex_unlock(&store->mutex);
                return STORE_NOMEM;
            }
            entry->created_at = node->created_at;
            pthread_mutex_unlock(&store->mutex);
            return STORE_OK;
        }
        prev = node;
        node = next;
    }

    pthread_mutex_unlock(&store->mutex);
    return STORE_NOT_FOUND;
}

static StoreResult shard_delete(Shard *store, const char *key, unsigned long hash) {
    if (!store || !key) {
        return STORE_INVALID;
    }

    pthread_mutex_lock(&store->mutex);
    size_t index = (hash >> 6) % store->capacity;
    StoreNode *node = store->buckets[index];
    StoreNode *prev = NULL;
    time_t now = time(NULL);

    while (node) {
        StoreNode *next = node->next;
        if (node->hash == hash && strcmp(node->key, key) == 0) {
            if (is_expired(node, now)) {
                if (prev) {
                    prev->next = next;
                } else {
                    store->buckets[index] = next;
                }
                free_node(node);
                store->count--;
                pthread_mutex_unlock(&store->mutex);
                return STORE_NOT_FOUND;
            }

            if (prev) {
                prev->next = next;
            } else {
                store->buckets[index] = next;
            }
            free_node(node);
            store->count--;
            pthread_mutex_unlock(&store->mutex);
            return STORE_OK;
        }
        prev = node;
        node = next;
    }

    pthread_mutex_unlock(&store->mutex);
    return STORE_NOT_FOUND;
}

static char *shard_dump_locked(Shard *store, size_t index, size_t offset, size_t base) {
    if (!store) {
        return NULL;
    }

    collect_expired_locked(store);

    if (index >= store->capacity || offset > store->capacity - index) {
        return NULL;
    }

    size_t used = 0;
    size_t capacity = 1024;
    char *dump = calloc(1, capacity);
    if (!dump) {
        return NULL;
    }

    int increment = 0;
    for (size_t i = index; i < index + offset; i++) {
        for (StoreNode *node = store->buckets[i]; node; node = node->next) {
            int needed = snprintf(NULL, 0, "%d: %s -- %s;\n bucket: %zu; timestamp: %ld; index: %zu\n\n",
                                  increment, node->key, node->value, base + i, (long)node->created_at,
                                  base + (node->hash >> 6) % store->capacity);
            if (needed < 0) {
                free(dump);
                return NULL;
            }

            if (used + (size_t)needed + 1 > capacity) {
                while (used + (size_t)needed + 1 > capacity) {
                    capacity *= 2;
                }
                char *new_dump = realloc(dump, capacity);
                if (!new_dump) {
                    free(dump);
                    return NULL;
                }
                dump = new_dump;
            }

            snprintf(dump + used, capacity - used,
                     "%d: %s -- %s;\n bucket: %zu; timestamp: %ld; index: %zu\n\n",
                     increment++, node->key, node->value, base + i, (long)node->created_at,
                     base + (node->hash >> 6) % store->capacity);
            used += (size_t)needed;
        }
    }

    return dump;
}

void store_entry_free(StoreEntry *entry) {
    if (!entry) {
        return;
    }

    free(entry->value);
    entry->value = NULL;
    entry->created_at = 0;
}

/* Independent tables remove the single global lock from GET/SET/DELETE.
 * Whole-store operations lock in ascending order, unlock in reverse order. */
#define STORE_SHARDS 64
struct Store { Shard *shards[STORE_SHARDS]; };

Store *store_create(size_t capacity, time_t default_ttl) {
    if (!capacity || capacity > SIZE_MAX - (STORE_SHARDS - 1)) return NULL;
    Store *store = calloc(1, sizeof(*store));
    if (!store) return NULL;
    size_t per_shard = (capacity + STORE_SHARDS - 1) / STORE_SHARDS;
    for (size_t i = 0; i < STORE_SHARDS; i++) {
        store->shards[i] = shard_create(per_shard, default_ttl);
        if (!store->shards[i]) {
            for (size_t j = 0; j < i; j++) shard_destroy(store->shards[j]);
            free(store);
            return NULL;
        }
    }
    return store;
}
void store_destroy(Store *store) {
    if (!store) return;
    for (size_t i = 0; i < STORE_SHARDS; i++) shard_destroy(store->shards[i]);
    free(store);
}
static Shard *key_shard(Store *store, unsigned long hash) {
    return store->shards[hash % STORE_SHARDS];
}
StoreResult store_put(Store *store, const char *key, const char *value, time_t ttl, StoreWriteMode mode) {
    if (!store || !key) return STORE_INVALID;
    unsigned long hash = hash_key(key);
    return shard_put(key_shard(store, hash), key, value, ttl, mode, hash);
}
StoreResult store_get(Store *store, const char *key, StoreEntry *entry) {
    if (!store || !key) return STORE_INVALID;
    unsigned long hash = hash_key(key);
    return shard_get(key_shard(store, hash), key, entry, hash);
}
StoreResult store_delete(Store *store, const char *key) {
    if (!store || !key) return STORE_INVALID;
    unsigned long hash = hash_key(key);
    return shard_delete(key_shard(store, hash), key, hash);
}
static void lock_all(Store *store) {
    for (size_t i = 0; i < STORE_SHARDS; i++) pthread_mutex_lock(&store->shards[i]->mutex);
}
static void unlock_all(Store *store) {
    for (size_t i = STORE_SHARDS; i-- > 0;) pthread_mutex_unlock(&store->shards[i]->mutex);
}
void store_clear(Store *store) {
    if (!store) return;
    lock_all(store);
    for (size_t i = 0; i < STORE_SHARDS; i++) {
        Shard *s = store->shards[i];
        for (size_t j = 0; j < s->capacity; j++) {
            free_list(s->buckets[j]);
            s->buckets[j] = NULL;
        }
        s->count = 0;
    }
    unlock_all(store);
}
void store_collect_expired(Store *store) {
    if (!store) return;
    lock_all(store);
    for (size_t i = 0; i < STORE_SHARDS; i++) collect_expired_locked(store->shards[i]);
    unlock_all(store);
}
size_t store_count(Store *store) {
    if (!store) return 0;
    size_t count = 0;
    lock_all(store);
    for (size_t i = 0; i < STORE_SHARDS; i++) {
        collect_expired_locked(store->shards[i]);
        count += store->shards[i]->count;
    }
    unlock_all(store);
    return count;
}
size_t store_capacity(Store *store) {
    if (!store) return 0;
    size_t capacity = 0;
    lock_all(store);
    for (size_t i = 0; i < STORE_SHARDS; i++) capacity += store->shards[i]->capacity;
    unlock_all(store);
    return capacity;
}
char *store_dump(Store *store, size_t index, size_t offset) {
    if (!store) return NULL;
    /* Debug dump uses a coherent snapshot across all shards. */
    lock_all(store);
    size_t total = 0;
    for (size_t i = 0; i < STORE_SHARDS; i++) total += store->shards[i]->capacity;
    if (index >= total || offset > total - index) { unlock_all(store); return NULL; }
    char *result = calloc(1, 1);
    size_t used = 0, base = 0;
    for (size_t i = 0; result && i < STORE_SHARDS; i++) {
        Shard *shard = store->shards[i];
        size_t end = base + shard->capacity;
        if (index < end && index + offset > base) {
            size_t start = index > base ? index - base : 0;
            size_t stop = index + offset < end ? index + offset - base : shard->capacity;
            /* shard_dump normally locks itself; already hold its lock here. */
            char *part = shard_dump_locked(shard, start, stop - start, base);
            if (!part) { free(result); result = NULL; break; }
            size_t len = strlen(part);
            char *grown = realloc(result, used + len + 1);
            if (!grown) { free(part); free(result); result = NULL; break; }
            result = grown;
            memcpy(result + used, part, len + 1);
            used += len;
            free(part);
        }
        base = end;
    }
    unlock_all(store);
    return result;
}
