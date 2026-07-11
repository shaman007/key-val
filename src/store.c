#include "store.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define LOAD_FACTOR_THRESHOLD 0.75

typedef struct StoreNode {
    char *key;
    char *value;
    time_t created_at;
    time_t ttl;
    unsigned long hash;
    struct StoreNode *next;
} StoreNode;

struct Store {
    StoreNode **buckets;
    size_t capacity;
    size_t count;
    time_t default_ttl;
    pthread_mutex_t mutex;
};

static unsigned long hash_key(const char *str) {
    unsigned long hash = 5381;
    int c;

    while ((c = *str++)) {
        hash = ((hash << 5) + hash) + c;
    }

    return hash;
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

    node->hash = hash;
    node->ttl = ttl;
    node->created_at = time(NULL);
    return node;
}

static StoreResult resize_locked(Store *store) {
    size_t new_capacity = store->capacity * 3;
    StoreNode **new_buckets = calloc(new_capacity, sizeof(*new_buckets));
    if (!new_buckets) {
        return STORE_NOMEM;
    }

    for (size_t i = 0; i < store->capacity; i++) {
        StoreNode *node = store->buckets[i];
        while (node) {
            StoreNode *next = node->next;
            size_t new_index = node->hash % new_capacity;
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

static void collect_expired_locked(Store *store) {
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

Store *store_create(size_t capacity, time_t default_ttl) {
    if (capacity == 0) {
        return NULL;
    }

    Store *store = calloc(1, sizeof(*store));
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
    pthread_mutex_init(&store->mutex, NULL);
    return store;
}

void store_destroy(Store *store) {
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

StoreResult store_put(Store *store, const char *key, const char *value, time_t ttl, StoreWriteMode mode) {
    if (!store || !key || !value || key[0] == '\0') {
        return STORE_INVALID;
    }

    if (ttl <= 0) {
        ttl = store->default_ttl;
    }

    pthread_mutex_lock(&store->mutex);
    collect_expired_locked(store);

    if ((double)store->count / store->capacity > LOAD_FACTOR_THRESHOLD) {
        StoreResult resized = resize_locked(store);
        if (resized != STORE_OK) {
            pthread_mutex_unlock(&store->mutex);
            return resized;
        }
    }

    unsigned long hash = hash_key(key);
    size_t index = hash % store->capacity;
    StoreNode *head = store->buckets[index];
    time_t now = time(NULL);

    for (StoreNode *node = head; node; node = node->next) {
        if (node->hash == hash && strcmp(node->key, key) == 0) {
            if (mode == STORE_ADD) {
                pthread_mutex_unlock(&store->mutex);
                return STORE_EXISTS;
            }

            char *new_value = strdup(value);
            if (!new_value) {
                pthread_mutex_unlock(&store->mutex);
                return STORE_NOMEM;
            }
            free(node->value);
            node->value = new_value;
            node->ttl = ttl;
            node->created_at = now;
            pthread_mutex_unlock(&store->mutex);
            return STORE_OK;
        }
    }

    if (mode == STORE_UPDATE) {
        pthread_mutex_unlock(&store->mutex);
        return STORE_NOT_FOUND;
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

StoreResult store_get(Store *store, const char *key, StoreEntry *entry) {
    if (!store || !key || !entry) {
        return STORE_INVALID;
    }

    entry->value = NULL;
    entry->created_at = 0;

    pthread_mutex_lock(&store->mutex);
    unsigned long hash = hash_key(key);
    size_t index = hash % store->capacity;
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

StoreResult store_delete(Store *store, const char *key) {
    if (!store || !key) {
        return STORE_INVALID;
    }

    pthread_mutex_lock(&store->mutex);
    unsigned long hash = hash_key(key);
    size_t index = hash % store->capacity;
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

void store_collect_expired(Store *store) {
    if (!store) {
        return;
    }

    pthread_mutex_lock(&store->mutex);
    collect_expired_locked(store);
    pthread_mutex_unlock(&store->mutex);
}

void store_clear(Store *store) {
    if (!store) {
        return;
    }

    pthread_mutex_lock(&store->mutex);
    for (size_t i = 0; i < store->capacity; i++) {
        free_list(store->buckets[i]);
        store->buckets[i] = NULL;
    }
    store->count = 0;
    pthread_mutex_unlock(&store->mutex);
}

size_t store_count(Store *store) {
    if (!store) {
        return 0;
    }

    pthread_mutex_lock(&store->mutex);
    collect_expired_locked(store);
    size_t count = store->count;
    pthread_mutex_unlock(&store->mutex);
    return count;
}

size_t store_capacity(Store *store) {
    if (!store) {
        return 0;
    }

    pthread_mutex_lock(&store->mutex);
    size_t capacity = store->capacity;
    pthread_mutex_unlock(&store->mutex);
    return capacity;
}

char *store_dump(Store *store, size_t index, size_t offset) {
    if (!store) {
        return NULL;
    }

    pthread_mutex_lock(&store->mutex);
    collect_expired_locked(store);

    if (index >= store->capacity || offset > store->capacity - index) {
        pthread_mutex_unlock(&store->mutex);
        return NULL;
    }

    size_t used = 0;
    size_t capacity = 1024;
    char *dump = calloc(1, capacity);
    if (!dump) {
        pthread_mutex_unlock(&store->mutex);
        return NULL;
    }

    int increment = 0;
    for (size_t i = index; i < index + offset; i++) {
        for (StoreNode *node = store->buckets[i]; node; node = node->next) {
            int needed = snprintf(NULL, 0, "%d: %s -- %s;\n bucket: %zu; timestamp: %ld; index: %zu\n\n",
                                  increment, node->key, node->value, i, (long)node->created_at,
                                  node->hash % store->capacity);
            if (needed < 0) {
                free(dump);
                pthread_mutex_unlock(&store->mutex);
                return NULL;
            }

            if (used + (size_t)needed + 1 > capacity) {
                while (used + (size_t)needed + 1 > capacity) {
                    capacity *= 2;
                }
                char *new_dump = realloc(dump, capacity);
                if (!new_dump) {
                    free(dump);
                    pthread_mutex_unlock(&store->mutex);
                    return NULL;
                }
                dump = new_dump;
            }

            snprintf(dump + used, capacity - used,
                     "%d: %s -- %s;\n bucket: %zu; timestamp: %ld; index: %zu\n\n",
                     increment++, node->key, node->value, i, (long)node->created_at,
                     node->hash % store->capacity);
            used += (size_t)needed;
        }
    }

    pthread_mutex_unlock(&store->mutex);
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
