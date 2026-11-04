#include "../src/store.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <stdatomic.h>

#define ASSERT_TRUE(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "assertion failed at %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        exit(EXIT_FAILURE); \
    } \
} while (0)

static void test_write_add_update_delete(void) {
    Store *store = store_create(3, STORE_DEFAULT_MAX_TTL);
    StoreEntry entry;

    ASSERT_TRUE(store != NULL);
    ASSERT_TRUE(store_put(store, "a", "one", 0, STORE_WRITE) == STORE_OK);
    ASSERT_TRUE(store_get(store, "a", &entry) == STORE_OK);
    ASSERT_TRUE(strcmp(entry.value, "one") == 0);
    store_entry_free(&entry);

    ASSERT_TRUE(store_put(store, "a", "two", 0, STORE_ADD) == STORE_EXISTS);
    ASSERT_TRUE(store_put(store, "a", "two", 0, STORE_UPDATE) == STORE_OK);
    ASSERT_TRUE(store_get(store, "a", &entry) == STORE_OK);
    ASSERT_TRUE(strcmp(entry.value, "two") == 0);
    store_entry_free(&entry);

    ASSERT_TRUE(store_put(store, "missing", "value", 0, STORE_UPDATE) == STORE_NOT_FOUND);
    ASSERT_TRUE(store_delete(store, "a") == STORE_OK);
    ASSERT_TRUE(store_get(store, "a", &entry) == STORE_NOT_FOUND);
    ASSERT_TRUE(store_delete(store, "a") == STORE_NOT_FOUND);

    store_destroy(store);
}

static void test_ttl_and_clear(void) {
    Store *store = store_create(5, STORE_DEFAULT_MAX_TTL);
    StoreEntry entry;

    ASSERT_TRUE(store != NULL);
    ASSERT_TRUE(store_put(store, "short", "gone", 1, STORE_WRITE) == STORE_OK);
    sleep(2);
    ASSERT_TRUE(store_get(store, "short", &entry) == STORE_NOT_FOUND);
    ASSERT_TRUE(store_count(store) == 0);

    ASSERT_TRUE(store_put(store, "a", "1", 0, STORE_WRITE) == STORE_OK);
    ASSERT_TRUE(store_put(store, "b", "2", 0, STORE_WRITE) == STORE_OK);
    ASSERT_TRUE(store_count(store) == 2);
    store_clear(store);
    ASSERT_TRUE(store_count(store) == 0);

    store_destroy(store);
}

static void test_resize_and_dump(void) {
    Store *store = store_create(3, STORE_DEFAULT_MAX_TTL);
    char key[32];
    char value[32];

    ASSERT_TRUE(store != NULL);
    size_t initial_capacity = store_capacity(store);
    for (int i = 0; i < 2000; i++) {
        snprintf(key, sizeof(key), "key-%d", i);
        snprintf(value, sizeof(value), "value-%d", i);
        ASSERT_TRUE(store_put(store, key, value, 0, STORE_WRITE) == STORE_OK);
    }

    ASSERT_TRUE(store_count(store) == 2000);
    ASSERT_TRUE(store_capacity(store) > initial_capacity);
    for (int i = 0; i < 2000; i++) {
        snprintf(key, sizeof(key), "key-%d", i);
        snprintf(value, sizeof(value), "value-%d", i);
        StoreEntry entry;
        ASSERT_TRUE(store_get(store, key, &entry) == STORE_OK);
        ASSERT_TRUE(strcmp(entry.value, value) == 0);
        store_entry_free(&entry);
    }

    char *dump = store_dump(store, 0, store_capacity(store));
    ASSERT_TRUE(dump != NULL);
    ASSERT_TRUE(strstr(dump, "key-") != NULL);
    free(dump);

    store_destroy(store);
}

typedef struct { Store *store; int id; atomic_int *wins; } ThreadArgs;
static void *exercise_store(void *arg) {
    ThreadArgs *a = arg;
    if (store_put(a->store, "shared", "winner", 0, STORE_ADD) == STORE_OK)
        atomic_fetch_add(a->wins, 1);
    char key[64], value[64];
    for (int i = 0; i < 2000; i++) {
        snprintf(key, sizeof(key), "thread-%d-key-%d", a->id, i);
        snprintf(value, sizeof(value), "value-%d", i);
        ASSERT_TRUE(store_put(a->store, key, value, 0, STORE_ADD) == STORE_OK);
        StoreEntry entry;
        ASSERT_TRUE(store_get(a->store, key, &entry) == STORE_OK);
        ASSERT_TRUE(strcmp(entry.value, value) == 0);
        store_entry_free(&entry);
        ASSERT_TRUE(store_put(a->store, key, "a much longer replacement value", 0, STORE_UPDATE) == STORE_OK);
        ASSERT_TRUE(store_put(a->store, key, "short", 0, STORE_UPDATE) == STORE_OK);
        ASSERT_TRUE(store_get(a->store, key, &entry) == STORE_OK);
        ASSERT_TRUE(strcmp(entry.value, "short") == 0);
        store_entry_free(&entry);
        if (i % 2 == 0) ASSERT_TRUE(store_delete(a->store, key) == STORE_OK);
    }
    return NULL;
}
static void *snapshot_store(void *arg) {
    Store *store = arg;
    for (int i = 0; i < 30; i++) {
        ASSERT_TRUE(store_count(store) <= 32001);
        size_t capacity = store_capacity(store);
        char *dump = store_dump(store, 0, capacity);
        ASSERT_TRUE(dump != NULL);
        free(dump);
    }
    return NULL;
}
static void test_concurrent_resize_and_add(void) {
    Store *store = store_create(1, STORE_DEFAULT_MAX_TTL);
    ASSERT_TRUE(store != NULL);
    atomic_int wins = 0;
    pthread_t threads[16], snapshot;
    ASSERT_TRUE(pthread_create(&snapshot, NULL, snapshot_store, store) == 0);
    ThreadArgs args[16];
    for (int i = 0; i < 16; i++) {
        args[i] = (ThreadArgs){store, i, &wins};
        ASSERT_TRUE(pthread_create(&threads[i], NULL, exercise_store, &args[i]) == 0);
    }
    for (int i = 0; i < 16; i++) ASSERT_TRUE(pthread_join(threads[i], NULL) == 0);
    ASSERT_TRUE(pthread_join(snapshot, NULL) == 0);
    ASSERT_TRUE(atomic_load(&wins) == 1);
    ASSERT_TRUE(store_count(store) == 16001);
    char *dump = store_dump(store, 0, store_capacity(store));
    ASSERT_TRUE(dump != NULL);
    ASSERT_TRUE(strstr(dump, "shared -- winner") != NULL);
    free(dump);
    store_clear(store);
    ASSERT_TRUE(store_count(store) == 0);
    store_destroy(store);
}

int main(void) {
    test_write_add_update_delete();
    test_ttl_and_clear();
    test_resize_and_dump();
    test_concurrent_resize_and_add();
    puts("store tests passed");
    return EXIT_SUCCESS;
}
