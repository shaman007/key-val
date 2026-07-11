#include "../src/store.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

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
    for (int i = 0; i < 20; i++) {
        snprintf(key, sizeof(key), "key-%d", i);
        snprintf(value, sizeof(value), "value-%d", i);
        ASSERT_TRUE(store_put(store, key, value, 0, STORE_WRITE) == STORE_OK);
    }

    ASSERT_TRUE(store_count(store) == 20);
    ASSERT_TRUE(store_capacity(store) > 3);

    char *dump = store_dump(store, 0, store_capacity(store));
    ASSERT_TRUE(dump != NULL);
    ASSERT_TRUE(strstr(dump, "key-") != NULL);
    free(dump);

    store_destroy(store);
}

int main(void) {
    test_write_add_update_delete();
    test_ttl_and_clear();
    test_resize_and_dump();
    puts("store tests passed");
    return EXIT_SUCCESS;
}
