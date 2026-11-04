#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <netinet/tcp.h>
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/epoll.h>
#include <unistd.h>

#include "src/store.h"

#define PORT 8080
#define MAX_EVENTS 1000
#define WORKER_THREADS 4
#define INPUT_BUFFER_SIZE 4096
#define INITIAL_OUTPUT_CAPACITY 1024
#define MAX_OUTPUT_CAPACITY (1024 * 1024)
#define INITIAL_CAPACITY 1023

typedef struct Worker {
    int epoll_fd;
    pthread_t thread;
} Worker;

typedef struct Connection {
    int fd;
    Worker *worker;
    char input[INPUT_BUFFER_SIZE];
    size_t input_used;
    char *output;
    size_t output_used;
    size_t output_sent;
    size_t output_capacity;
    int close_after_write;
    uint32_t registered_events;
} Connection;

static int server_socket;
static Store *global_store;
static Worker workers[WORKER_THREADS];
static size_t next_worker;

static int set_nonblocking(int sock) {
    int flags = fcntl(sock, F_GETFL, 0);
    if (flags == -1) {
        return -1;
    }
    return fcntl(sock, F_SETFL, flags | O_NONBLOCK);
}

static Connection *connection_create(int fd, Worker *worker) {
    Connection *conn = calloc(1, sizeof(*conn));
    if (!conn) {
        return NULL;
    }

    conn->output = malloc(INITIAL_OUTPUT_CAPACITY);
    if (!conn->output) {
        free(conn);
        return NULL;
    }

    conn->fd = fd;
    conn->worker = worker;
    conn->output_capacity = INITIAL_OUTPUT_CAPACITY;
    return conn;
}

static void connection_destroy(Connection *conn) {
    if (!conn) {
        return;
    }

    epoll_ctl(conn->worker->epoll_fd, EPOLL_CTL_DEL, conn->fd, NULL);
    close(conn->fd);
    free(conn->output);
    free(conn);
}

static int connection_update_events(Connection *conn) {
    struct epoll_event event;
    event.data.ptr = conn;
    event.events = conn->close_after_write ? 0 : EPOLLIN | EPOLLRDHUP;
    if (conn->output_sent < conn->output_used) {
        event.events |= EPOLLOUT;
    }

    if (event.events == conn->registered_events) return 0;
    if (epoll_ctl(conn->worker->epoll_fd, EPOLL_CTL_MOD, conn->fd, &event) < 0) return -1;
    conn->registered_events = event.events;
    return 0;
}

static int queue_bytes(Connection *conn, const char *data, size_t len) {
    if (len == 0) {
        return 0;
    }

    size_t pending = conn->output_used - conn->output_sent;
    if (len > MAX_OUTPUT_CAPACITY - pending) return -1;
    if (conn->output_sent > 0 && pending > 0) {
        memmove(conn->output, conn->output + conn->output_sent, pending);
        conn->output_used = pending;
        conn->output_sent = 0;
    } else if (pending == 0) {
        conn->output_used = 0;
        conn->output_sent = 0;
    }

    if (conn->output_used + len > conn->output_capacity) {
        size_t new_capacity = conn->output_capacity;
        while (conn->output_used + len > new_capacity) {
            new_capacity *= 2;
        }

        char *new_output = realloc(conn->output, new_capacity);
        if (!new_output) {
            return -1;
        }

        conn->output = new_output;
        conn->output_capacity = new_capacity;
    }

    memcpy(conn->output + conn->output_used, data, len);
    conn->output_used += len;
    return 0;
}

static int queue_text(Connection *conn, const char *text) {
    return queue_bytes(conn, text, strlen(text));
}

static int flush_output(Connection *conn) {
    while (conn->output_sent < conn->output_used) {
        ssize_t written = send(conn->fd, conn->output + conn->output_sent, conn->output_used - conn->output_sent, MSG_NOSIGNAL);
        if (written < 0) {
            if (errno == EINTR) {
                continue;
            }
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                return 0;
            }
            return -1;
        }
        if (written == 0) {
            return 0;
        }
        conn->output_sent += (size_t)written;
    }

    conn->output_used = 0;
    conn->output_sent = 0;
    return conn->close_after_write ? 1 : 0;
}

static time_t parse_ttl(int num_tokens, const char *ttl) {
    if (num_tokens != 4) {
        return STORE_DEFAULT_MAX_TTL;
    }

    char *end = NULL;
    long value = strtol(ttl, &end, 10);
    if (!end || *end != '\0' || value <= 0) {
        return STORE_DEFAULT_MAX_TTL;
    }
    return (time_t)value;
}

static int handle_store_write(Connection *conn, const char *key, const char *value, time_t ttl, StoreWriteMode mode) {
    StoreResult result = store_put(global_store, key, value, ttl, mode);
    if (result == STORE_OK) {
        return queue_text(conn, "OK\n");
    }
    if (result == STORE_EXISTS) {
        return queue_text(conn, "Error: failed to add, key exists\n");
    }
    if (result == STORE_NOT_FOUND) {
        return queue_text(conn, "Error: failed to update, key not found\n");
    }
    return queue_text(conn, "Error: failed to write\n");
}

static int handle_command(Connection *conn, char *line) {
    /* Tokenize in place: no format interpreter, copies, or truncation. */
    char *tokens[5];
    int num_tokens = 0;
    char *cursor = line;
    while (*cursor && num_tokens < 5) {
        while (isspace((unsigned char)*cursor)) cursor++;
        if (!*cursor) break;
        tokens[num_tokens++] = cursor;
        while (*cursor && !isspace((unsigned char)*cursor)) cursor++;
        if (*cursor) *cursor++ = '\0';
    }
    char *command = num_tokens > 0 ? tokens[0] : "";
    char *key = num_tokens > 1 ? tokens[1] : "";
    char *value = num_tokens > 2 ? tokens[2] : "";
    char *ttl = num_tokens > 3 ? tokens[3] : "";
    if (num_tokens > 4 || strlen(command) > 15 || strlen(key) > 255 ||
        strlen(value) > 767 || strlen(ttl) > 31)
        return queue_text(conn, "Error: invalid command arguments\n");

    if (num_tokens < 1) {
        return queue_text(conn, "Error: invalid command! Use write, search, dump, delete, size, wipe or quit.\n");
    }

    time_t ttl_val = parse_ttl(num_tokens, ttl);

    if (strcasecmp(command, "write") == 0 && num_tokens >= 3) {
        return handle_store_write(conn, key, value, ttl_val, STORE_WRITE);
    }
    if (strcasecmp(command, "update") == 0 && num_tokens >= 3) {
        return handle_store_write(conn, key, value, ttl_val, STORE_UPDATE);
    }
    if (strcasecmp(command, "add") == 0 && num_tokens >= 3) {
        return handle_store_write(conn, key, value, ttl_val, STORE_ADD);
    }
    if (strcasecmp(command, "search") == 0 && num_tokens == 2) {
        StoreEntry entry;
        StoreResult result = store_get(global_store, key, &entry);
        if (result == STORE_OK) {
            char response[832]; /* protocol values are at most 767 bytes */
            int needed = snprintf(response, sizeof(response), "Found: %s, timestamp: %ld\n",
                                  entry.value, (long)entry.created_at);
            int queued = needed < 0 || (size_t)needed >= sizeof(response) ? -1 :
                         queue_bytes(conn, response, (size_t)needed);
            store_entry_free(&entry);
            return queued;
        }
        return queue_text(conn, result == STORE_NOT_FOUND ? "Not found\n" : "Error: failed to read\n");
    }
    if (strcasecmp(command, "quit") == 0 && num_tokens == 1) {
        conn->close_after_write = 1;
        return queue_text(conn, "Goodbye!\n");
    }
    if (strcasecmp(command, "dump") == 0 && (num_tokens == 1 || num_tokens == 3)) {
        size_t index = 0;
        size_t offset = INITIAL_CAPACITY - 1;
        if (num_tokens == 3) {
            index = (size_t)strtoul(key, NULL, 10);
            offset = (size_t)strtoul(value, NULL, 10);
        }

        char *dump = store_dump(global_store, index, offset);
        if (dump) {
            int queued = queue_text(conn, dump);
            free(dump);
            return queued;
        }
        return queue_text(conn, "Error: failed to dump store\n");
    }
    if (strcasecmp(command, "size") == 0 && num_tokens == 1) {
        char response[128];
        snprintf(response, sizeof(response), "%zu, %zu\n", store_count(global_store), store_capacity(global_store));
        return queue_text(conn, response);
    }
    if (strcasecmp(command, "wipe") == 0 && num_tokens == 1) {
        store_clear(global_store);
        return queue_text(conn, "All clean!\n");
    }
    if (strcasecmp(command, "delete") == 0 && num_tokens == 2) {
        if (store_delete(global_store, key) == STORE_OK) {
            return queue_text(conn, "OK\n");
        }
        return queue_text(conn, "Not found\n");
    }

    return queue_text(conn, "Error: unknown command! Use write, search, dump, delete, size, wipe or quit.\n");
}

static void consume_input(Connection *conn, size_t consumed) {
    size_t remaining = conn->input_used - consumed;
    if (remaining > 0) {
        memmove(conn->input, conn->input + consumed, remaining);
    }
    conn->input_used = remaining;
}

static int process_input_lines(Connection *conn) {
    size_t consumed = 0;
    while (consumed < conn->input_used) {
        char *line = conn->input + consumed;
        char *newline = memchr(line, '\n', conn->input_used - consumed);
        if (!newline) break;
        size_t length = (size_t)(newline - line);
        /* Reject embedded NULs instead of executing a truncated prefix. */
        if (memchr(line, '\0', length)) return -1;
        *newline = '\0';
        if (length && line[length - 1] == '\r') line[length - 1] = '\0';
        consumed = (size_t)(newline - conn->input) + 1;
        if (handle_command(conn, line) < 0) return -1;
        if (conn->close_after_write) break;
    }
    consume_input(conn, consumed);
    if (conn->input_used == sizeof(conn->input)) {
        conn->input_used = 0;
        conn->close_after_write = 1;
        return queue_text(conn, "Error: command too long\n");
    }
    return 0;
}

static int read_client_data(Connection *conn) {
    for (int batch = 0; batch < 16 && !conn->close_after_write; batch++) {
        ssize_t bytes_read = read(conn->fd, conn->input + conn->input_used, sizeof(conn->input) - conn->input_used);
        if (bytes_read < 0) {
            if (errno == EINTR) {
                continue;
            }
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                return 0;
            }
            return -1;
        }

        if (bytes_read == 0) {
            conn->close_after_write = 1;
            return conn->output_sent == conn->output_used ? 1 : 0;
        }

        conn->input_used += (size_t)bytes_read;
        if (process_input_lines(conn) < 0) {
            return -1;
        }
        /* Bound queued replies even when a peer pipelines without reading.
         * Flush once per read batch, rather than after draining the socket. */
        int flushed = flush_output(conn);
        if (flushed != 0) return flushed;
        if (conn->input_used == sizeof(conn->input) && !conn->close_after_write) {
            conn->input_used = 0;
            conn->close_after_write = 1;
            if (queue_text(conn, "Error: command too long\n") < 0) {
                return -1;
            }
        }
    }

    return 0;
}

static void handle_connection_event(Connection *conn, uint32_t events) {
    int should_close = 0;

    if (events & (EPOLLERR | EPOLLHUP)) {
        should_close = 1;
    }

    if (!should_close && (events & (EPOLLIN | EPOLLRDHUP))) {
        int read_result = read_client_data(conn);
        if (read_result != 0) {
            should_close = 1;
        }
    }

    if (!should_close && (events & EPOLLOUT)) {
        int write_result = flush_output(conn);
        if (write_result != 0) {
            should_close = 1;
        }
    }

    if (!should_close && conn->output_sent < conn->output_used) {
        int write_result = flush_output(conn);
        if (write_result != 0) {
            should_close = 1;
        }
    }

    if (should_close) {
        connection_destroy(conn);
        return;
    }

    if (connection_update_events(conn) < 0) {
        connection_destroy(conn);
    }
}

static void *worker_thread(void *arg) {
    Worker *worker = arg;
    struct epoll_event events[MAX_EVENTS];

    while (1) {
        int num_events = epoll_wait(worker->epoll_fd, events, MAX_EVENTS, -1);
        if (num_events < 0) {
            if (errno == EINTR) {
                continue;
            }
            perror("epoll_wait");
            break;
        }

        for (int i = 0; i < num_events; i++) {
            handle_connection_event(events[i].data.ptr, events[i].events);
        }
    }

    return NULL;
}

static int accept_client(int client_socket) {
    if (set_nonblocking(client_socket) < 0) {
        perror("set_nonblocking");
        close(client_socket);
        return -1;
    }

    int nodelay = 1;
    setsockopt(client_socket, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay));

    Worker *worker = &workers[next_worker];
    next_worker = (next_worker + 1) % WORKER_THREADS;

    Connection *conn = connection_create(client_socket, worker);
    if (!conn) {
        close(client_socket);
        return -1;
    }

    struct epoll_event event;
    event.events = EPOLLIN | EPOLLRDHUP;
    event.data.ptr = conn;
    conn->registered_events = event.events;
    if (epoll_ctl(worker->epoll_fd, EPOLL_CTL_ADD, client_socket, &event) < 0) {
        perror("epoll_ctl client");
        connection_destroy(conn);
        return -1;
    }

    return 0;
}

int main(void) {
    struct sockaddr_in server_addr;
    int opt = 1;

    global_store = store_create(INITIAL_CAPACITY, STORE_DEFAULT_MAX_TTL);
    if (!global_store) {
        fprintf(stderr, "Failed to create store\n");
        return EXIT_FAILURE;
    }

    server_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (server_socket == -1) {
        perror("socket");
        return EXIT_FAILURE;
    }

    if (setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        perror("setsockopt");
        return EXIT_FAILURE;
    }

    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(PORT);

    if (bind(server_socket, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        perror("bind");
        return EXIT_FAILURE;
    }

    if (listen(server_socket, SOMAXCONN) < 0) {
        perror("listen");
        return EXIT_FAILURE;
    }

    for (int i = 0; i < WORKER_THREADS; i++) {
        workers[i].epoll_fd = epoll_create1(0);
        if (workers[i].epoll_fd == -1) {
            perror("epoll_create1");
            return EXIT_FAILURE;
        }

        if (pthread_create(&workers[i].thread, NULL, worker_thread, &workers[i]) != 0) {
            perror("pthread_create");
            return EXIT_FAILURE;
        }
    }

    printf("Server listening on port %d...\n", PORT);

    while (1) {
        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);
        int client_socket = accept(server_socket, (struct sockaddr *)&client_addr, &client_len);

        if (client_socket < 0) {
            if (errno == EINTR) {
                continue;
            }
            perror("accept");
            continue;
        }

        accept_client(client_socket);
    }

    store_destroy(global_store);
    close(server_socket);
    return EXIT_SUCCESS;
}
