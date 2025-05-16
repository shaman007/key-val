#include <stdio.h>                  // For printf, perror, snprintf
#include <stdlib.h>                 // For malloc, calloc, realloc, free, exit
#include <string.h>                 // For strlen, strcmp, strdup, strtok, strncat, snprintf
#include <unistd.h>                 // For close, read, write
#include <arpa/inet.h>              // For sockaddr_in, inet_ntoa
#include <fcntl.h>                  // For fcntl
#include <sys/epoll.h>              // For epoll_create1, epoll_ctl, struct epoll_event
#include <pthread.h>                // For pthread_create, pthread_mutex_t
#include <errno.h>                  // For EAGAIN, EWOULDBLOCK
#include <ctype.h>                  // For isspace
#include <time.h>                   // For time

#define PORT 8080                   // Port to listen on
#define MAX_EVENTS 1000             // Maximum number of events to process at once
#define WORKER_THREADS 4            // Number of worker threads to create
#define BUFFER_SIZE 1024            // Size of the read/write buffer
#define INITIAL_CAPACITY 1023       // Initial capacity of the hash table, start with a prime number
#define LOAD_FACTOR_THRESHOLD 0.75  // Load factor threshold for resizing the hash table
#define MAX_TTL 31536000            // Maximum time-to-live for a key-value pair in seconds

pthread_mutex_t store_mutex = PTHREAD_MUTEX_INITIALIZER; // Mutex for the hash table modifications in the concurrent worker threads

// ======== Hash Table Definitions =========
typedef struct Node {
    char *key;
    char *value;
    time_t created_at;
    time_t ttl;
    unsigned long hash;
    struct Node *next;
} Node;

typedef struct HashTable {
    Node **buckets;
    size_t capacity;
    size_t count;
} HashTable;

// ======== Epoll Variables =========
int epoll_fd;                   // Epoll file descriptor
int server_socket;              // Server socket file descriptor
HashTable *global_table;        // Global hash table

// ======== Utility Functions =========
void set_nonblocking(int sock) {
    int flags = fcntl(sock, F_GETFL, 0);
    fcntl(sock, F_SETFL, flags | O_NONBLOCK);
}

// ======== Hash Table Functions =========
unsigned long hash(const char *str) {
    unsigned long hash = 5381;
    int c;
    while ((c = *str++)) {
        hash = ((hash << 5) + hash) + c;
    }
    return hash;
}

// Helper to trim newline and carriage return characters.
void trim_newline(char *s) {
    char *p = s;
    while (*p) {
        if (*p == '\n' || *p == '\r') {
            *p = '\0';
            break;
        }
        p++;
    }
}

// Create a new hash table with a given capacity.
int create_table(size_t capacity) {
    global_table = malloc(sizeof(HashTable));
    if (!global_table) {
        perror("Failed to allocate hash table");
        exit(EXIT_FAILURE);
    }
    global_table->capacity = capacity;
    global_table->count = 0;
    global_table->buckets = malloc(global_table->capacity * sizeof(Node *));
    if (!global_table->buckets) {
        perror("Failed to allocate buckets");
        free(global_table);
        exit(EXIT_FAILURE);
    }
    return 0;
}

// Create a new node with key and value.
Node *create_node(const char *key, const char *value, unsigned long h) {
    Node *new_node = calloc(1, sizeof(Node));
    if (!new_node) {
        perror("Failed to allocate node");
        exit(EXIT_FAILURE);
    }
    new_node->key = strdup(key);
    if (!new_node->key) {
        perror("Failed to allocate key string");
        free(new_node);
        exit(EXIT_FAILURE);
    }
    new_node->value = strdup(value);
    if (!new_node->value) {
        perror("Failed to allocate value string");
        free(new_node->key);
        free(new_node);
        exit(EXIT_FAILURE);
    }
    new_node->ttl = MAX_TTL;
    new_node->hash = h;
    new_node->created_at = time(NULL);
    new_node->next = NULL;

    return new_node;
}

// Forward declaration for rehashing.
void resize_table();

// Insert a key-value pair into the hash table.
int insert(const char *key, const char *value, size_t ttl, int type) {
    pthread_mutex_lock(&store_mutex);
    // Check if we need to resize before inserting.
    double load_factor = (double)global_table->count / global_table->capacity;
    if (load_factor > LOAD_FACTOR_THRESHOLD) {
        resize_table(global_table);
    }
    unsigned long h = hash(key);
    unsigned long index = h % global_table->capacity;
    Node *head = global_table->buckets[index];

    // Check if the key already exists; if so, update its value.
    for (Node *curr = head; curr != NULL; curr = curr->next) {
        if (strcmp(curr->key, key) == 0) {
            // Replace the existing value, if it's not 'add' operation.
            if (type !=2)
            {
                free(curr->value);
                curr->value = strdup(value);
                curr->ttl = ttl;
                if (!curr->value) {
                    perror("Failed to allocate new value string");
                    exit(EXIT_FAILURE);
                }
                curr->created_at = time(NULL);
                pthread_mutex_unlock(&store_mutex);
                return 1;
            } else {
                pthread_mutex_unlock(&store_mutex);
                return -1;
            }
        }
    }

    // Key not found; create a new node and insert at the beginning.
    if (type != 1)
    {
        Node *new_node = create_node(key, value, h);
        new_node->created_at = time(NULL);
        new_node->ttl = ttl;
        new_node->next = head;
        global_table->buckets[index] = new_node;
        global_table->count++;
        pthread_mutex_unlock(&store_mutex);
        return 1;
    } else {
        pthread_mutex_unlock(&store_mutex);
        return -1;
    }
}

int delete(const char *key);

// Search for a key in the hash table. Returns the value string if found,
// or NULL if the key does not exist.
Node *search(const char *key) {
    pthread_mutex_lock(&store_mutex);
    unsigned long h = hash(key);
    unsigned long index = h % global_table->capacity;
    Node *node = global_table->buckets[index];
    Node *prev = NULL;
    while (node) {
        if (node->hash == h && strcmp(node->key, key) == 0 && (time(NULL) - node->created_at) < node->ttl) {
            printf("%ld, %ld\n", time(NULL) - node->created_at, node->ttl);
            pthread_mutex_unlock(&store_mutex);
            return node;
        }else if (node->hash == h && strcmp(node->key, key) == 0 && (time(NULL) - node->created_at) >= node->ttl) {
            // This block basically do part of delete(), but without strings comparison since we already have the node to delete
            if (prev) {
                    prev->next = node->next;
                } else {
                    global_table->buckets[index] = node->next;
                }
            free(node->key);
            free(node->value);
            free(node);
            global_table->count--;
            pthread_mutex_unlock(&store_mutex);
            return NULL;              
        }
        prev = node;
        node = node->next;
    }
    pthread_mutex_unlock(&store_mutex);
    return NULL;
}

// Delete a key from the hash table. Returns 1 if the key was found and deleted, -1 otherwise.
int delete(const char *key) {
    Node *delete_node = search(key);
    if (delete_node == NULL) {
        return -1;
    } else
    {
        pthread_mutex_lock(&store_mutex);
        unsigned long h = hash(key);
        unsigned long index = h % global_table->capacity;
        Node *node = global_table->buckets[index];
        Node *prev = NULL;
        while (node) {
            if (node->hash == h && strcmp(node->key, key) == 0) {
                if (prev) {
                    prev->next = node->next;
                } else {
                    global_table->buckets[index] = node->next;
                }
                free(node->key);
                free(node->value);
                free(node);
                global_table->count--;
                pthread_mutex_unlock(&store_mutex);
                return 1;
            }
            prev = node;
            node = node->next;
        }
        pthread_mutex_unlock(&store_mutex);
        return -1;
    }
}


// Resize the hash table to a new capacity.
void resize_table() {
    pthread_mutex_lock(&store_mutex);
    size_t old_capacity = global_table->capacity;
    size_t new_capacity = old_capacity * 3;  // Growth strategy is simple.
    Node **new_buckets = calloc(new_capacity, sizeof(Node *));
    if (!new_buckets) {
        perror("Failed to allocate new buckets during resize");
        pthread_mutex_unlock(&store_mutex);
        exit(EXIT_FAILURE);
    }

    // Rehash all existing entries into the new bucket array.
    for (size_t i = 0; i < old_capacity; i++) {
        Node *node = global_table->buckets[i];
        while (node) {
            Node *next_node = node->next; // Save next pointer.

            unsigned long new_index = node->hash % new_capacity;
            // Insert node into new bucket list.
            node->next = new_buckets[new_index];
            new_buckets[new_index] = node;
            node = next_node;
        }
    }

    // Replace old buckets with new buckets.
    free(global_table->buckets);
    global_table->buckets = new_buckets;
    global_table->capacity = new_capacity;

    // The count remains unchanged.
    printf("Resized table to new capacity: %zu\n", new_capacity);
    pthread_mutex_unlock(&store_mutex);
}

// Free all nodes in a linked list.
void free_list(Node *node) {
    if (!node) {
        // If the node is NULL, there's nothing to free.
        return;
    }
    while (node) {
        Node *temp = node;
        node = node->next;
        free(temp->key);
        free(temp->value);
        free(temp);
    }
}

// Free the entire hash table.
void free_table() {
    pthread_mutex_lock(&store_mutex);
    // Check if the global table exists
    if (!global_table) {
        fprintf(stderr, "Error: Attempted to free a NULL hash table.\n");
        pthread_mutex_unlock(&store_mutex);
        return;
    }

    // Check if the buckets array exists
    if (!global_table->buckets) {
        fprintf(stderr, "Error: Hash table buckets are NULL.\n");
        free(global_table);
        pthread_mutex_unlock(&store_mutex);
        return;
    }
    for (size_t i = 0; i < global_table->capacity; i++) {
        if (global_table->buckets[i])
            free_list(global_table->buckets[i]);
    }
    free(global_table->buckets);
    free(global_table);
    global_table = NULL; // Set to NULL to avoid dangling pointer
    printf("Freed hash table and its buckets.\n");
    pthread_mutex_unlock(&store_mutex);
};

void garbage_collect() {
    pthread_mutex_lock(&store_mutex);
    for (size_t i = 0; i < global_table->capacity; i++) {
        Node *node = global_table->buckets[i];
        Node *prev = NULL;
        while (node) {
            if (node->ttl < (time(NULL) - node->created_at)) {
                if (prev) {
                    prev->next = node->next;
                } else {
                    global_table->buckets[i] = node->next;
                }
                free(node->key);
                free(node->value);
                free(node);
                if (prev) {
                    node = prev->next;
                } else {
                    node = global_table->buckets[i];
                }
                global_table->count--;
            } else {
                prev = node;
                node = node->next;
            }
        }
    }
    pthread_mutex_unlock(&store_mutex);
}

// Dump the contents of the hash table as a string.
char *dump_store(size_t index, size_t offset) {
    if (index >= global_table->capacity || index + offset >= global_table->capacity) {
        perror("Invalid index or offset");
        return NULL;
    }
    garbage_collect();
    pthread_mutex_lock(&store_mutex);

    size_t buffer_size = 1024; // Initial buffer size
    char *dump = malloc(buffer_size);
    if (!dump) {
        perror("Failed to allocate dump string");
        exit(EXIT_FAILURE);
    }
    dump[0] = '\0';  // Start with an empty string.

    int increment = 0;
    for (size_t i = index; i < index+offset; i++) {
        Node *node = global_table->buckets[i];
        while (node) {
            // Calculate the required size for the new line
            size_t line_size = strlen(node->key) + strlen(node->value) + 128;
            if (strlen(dump) + line_size + 1 > buffer_size) {
                // Expand the buffer size if needed
                buffer_size = strlen(dump) + line_size + 1;
                char *new_dump = realloc(dump, buffer_size);
                if (!new_dump) {
                    perror("Failed to reallocate dump string");
                    free(dump);
                    pthread_mutex_unlock(&store_mutex);
                    return NULL;
                }
                dump = new_dump;
            }
                char line[line_size];
                snprintf(line, sizeof(line), "%d: %s -- %s;\n bucket: %ld; timestamp: %ld; index: %ld\n\n",increment++, node->key, node->value, i, node->created_at, node->hash % global_table->capacity);
                dump = realloc(dump, strlen(dump) + strlen(line) + 1); //this is stupid, I will do it better eventually
                strncat(dump, line, strlen(line));
                node = node->next;
        }
    }
    pthread_mutex_unlock(&store_mutex);
    return dump;
}

// ======== Read Client Data Function =========
void read_client_data(int client_socket) {
    char buffer[BUFFER_SIZE];

    while (1) {
        // Read data from the client
        int bytes_read = read(client_socket, buffer, BUFFER_SIZE - 1);
        if (bytes_read < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // No more data to read
                break;
            }
            perror("Read error");
            close(client_socket);
            return;
        }

        if (bytes_read == 0) {
            // Client disconnected
            printf("Client disconnected: %d\n", client_socket);
            close(client_socket);
            epoll_ctl(epoll_fd, EPOLL_CTL_DEL, client_socket, NULL);
            return;
        }

        // Null-terminate the buffer and trim newlines
        buffer[bytes_read] = '\0';
        trim_newline(buffer);

        // Parse the command and arguments
        char command[16], key[256], value[768], ttl[32];
        size_t ttl_val = 0;
        int num_tokens = sscanf(buffer, "%15s %255s %767s %31s", command, key, value, ttl);

        if (num_tokens >= 1) {
            // Determine TTL value
            ttl_val = (num_tokens == 4) ? atoi(ttl) : MAX_TTL;

            if (strcasecmp(command, "write") == 0 && num_tokens >= 3) {
                if (insert(key, value, ttl_val, 0) == 1) {
                    write(client_socket, "OK\n", 3);
                } else {
                    write(client_socket, "Error: failed to write\n", 23);
                }
            } else if (strcasecmp(command, "update") == 0 && num_tokens >= 3) {
                if (insert(key, value, ttl_val, 1) == 1) {
                    write(client_socket, "OK\n", 3);
                } else {
                    write(client_socket, "Error: failed to update, key not found\n", 39);
                }
            } else if (strcasecmp(command, "add") == 0 && num_tokens >= 3) {
                if (insert(key, value, ttl_val, 2) == 1) {
                    write(client_socket, "OK\n", 3);
                } else {
                    write(client_socket, "Error: failed to add, key exists\n", 33);
                }
            } else if (strcasecmp(command, "search") == 0 && num_tokens == 2) {
                Node *found = search(key);
                if (found) {
                    char response[BUFFER_SIZE];
                    snprintf(response, BUFFER_SIZE, "Found: %s, timestamp: %ld\n", found->value, found->created_at);
                    write(client_socket, response, strlen(response));
                } else {
                    write(client_socket, "Not found\n", 10);
                }
            } else if (strcasecmp(command, "quit") == 0) {
                const char *response = "Goodbye!\n";
                send(client_socket, response, strlen(response), 0);
                close(client_socket);
                break;
            } else if (strcasecmp(command, "dump") == 0) {
                size_t index = 0, offset = INITIAL_CAPACITY - 1;
                if (num_tokens == 3) {
                    index = atoi(key);
                    offset = atoi(value);
                }
                char *dump = dump_store(index, offset);
                if (dump) {
                    send(client_socket, dump, strlen(dump), 0);
                    free(dump);
                } else {
                    write(client_socket, "Error: failed to dump store\n", 28);
                }
            } else if (strcasecmp(command, "size") == 0) {
                garbage_collect();
                char response[BUFFER_SIZE];
                snprintf(response, BUFFER_SIZE, "%zu, %zu\n", global_table->count, global_table->capacity);
                send(client_socket, response, strlen(response), 0);
            } else if (strcasecmp(command, "wipe") == 0) {
                free_table();
                if (create_table(INITIAL_CAPACITY) != 0) {
                    perror("Failed to create hash table");
                    exit(EXIT_FAILURE);
                }
                write(client_socket, "All clean!\n", 11);
            } else if (strcasecmp(command, "delete") == 0 && num_tokens == 2) {
                if (delete(key) == 1) {
                    write(client_socket, "OK\n", 3);
                } else {
                    write(client_socket, "Not found\n", 10);
                }
            } else {
                write(client_socket, "Error: unknown command! Use write, search, dump, delete, size, wipe or quit.\n", 78);
            }
        } else {
            write(client_socket, "Error: invalid command! Use write, search, dump, delete, size, wipe or quit.\n", 78);
        }
    }
}

// ======== Worker Thread Function =========
void *worker_thread() {
    struct epoll_event events[MAX_EVENTS];

    while (1) {
        // Wait for events on the epoll instance
        int num_events = epoll_wait(epoll_fd, events, MAX_EVENTS, -1);
        if (num_events < 0) {
            if (errno == EINTR) {
                // Interrupted by a signal, continue waiting
                continue;
            }
            perror("epoll_wait failed");
            break; // Exit the loop on critical error
        }

        // Process each event
        for (int i = 0; i < num_events; i++) {
            int client_socket = events[i].data.fd;

            // Skip the server socket (handled in the main thread)
            if (client_socket == server_socket) {
                continue;
            }

            // Check for errors or invalid events
            if (events[i].events & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) {
                fprintf(stderr, "Client socket %d error or hang-up, closing connection\n", client_socket);
                close(client_socket);
                epoll_ctl(epoll_fd, EPOLL_CTL_DEL, client_socket, NULL);
                continue;
            }

            // Handle readable events
            if (events[i].events & EPOLLIN) {
                read_client_data(client_socket);
            }
        }
    }

    return NULL;
}

// ======== Main Function =========
int main() {
    struct sockaddr_in server_addr; // Server address structure
    int opt = 1;                    // For setsockopt reuseaddr option to avoid "Address already in use" error

    // Create server socket
    server_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (server_socket == -1) {
        perror("Socket creation failed");
        exit(1);
    }

    setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(PORT);

    if (bind(server_socket, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        perror("Bind failed");
        exit(1);
    }

    if (listen(server_socket, SOMAXCONN) < 0) {
        perror("Listen failed");
        exit(1);
    }

    printf("Server listening on port %d...\n", PORT);

    // Create epoll instance
    epoll_fd = epoll_create1(0);
    if (epoll_fd == -1) {
        perror("epoll_create1 failed");
        exit(1);
    }

    // Add server socket to epoll
    struct epoll_event event;        // Event structure to add to epoll
    event.events = EPOLLIN;          // Read event for the server socket
    event.data.fd = server_socket;   // Server socket file descriptor
    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_socket, &event) < 0) {
        perror("epoll_ctl server_socket failed");
        exit(1);
    }

    // Create worker threads
    pthread_t threads[WORKER_THREADS];         // Worker thread array
    for (int i = 0; i < WORKER_THREADS; i++) {
        pthread_create(&threads[i], NULL, worker_thread, NULL);
    }

    int error = create_table(INITIAL_CAPACITY);
    if (error != 0) {
        perror("Failed to create hash table");
        exit(1);
    }

    while (1) {
        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);
        int client_socket = accept(server_socket, (struct sockaddr *)&client_addr, &client_len);

        if (client_socket < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) continue;
            perror("accept failed");
            continue;
        }

        printf("Accepted connection from %s:%d\n", inet_ntoa(client_addr.sin_addr), ntohs(client_addr.sin_port));
        set_nonblocking(client_socket);

        event.events = EPOLLIN | EPOLLET;
        event.data.fd = client_socket;
        if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_socket, &event) < 0) {
            perror("epoll_ctl client_socket failed");
            close(client_socket);
        }
    }

    close(server_socket);
    return 0;
}
