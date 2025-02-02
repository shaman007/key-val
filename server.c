#include <stdio.h>     // For printf, perror, snprintf
#include <stdlib.h>    // For malloc, calloc, realloc, free, exit
#include <string.h>    // For strlen, strcmp, strdup, strtok, strncat, snprintf
#include <unistd.h>    // For close, read, write
#include <arpa/inet.h> // For sockaddr_in, inet_ntoa
#include <fcntl.h>     // For fcntl
#include <sys/epoll.h> // For epoll_create1, epoll_ctl, struct epoll_event
#include <pthread.h>   // For pthread_create, pthread_mutex_t
#include <errno.h>     // For EAGAIN, EWOULDBLOCK
#include <ctype.h>     // For isspace
#include <time.h>      // For time

#define PORT 8080                   // Port to listen on
#define MAX_EVENTS 1000             // Maximum number of events to process at once
#define WORKER_THREADS 4            // Number of worker threads to create
#define BUFFER_SIZE 1024            // Size of the read/write buffer
#define INITIAL_CAPACITY 101        // Initial capacity of the hash table, start with a prime number
#define LOAD_FACTOR_THRESHOLD 0.75  // Load factor threshold for resizing the hash table

pthread_mutex_t store_mutex = PTHREAD_MUTEX_INITIALIZER; // Mutex for the hash table modifications in the concurrent worker threads

// ======== Hash Table Definitions =========
typedef struct Node {
    char *key;
    char *value;
    time_t created_at;
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
    global_table->buckets = calloc(global_table->capacity, sizeof(Node *));
    if (!global_table->buckets) {
        perror("Failed to allocate buckets");
        free(global_table);
        exit(EXIT_FAILURE);
    }
    return 0;
}

// Create a new node with key and value.
Node *create_node(const char *key, const char *value) {
    Node *new_node = malloc(sizeof(Node));
    if (!new_node) {
        perror("Failed to allocate node");
        exit(EXIT_FAILURE);
    }
    new_node->key = malloc(strlen(key) + 1);
    new_node->key = strdup(key);
    if (!new_node->key) {
        perror("Failed to allocate key string");
        free(new_node->key);
        free(new_node);
        exit(EXIT_FAILURE);
    }
    new_node->value = malloc(strlen(value) + 1);
    new_node->value = strdup(value);
    if (!new_node->value) {
        perror("Failed to allocate value string");
        free(new_node->value);
        free(new_node);
        exit(EXIT_FAILURE);
    }
    new_node->next = NULL;
    return new_node;
}

// Forward declaration for rehashing.
void resize_table();

// Insert a key-value pair into the hash table.
int insert(const char *key, const char *value) {
    pthread_mutex_lock(&store_mutex);
    // Check if we need to resize before inserting.
    double load_factor = (double)global_table->count / global_table->capacity;
    if (load_factor > LOAD_FACTOR_THRESHOLD) {
        resize_table(global_table);
    }

    unsigned long index = hash(key) % global_table->capacity;
    Node *head = global_table->buckets[index];

    // Check if the key already exists; if so, update its value.
    for (Node *curr = head; curr != NULL; curr = curr->next) {
        if (strcmp(curr->key, key) == 0) {
            // Replace the existing value.
            free(curr->value);
            curr->value = malloc(strlen(value) + 1);
            curr->value = strdup(value);
            if (!curr->value) {
                perror("Failed to allocate new value string");
                exit(EXIT_FAILURE);
            }
            curr->created_at = time(NULL);
            pthread_mutex_unlock(&store_mutex);
            return 1;
        }
    }

    // Key not found; create a new node and insert at the beginning.
    Node *new_node = create_node(key, value);
    new_node->created_at = time(NULL);
    new_node->next = head;
    global_table->buckets[index] = new_node;
    global_table->count++;
    pthread_mutex_unlock(&store_mutex);
    return 1;
}

// Search for a key in the hash table. Returns the value string if found,
// or NULL if the key does not exist.
Node *search(const char *key) {
    pthread_mutex_lock(&store_mutex);
    unsigned long index = hash(key) % global_table->capacity;
    printf("Index: %ld\n", index);
    Node *node = global_table->buckets[index];
    while (node) {
        if (strcmp(node->key, key) == 0) {
            pthread_mutex_unlock(&store_mutex);
            return node;
        }
        node = node->next;
    }
    pthread_mutex_unlock(&store_mutex);
    return NULL;
}

// Resize the hash table to a new capacity.
void resize_table() {
    size_t old_capacity = global_table->capacity;
    size_t new_capacity = old_capacity * 2 + 1;  // Example growth strategy.
    Node **new_buckets = calloc(new_capacity, sizeof(Node *));
    if (!new_buckets) {
        perror("Failed to allocate new buckets during resize");
        exit(EXIT_FAILURE);
    }

    // Rehash all existing entries into the new bucket array.
    for (size_t i = 0; i < old_capacity; i++) {
        Node *node = global_table->buckets[i];
        while (node) {
            Node *next_node = node->next; // Save next pointer.

            unsigned long new_index = hash(node->key) % new_capacity;
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
}

// Free all nodes in a linked list.
void free_list(Node *node) {
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
    for (size_t i = 0; i < global_table->capacity; i++) {
        if (global_table->buckets[i])
            free_list(global_table->buckets[i]);
    }
    free(global_table->buckets);
    free(global_table);
    pthread_mutex_unlock(&store_mutex);
};


// Dump the contents of the hash table as a string.
char *dump_store() {
    pthread_mutex_lock(&store_mutex);
    char *dump = malloc(1);
    if (!dump) {
        perror("Failed to allocate dump string");
        exit(EXIT_FAILURE);
    }
    dump[0] = '\0';  // Start with an empty string.

    for (size_t i = 0; i < global_table->capacity; i++) {
        Node *node = global_table->buckets[i];
        while (node) {
            char line[strlen(node->key) + strlen(node->value) + 32];
            snprintf(line, sizeof(line), "%s: %s, %ld\n", node->key, node->value, node->created_at);
            dump = realloc(dump, strlen(dump) + strlen(line) + 1);
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
        int bytes_read = read(client_socket, buffer, BUFFER_SIZE - 1);
        buffer[bytes_read] = '\0';
        trim_newline(buffer);  // Remove newline if present.

        if (bytes_read == -1) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            perror("Read error");
            close(client_socket);
            return;
        }

        if (bytes_read == 0) {
            printf("Client disconnected: %d\n", client_socket);
            close(client_socket);
            epoll_ctl(epoll_fd, EPOLL_CTL_DEL, client_socket, NULL);
            return;
        }

        buffer[bytes_read] = '\0';

        char command[16], key[256], value[768];
        int num_tokens = sscanf(buffer, "%15s %255s %767[^\n]", command, key, value);

        if (num_tokens >= 1) {
            if (strcasecmp(command, "write") == 0 && num_tokens == 3) {
                insert(key, value);
                write(client_socket, "OK\n", 3);
            } else if (strcasecmp(command, "search") == 0 && num_tokens == 2) {
                Node *found = search(key);
                if (found) {
                    char *response;
                    response = malloc(BUFFER_SIZE);
                    snprintf(response, BUFFER_SIZE, "Found: %s, timestamp: %ld\n", found->value, found->created_at);
                    write(client_socket, response, strlen(response));
                    free (response);
                } else {
                    write(client_socket, "Not found\n", 10);
                }
            } else if (strcasecmp(command, "quit") == 0) {
                    const char *response = "Goodbye!\n";
                    send(client_socket, response, strlen(response), 0);
                    close(client_socket);
                    break;
            }  else if (strcasecmp(command, "dump") == 0) {
                    char *dump = dump_store();
                    if (!dump) {
                        const char *response = "Error: failed to dump store\n";
                        send(client_socket, response, strlen(response), 0);
                        continue;
                    } else{
                       send(client_socket, dump, strlen(dump), 0);
                    } 
            }  else if (strcasecmp(command, "wipe") == 0) {
                    free_table();
                    int error = create_table(INITIAL_CAPACITY);
                    if (error != 0) {
                        perror("Failed to create hash table");
                        exit(1);
                    }
                    const char *response = "All clean!\n";
                    send(client_socket, response, strlen(response), 0);
            } else {
                write(client_socket, "Error: unknown command! Use write, search, dump, wipe or quit.\n", 64);
            }
        } else {
            write(client_socket, "Error: invalid command! Use write, search, dump, wipe or quit.\n", 64);
        }
    }
}

// ======== Worker Thread Function =========
void *worker_thread(void *arg) {
    struct epoll_event events[MAX_EVENTS];

    while (1) {
        int num_events = epoll_wait(epoll_fd, events, MAX_EVENTS, -1);
        if (num_events < 0) {
            perror("epoll_wait failed");
            continue;
        }

        for (int i = 0; i < num_events; i++) {
            int client_socket = events[i].data.fd;
            if (client_socket == server_socket) continue;
            read_client_data(client_socket);
        }
    }
    return NULL;
}

// ======== Main Function =========
int main() {
    struct sockaddr_in server_addr;
    int opt = 1;

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
    struct epoll_event event;
    event.events = EPOLLIN;
    event.data.fd = server_socket;
    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_socket, &event) < 0) {
        perror("epoll_ctl server_socket failed");
        exit(1);
    }

    // Create worker threads
    pthread_t threads[WORKER_THREADS];
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
