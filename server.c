#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>         // for close()
#include <arpa/inet.h>      // for sockaddr_in and inet_ntoa
#include <ctype.h>
#include <pthread.h>        // for pthreads

#define PORT 8080
#define MAX_CLIENTS 521
#define BUFFER_SIZE 1024
#define INITIAL_CAPACITY 101   // Initial bucket count; using a prime can be beneficial.
#define LOAD_FACTOR_THRESHOLD 0.75
pthread_mutex_t store_mutex = PTHREAD_MUTEX_INITIALIZER;
// Node structure for the linked list in each bucket.
typedef struct Node {
    char *key;
    char *value;
    time_t created_at;  // Store creation time
    struct Node *next;
} Node;

// Hash table structure.
typedef struct HashTable {
    Node **buckets;
    size_t capacity;       // Total number of buckets.
    size_t count;          // Number of key-value pairs stored.
} HashTable;

// djb2 hash function by Dan Bernstein.
unsigned long hash(const char *str) {
    unsigned long hash = 5381;
    int c;
    while ((c = *str++))
        hash = ((hash << 5) + hash) + c; // hash * 33 + c
    return hash;
}

// Create a new hash table.
HashTable *create_table(size_t capacity) {
    HashTable *table = malloc(sizeof(HashTable));
    if (!table) {
        perror("Failed to allocate hash table");
        exit(EXIT_FAILURE);
    }
    table->capacity = capacity;
    table->count = 0;
    table->buckets = calloc(table->capacity, sizeof(Node *));
    if (!table->buckets) {
        perror("Failed to allocate buckets");
        free(table);
        exit(EXIT_FAILURE);
    }
    return table;
}

// Create a new node with key and value.
Node *create_node(const char *key, const char *value) {
    Node *new_node = malloc(sizeof(Node));
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
    new_node->next = NULL;
    return new_node;
}

// Forward declaration for rehashing.
void resize_table(HashTable *table);

// Insert a key-value pair into the hash table.
int insert(HashTable *table, const char *key, const char *value) {
    pthread_mutex_lock(&store_mutex);
    // Check if we need to resize before inserting.
    double load_factor = (double)table->count / table->capacity;
    if (load_factor > LOAD_FACTOR_THRESHOLD) {
        resize_table(table);
    }

    unsigned long index = hash(key) % table->capacity;
    Node *head = table->buckets[index];

    // Check if the key already exists; if so, update its value.
    for (Node *curr = head; curr != NULL; curr = curr->next) {
        if (strcmp(curr->key, key) == 0) {
            // Replace the existing value.
            free(curr->value);
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
    table->buckets[index] = new_node;
    table->count++;
    pthread_mutex_unlock(&store_mutex);
    return 1;
}

// Search for a key in the hash table. Returns the value string if found,
// or NULL if the key does not exist.
Node *search(HashTable *table, const char *key) {
    pthread_mutex_lock(&store_mutex);
    unsigned long index = hash(key) % table->capacity;
    printf("Index: %ld\n", index);
    Node *node = table->buckets[index];
    while (node) {
        if (strcmp(node->key, key) == 0)
            pthread_mutex_unlock(&store_mutex);
            return node;
        node = node->next;
    }
    pthread_mutex_unlock(&store_mutex);
    return NULL;
}

// Resize the hash table to a new capacity.
void resize_table(HashTable *table) {
    size_t old_capacity = table->capacity;
    size_t new_capacity = old_capacity * 2 + 1;  // Example growth strategy.
    Node **new_buckets = calloc(new_capacity, sizeof(Node *));
    if (!new_buckets) {
        perror("Failed to allocate new buckets during resize");
        exit(EXIT_FAILURE);
    }

    // Rehash all existing entries into the new bucket array.
    for (size_t i = 0; i < old_capacity; i++) {
        Node *node = table->buckets[i];
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
    free(table->buckets);
    table->buckets = new_buckets;
    table->capacity = new_capacity;

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
void free_table(HashTable *table) {
    pthread_mutex_lock(&store_mutex);
    for (size_t i = 0; i < table->capacity; i++) {
        if (table->buckets[i])
            free_list(table->buckets[i]);
    }
    free(table->buckets);
    free(table);
    pthread_mutex_unlock(&store_mutex);
}

// Dump the contents of the hash table as a string.
char *dump_store(HashTable *table) {
    pthread_mutex_lock(&store_mutex);
    char *dump = malloc(sizeof(Node)* table->count + 1);
    if (!dump) {
        perror("Failed to allocate dump string");
        exit(EXIT_FAILURE);
    }
    dump[0] = '\0';  // Start with an empty string.

    for (size_t i = 0; i < table->capacity; i++) {
        Node *node = table->buckets[i];
        while (node) {
            char line[sizeof(node->key) + sizeof(node->value) + 32];
            snprintf(line, sizeof(line), "%s: %s, %ld\n", node->key, node->value, node->created_at);
            strncat(dump, line, strlen(line));
            node = node->next;
        }
    }
    pthread_mutex_unlock(&store_mutex);
    return dump;
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

typedef struct {
    int client_socket;
    HashTable *table; 
} client_info;

void* client_handler(void* arg) {    
    client_info *info = (client_info *)arg;
    printf("Client handler started\n");
    int client_socket = info->client_socket;
    printf("Client socket: %d\n", client_socket);
    HashTable *table = info->table;
    char buffer[BUFFER_SIZE];
        // Process commands from the connected client.
        while (1) {
            memset(buffer, 0, BUFFER_SIZE);
            int bytes_read = read(client_socket, buffer, BUFFER_SIZE - 1);
            if (bytes_read <= 0) {
                // Connection closed or error.
                break;
            }
            
            buffer[bytes_read] = '\0';
            trim_newline(buffer);  // Remove newline if present.
            
            // We expect commands like:
            //   write key value
            //   search key
            //   dump
            //   quit
            //   wipe
            char command[16], key[256], value[768];
            int num_tokens = sscanf(buffer, "%15s %255s %767[^\n]", command, key, value);
            
            if (num_tokens >= 1) {
                // Compare commands case-insensitively.
                if (strcasecmp(command, "write") == 0) {
                    if (num_tokens == 3) {
                        if (insert(table, key, value)) {
                            const char *response = "OK\n";
                            send(client_socket, response, strlen(response), 0);
                        } else {
                            const char *response = "Error: store full\n";
                            send(client_socket, response, strlen(response), 0);
                        }
                    } else {
                        const char *response = "Error: usage: write key value\n";
                        send(client_socket, response, strlen(response), 0);
                    }
                } else if (strcasecmp(command, "search") == 0) {
                    Node *found = search(table, key);
                    if (found) {
                        char response[BUFFER_SIZE];
                        snprintf(response, sizeof(response), "Found: %s, timestamp: %ld\n", found->value, found->created_at);
                        send(client_socket, response, strlen(response), 0);
                    } else {
                        const char *response = "Not found\n";
                        send(client_socket, response, strlen(response), 0);
                    }
                } else if (strcasecmp(command, "dump") == 0) {
                    char *dump = dump_store(table);
                    if (!dump) {
                        const char *response = "Error: failed to dump store\n";
                        send(client_socket, response, strlen(response), 0);
                        continue;
                    } else{
                       send(client_socket, dump, strlen(dump), 0);
                    }
                } else if (strcasecmp(command, "wipe") == 0) {
                    free_table(table);
                    table = create_table(INITIAL_CAPACITY);
                    const char *response = "All clean!\n";
                    send(client_socket, response, strlen(response), 0);
                } 
                else if (strcasecmp(command, "quit") == 0) {
                    const char *response = "Goodbye!\n";
                    send(client_socket, response, strlen(response), 0);
                    close(client_socket);
                    break;
                } else {
                    const char *response = "Error: unknown command. Use write, search, dump, wipe or quit.\n";
                    send(client_socket, response, strlen(response), 0);
                }
            } else {
                const char *response = "Error: invalid command format.\n";
                send(client_socket, response, strlen(response), 0);
            }
        }
}

int main() {
    int server_fd, new_socket;
    struct sockaddr_in address;
    int opt = 1;
    int addrlen = sizeof(address);
    char buffer[BUFFER_SIZE];
    HashTable *table = create_table(INITIAL_CAPACITY);
    client_info *info = malloc(sizeof(client_info));
    info->table = table;
    // Create socket file descriptor.
    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
        perror("socket failed");
        exit(EXIT_FAILURE);
    }
    
    // Allow reuse of the address and port.
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT,
                   &opt, sizeof(opt))) {
        perror("setsockopt");
        close(server_fd);
        exit(EXIT_FAILURE);
    }
    
    // Prepare the sockaddr_in structure.
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;  // Listen on all interfaces.
    address.sin_port = htons(PORT);
    
    // Bind the socket.
    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
        perror("bind failed");
        close(server_fd);
        exit(EXIT_FAILURE);
    }
    
    // Listen for incoming connections.
    if (listen(server_fd, MAX_CLIENTS) < 0) {
        perror("listen");
        close(server_fd);
        exit(EXIT_FAILURE);
    }
    
    printf("Server listening on port %d\n", PORT);
    
    // Server loop: accept and handle client connections.
    while (1) {
        if ((new_socket = accept(server_fd, (struct sockaddr *)&address,
                                 (socklen_t*)&addrlen)) < 0) {
            perror("accept");
            continue;
        }
        printf("Accepted connection from %s:%d\n", 
               inet_ntoa(address.sin_addr), ntohs(address.sin_port));
        info->client_socket = new_socket;
        pthread_t tid;
        if (pthread_create(&tid, NULL, client_handler, info) != 0) {
            perror("pthread_create");
            close(new_socket);
        } else {
            // Detach the thread so that resources are freed when it terminates.
            pthread_detach(tid);
        }       
        printf("Connection closed\n");
    }
    // Close the server socket (unreachable in this loop, but good practice).
    close(server_fd);
    return 0;
}
