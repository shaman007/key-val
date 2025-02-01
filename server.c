#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>         // for close()
#include <arpa/inet.h>      // for sockaddr_in and inet_ntoa
#include <ctype.h>

#define PORT 8080
#define MAX_CLIENTS 5
#define BUFFER_SIZE 1024
#define MAX_STORE 100

#define INITIAL_CAPACITY 101   // Initial bucket count; using a prime can be beneficial.
#define LOAD_FACTOR_THRESHOLD 0.75

// Node structure for the linked list in each bucket.
typedef struct Node {
    char *key;
    char *value;           // Now a string.
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
            return;
        }
    }

    // Key not found; create a new node and insert at the beginning.
    Node *new_node = create_node(key, value);
    new_node->next = head;
    table->buckets[index] = new_node;
    table->count++;
    return 1;
}

// Search for a key in the hash table. Returns the value string if found,
// or NULL if the key does not exist.
char *search(HashTable *table, const char *key) {
    unsigned long index = hash(key) % table->capacity;
    Node *node = table->buckets[index];
    while (node) {
        if (strcmp(node->key, key) == 0)
            return node->value;
        node = node->next;
    }
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
    for (size_t i = 0; i < table->capacity; i++) {
        if (table->buckets[i])
            free_list(table->buckets[i]);
    }
    free(table->buckets);
    free(table);
}

// Dump the contents of the hash table as a string.
char *dump_store(HashTable *table) {
    char *dump = malloc(MAX_STORE);
    if (!dump) {
        perror("Failed to allocate dump string");
        exit(EXIT_FAILURE);
    }
    dump[0] = '\0';  // Start with an empty string.

    for (size_t i = 0; i < table->capacity; i++) {
        Node *node = table->buckets[i];
        while (node) {
            char line[MAX_STORE];
            snprintf(line, sizeof(line), "%s: %s\n", node->key, node->value);
            strncat(dump, line, MAX_STORE - strlen(dump) - 1);
            node = node->next;
        }
    }

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

int main() {
    int server_fd, new_socket;
    struct sockaddr_in address;
    int opt = 1;
    int addrlen = sizeof(address);
    char buffer[BUFFER_SIZE];
    HashTable *table = create_table(INITIAL_CAPACITY);

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
        
        // Process commands from the connected client.
        while (1) {
            memset(buffer, 0, BUFFER_SIZE);
            int bytes_read = read(new_socket, buffer, BUFFER_SIZE - 1);
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
                            send(new_socket, response, strlen(response), 0);
                        } else {
                            const char *response = "Error: store full\n";
                            send(new_socket, response, strlen(response), 0);
                        }
                    } else {
                        const char *response = "Error: usage: write key value\n";
                        send(new_socket, response, strlen(response), 0);
                    }
                } else if (strcasecmp(command, "search") == 0) {
                    char *found = search(table, key);
                    if (found) {
                        char response[BUFFER_SIZE];
                        snprintf(response, sizeof(response), "Found: %s\n", found);
                        send(new_socket, response, strlen(response), 0);
                    } else {
                        const char *response = "Not found\n";
                        send(new_socket, response, strlen(response), 0);
                    }
                } else if (strcasecmp(command, "dump") == 0) {
                    char *dump = dump_store(table);
                    if (!dump) {
                        const char *response = "Error: failed to dump store\n";
                        send(new_socket, response, strlen(response), 0);
                        continue;
                    } else{
                       send(new_socket, dump, strlen(dump), 0);
                    }
                } else if (strcasecmp(command, "wipe") == 0) {
                    free_table(table);
                    table = create_table(INITIAL_CAPACITY);
                    const char *response = "All clean!\n";
                    send(new_socket, response, strlen(response), 0);
                } 
                else if (strcasecmp(command, "quit") == 0) {
                    const char *response = "Goodbye!\n";
                    send(new_socket, response, strlen(response), 0);
                    break;
                } else {
                    const char *response = "Error: unknown command. Use write or search.\n";
                    send(new_socket, response, strlen(response), 0);
                }
            } else {
                const char *response = "Error: invalid command format.\n";
                send(new_socket, response, strlen(response), 0);
            }
        }
        
        close(new_socket);
        printf("Connection closed\n");
    }
    
    // Close the server socket (unreachable in this loop, but good practice).
    close(server_fd);
    return 0;
}
