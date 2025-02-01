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

// A simple structure to hold a key-value pair.
typedef struct {
    char key[256];
    char value[768];
} kv_pair;

// Global store and its count.
kv_pair store[MAX_STORE];
int store_count = 0;

// Function to add/update a key-value pair.
int set_value(const char *key, const char *value) {
    // Update if key already exists.
    for (int i = 0; i < store_count; i++) {
        if (strcmp(store[i].key, key) == 0) {
            strncpy(store[i].value, value, sizeof(store[i].value) - 1);
            store[i].value[sizeof(store[i].value) - 1] = '\0';
            return 1; // Updated
        }
    }
    // Insert new pair if there is space.
    if (store_count < MAX_STORE) {
        strncpy(store[store_count].key, key, sizeof(store[store_count].key) - 1);
        store[store_count].key[sizeof(store[store_count].key) - 1] = '\0';
        strncpy(store[store_count].value, value, sizeof(store[store_count].value) - 1);
        store[store_count].value[sizeof(store[store_count].value) - 1] = '\0';
        store_count++;
        return 1; // Inserted
    }
    return 0; // Store full
}

// Function to retrieve a value by key.
char* get_value(const char *key) {
    for (int i = 0; i < store_count; i++) {
        if (strcmp(store[i].key, key) == 0) {
            return store[i].value;
        }
    }
    return NULL;
}

char* dump_store() {
    char *dump = malloc(MAX_STORE * (256 + 768));
    if (dump == NULL) {
        return NULL;
    }
    dump[0] = '\0';
    for (int i = 0; i < store_count; i++) {
        char line[1024];
        snprintf(line, sizeof(line), "%s: %s\n", store[i].key, store[i].value);
        strncat(dump, line, MAX_STORE * (256 + 768) - strlen(dump) - 1);
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
            char command[16], key[256], value[768];
            int num_tokens = sscanf(buffer, "%15s %255s %767[^\n]", command, key, value);
            
            if (num_tokens >= 1) {
                // Compare commands case-insensitively.
                if (strcasecmp(command, "write") == 0) {
                    if (num_tokens == 3) {
                        if (set_value(key, value)) {
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
                    char *found = get_value(key);
                    if (found) {
                        char response[BUFFER_SIZE];
                        snprintf(response, sizeof(response), "Found: %s\n", found);
                        send(new_socket, response, strlen(response), 0);
                    } else {
                        const char *response = "Not found\n";
                        send(new_socket, response, strlen(response), 0);
                    }
                } else if (strcasecmp(command, "dump") == 0) {
                    char *dump = dump_store();
                    send(new_socket, dump, strlen(dump), 0);
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
