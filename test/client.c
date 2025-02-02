#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <uuid/uuid.h>
#include <time.h>
#include <unistd.h>
#define SERVER_IP "127.0.0.1"
#define SERVER_PORT 8080
#define BUFFER_SIZE 1024

void generate_random_string(char *str, size_t length) {
    static const char charset[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    for (size_t i = 0; i < length; i++) {
        str[i] = charset[rand() % (sizeof(charset) - 1)];
    }
    str[length] = '\0';
}

void send_command(int sockfd, const char *command) {
    char response[BUFFER_SIZE];
    printf("Sending121212: %s\n", command);
    send(sockfd, command, strlen(command), 0);
    //send(sockfd, "\n", 1, 0);
    
    // Read server response
    ssize_t bytes_received = recv(sockfd, response, BUFFER_SIZE - 1, 0);
    printf("Bytes received: %ld\n", bytes_received);
    if (bytes_received > 0) {
        response[bytes_received] = '\0';
        printf("Server response: %s\n", response);
    }
}

void run_sequence(int sockfd, int num_writes) {
    char buffer[BUFFER_SIZE];
    uuid_t uuid;
    char uuid_str[37];

    for (int i = 0; i < num_writes; i++) {
        uuid_generate(uuid);
        uuid_unparse(uuid, uuid_str);

        int str_len = (rand() % 64) + 1;
        char random_string[str_len + 1];
        generate_random_string(random_string, str_len);

        snprintf(buffer, strlen(random_string)+strlen(uuid_str) + 128, "write %s %s", uuid_str, random_string);
        printf("Sending: %s\n", buffer);
        send_command(sockfd, buffer);
    }

}

int main(int argc, char *argv[]) {
    if (argc != 2) {
        fprintf(stderr, "Usage: %s <num_writes>\n", argv[0]);
        return EXIT_FAILURE;
    }

    int num_writes = atoi(argv[1]);
    if (num_writes <= 0) {
        fprintf(stderr, "Invalid number of writes. Must be positive.\n");
        return EXIT_FAILURE;
    }

    srand(time(NULL));
    
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        perror("Socket creation failed");
        return EXIT_FAILURE;
    }

    struct sockaddr_in server_addr;
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(SERVER_PORT);
    server_addr.sin_addr.s_addr = inet_addr(SERVER_IP);

    if (connect(sockfd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        perror("Connection failed");
        close(sockfd);
        return EXIT_FAILURE;
    }

    // First run with wipe
    run_sequence(sockfd, num_writes);
    close(sockfd);
    return EXIT_SUCCESS;
}