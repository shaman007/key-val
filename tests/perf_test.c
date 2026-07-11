#include <arpa/inet.h>
#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#define SERVER_IP "127.0.0.1"
#define SERVER_PORT 8080
#define LINE_SIZE 1024
#define IO_BUFFER_SIZE 65536

typedef struct {
    int id;
    int operations;
    int pipeline;
    int ok_count;
    int error_count;
} WorkerArgs;

static double now_seconds(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec / 1000000000.0;
}

static int connect_server(void) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return -1;
    }

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(SERVER_PORT);
    addr.sin_addr.s_addr = inet_addr(SERVER_IP);

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(fd);
        return -1;
    }

    return fd;
}

static void *run_worker(void *arg) {
    WorkerArgs *args = arg;
    int fd = connect_server();
    char command[LINE_SIZE];
    char send_buffer[IO_BUFFER_SIZE];
    char recv_buffer[IO_BUFFER_SIZE];
    size_t send_used = 0;
    size_t recv_used = 0;
    int sent_ops = 0;
    int received_ops = 0;

    if (fd < 0) {
        args->error_count = args->operations;
        return NULL;
    }

    while (received_ops < args->operations) {
        while (sent_ops < args->operations && sent_ops - received_ops < args->pipeline) {
            int len = snprintf(command, sizeof(command), "write perf-%d-%d value-%d 60\n", args->id, sent_ops, sent_ops);
            if (len < 0 || (size_t)len >= sizeof(command)) {
                args->error_count++;
                sent_ops++;
                continue;
            }

            if (send_used + (size_t)len > sizeof(send_buffer)) {
                size_t sent = 0;
                while (sent < send_used) {
                    ssize_t n = send(fd, send_buffer + sent, send_used - sent, 0);
                    if (n < 0) {
                        if (errno == EINTR) {
                            continue;
                        }
                        close(fd);
                        args->error_count += args->operations - received_ops;
                        return NULL;
                    }
                    sent += (size_t)n;
                }
                send_used = 0;
            }

            memcpy(send_buffer + send_used, command, (size_t)len);
            send_used += (size_t)len;
            sent_ops++;
        }

        if (send_used > 0) {
            size_t sent = 0;
            while (sent < send_used) {
                ssize_t n = send(fd, send_buffer + sent, send_used - sent, 0);
                if (n < 0) {
                    if (errno == EINTR) {
                        continue;
                    }
                    close(fd);
                    args->error_count += args->operations - received_ops;
                    return NULL;
                }
                sent += (size_t)n;
            }
            send_used = 0;
        }

        ssize_t n = recv(fd, recv_buffer + recv_used, sizeof(recv_buffer) - recv_used, 0);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            close(fd);
            args->error_count += args->operations - received_ops;
            return NULL;
        }
        if (n == 0) {
            close(fd);
            args->error_count += args->operations - received_ops;
            return NULL;
        }

        recv_used += (size_t)n;
        size_t consumed = 0;
        while (1) {
            void *newline = memchr(recv_buffer + consumed, '\n', recv_used - consumed);
            if (!newline) {
                break;
            }

            size_t line_len = (char *)newline - (recv_buffer + consumed) + 1;
            if (line_len == 3 && memcmp(recv_buffer + consumed, "OK\n", 3) == 0) {
                args->ok_count++;
            } else {
                args->error_count++;
            }
            received_ops++;
            consumed += line_len;
        }

        if (consumed > 0) {
            memmove(recv_buffer, recv_buffer + consumed, recv_used - consumed);
            recv_used -= consumed;
        }

        if (recv_used == sizeof(recv_buffer)) {
            close(fd);
            args->error_count += args->operations - received_ops;
            return NULL;
        }
    }

    close(fd);
    return NULL;
}

int main(int argc, char **argv) {
    int clients = 8;
    int operations_per_client = 1000;
    int pipeline = 64;

    if (argc >= 2) {
        clients = atoi(argv[1]);
    }
    if (argc >= 3) {
        operations_per_client = atoi(argv[2]);
    }
    if (argc >= 4) {
        pipeline = atoi(argv[3]);
    }
    if (clients <= 0 || operations_per_client <= 0 || pipeline <= 0) {
        fprintf(stderr, "usage: %s [clients] [operations-per-client] [pipeline]\n", argv[0]);
        return EXIT_FAILURE;
    }

    pthread_t *threads = calloc((size_t)clients, sizeof(*threads));
    WorkerArgs *args = calloc((size_t)clients, sizeof(*args));
    if (!threads || !args) {
        free(threads);
        free(args);
        return EXIT_FAILURE;
    }

    double started = now_seconds();
    for (int i = 0; i < clients; i++) {
        args[i].id = i;
        args[i].operations = operations_per_client;
        args[i].pipeline = pipeline;
        if (pthread_create(&threads[i], NULL, run_worker, &args[i]) != 0) {
            perror("pthread_create");
            free(threads);
            free(args);
            return EXIT_FAILURE;
        }
    }

    int ok_total = 0;
    int error_total = 0;
    for (int i = 0; i < clients; i++) {
        pthread_join(threads[i], NULL);
        ok_total += args[i].ok_count;
        error_total += args[i].error_count;
    }
    double elapsed = now_seconds() - started;
    int total = clients * operations_per_client;

    printf("clients=%d\n", clients);
    printf("operations_per_client=%d\n", operations_per_client);
    printf("pipeline=%d\n", pipeline);
    printf("total_operations=%d\n", total);
    printf("ok=%d\n", ok_total);
    printf("errors=%d\n", error_total);
    printf("elapsed_seconds=%.3f\n", elapsed);
    printf("ops_per_second=%.0f\n", elapsed > 0.0 ? (double)ok_total / elapsed : 0.0);

    free(threads);
    free(args);
    return error_total == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}
