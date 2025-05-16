/*
 * epoll_server.c
 *
 * A simple scalable TCP server using epoll and pthreads.
 * Listens on port 8080. For each connection, when data is read and a newline is seen,
 * it sends back "OK\n" and closes the connection.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <pthread.h>

#define PORT 8080
#define MAX_EVENTS 64
#define WORKER_COUNT 4

/* Per-connection structure to hold partial read data. */
typedef struct {
    int fd;
    char buf[1024];
    int buf_used;
} connection_t;

/* Worker thread structure */
typedef struct {
    int epoll_fd;
    pthread_t thread;
} worker_t;

worker_t workers[WORKER_COUNT];
int next_worker = 0;  /* Used for round–robin assignment */

/* Set a file descriptor to non–blocking mode */
int set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1)
        return -1;
    flags |= O_NONBLOCK;
    return fcntl(fd, F_SETFL, flags);
}

/* Worker thread: each thread runs an epoll_wait loop on its own epoll_fd.
 * For each connection event, it reads all available data (because we use edge–triggered),
 * and if a newline is encountered in the data, sends "OK\n" and closes the connection.
 */
void *worker_thread(void *arg) {
    worker_t *worker = (worker_t *)arg;
    struct epoll_event events[MAX_EVENTS];

    while (1) {
        int n = epoll_wait(worker->epoll_fd, events, MAX_EVENTS, -1);
        if (n < 0) {
            if (errno == EINTR)
                continue;
            perror("epoll_wait");
            break;
        }
        for (int i = 0; i < n; i++) {
            connection_t *conn = (connection_t *) events[i].data.ptr;
            int close_conn = 0;

            /* Read as much as possible */
            while (1) {
                ssize_t count = recv(conn->fd, conn->buf + conn->buf_used,
                                     sizeof(conn->buf) - conn->buf_used - 1, 0);
                if (count == -1) {
                    if (errno == EAGAIN || errno == EWOULDBLOCK) {
                        /* No more data to read */
                        break;
                    }
                    perror("recv");
                    close_conn = 1;
                    break;
                } else if (count == 0) {
                    /* Client closed connection */
                    close_conn = 1;
                    break;
                }
                conn->buf_used += count;
                conn->buf[conn->buf_used] = '\0';

                /* Check if we have received a newline */
                if (strchr(conn->buf, '\n') != NULL) {
                    const char *response = "OK\n";
                    ssize_t sent = send(conn->fd, response, strlen(response), 0);
                    if (sent == -1)
                        perror("send");
                    close_conn = 1;
                    break;
                }

                /* Prevent buffer overflow */
                if (conn->buf_used == sizeof(conn->buf) - 1) {
                    close_conn = 1;
                    break;
                }
            }

            if (close_conn) {
                if (epoll_ctl(worker->epoll_fd, EPOLL_CTL_DEL, conn->fd, NULL) == -1)
                    perror("epoll_ctl DEL");
                close(conn->fd);
                free(conn);
            }
        }
    }
    return NULL;
}

int main(void) {
    int listen_fd, s;
    struct sockaddr_in addr;

    /* Create the listening socket */
    listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd == -1) {
        perror("socket");
        exit(EXIT_FAILURE);
    }

    /* Set non–blocking */
    if (set_nonblocking(listen_fd) == -1) {
        perror("set_nonblocking");
        exit(EXIT_FAILURE);
    }

    /* Allow reuse of the address */
    int opt = 1;
    if (setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        perror("setsockopt");
        exit(EXIT_FAILURE);
    }

    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY; // Bind to all interfaces
    addr.sin_port = htons(PORT);

    if (bind(listen_fd, (struct sockaddr *)&addr, sizeof(addr)) == -1) {
        perror("bind");
        exit(EXIT_FAILURE);
    }

    if (listen(listen_fd, SOMAXCONN) == -1) {
        perror("listen");
        exit(EXIT_FAILURE);
    }

    /* Create an epoll instance for the main thread to watch the listening socket */
    int epoll_fd = epoll_create1(0);
    if (epoll_fd == -1) {
        perror("epoll_create1");
        exit(EXIT_FAILURE);
    }

    struct epoll_event event;
    event.data.fd = listen_fd;
    event.events = EPOLLIN | EPOLLET;  // Use edge-triggered mode
    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, listen_fd, &event) == -1) {
        perror("epoll_ctl");
        exit(EXIT_FAILURE);
    }

    /* Create worker threads */
    for (int i = 0; i < WORKER_COUNT; i++) {
        workers[i].epoll_fd = epoll_create1(0);
        if (workers[i].epoll_fd == -1) {
            perror("epoll_create1 (worker)");
            exit(EXIT_FAILURE);
        }
        if (pthread_create(&workers[i].thread, NULL, worker_thread, &workers[i]) != 0) {
            perror("pthread_create");
            exit(EXIT_FAILURE);
        }
    }

    /* Main loop: wait for events on the listening socket and accept new connections. */
    struct epoll_event events[MAX_EVENTS];
    while (1) {
        int n = epoll_wait(epoll_fd, events, MAX_EVENTS, -1);
        if (n < 0) {
            if (errno == EINTR)
                continue;
            perror("epoll_wait");
            break;
        }
        for (int i = 0; i < n; i++) {
            if (events[i].data.fd == listen_fd) {
                /* Accept as many incoming connections as possible */
                while (1) {
                    struct sockaddr_in client_addr;
                    socklen_t client_len = sizeof(client_addr);
                    int conn_fd = accept(listen_fd,
                                         (struct sockaddr *)&client_addr,
                                         &client_len);
                    if (conn_fd == -1) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK)
                            break;  // No more incoming connections.
                        else {
                            perror("accept");
                            break;
                        }
                    }

                    if (set_nonblocking(conn_fd) == -1) {
                        perror("set_nonblocking (conn_fd)");
                        close(conn_fd);
                        continue;
                    }

                    /* Allocate and initialize a connection structure */
                    connection_t *conn = malloc(sizeof(connection_t));
                    if (!conn) {
                        perror("malloc");
                        close(conn_fd);
                        continue;
                    }
                    conn->fd = conn_fd;
                    conn->buf_used = 0;

                    /* Round-robin assignment to a worker thread */
                    int worker_index = next_worker;
                    next_worker = (next_worker + 1) % WORKER_COUNT;

                    struct epoll_event conn_event;
                    conn_event.data.ptr = conn;
                    conn_event.events = EPOLLIN | EPOLLET;
                    s = epoll_ctl(workers[worker_index].epoll_fd,
                                  EPOLL_CTL_ADD, conn_fd, &conn_event);
                    if (s == -1) {
                        perror("epoll_ctl ADD (conn)");
                        close(conn_fd);
                        free(conn);
                        continue;
                    }
                }
            }
        }
    }

    close(listen_fd);
    return EXIT_SUCCESS;
}
