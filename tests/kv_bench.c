#define _GNU_SOURCE
#include <arpa/inet.h>
#include <errno.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

/* Same client engine for both protocols. Every reply is checked, including GET data.
 * Latency samples are batch completion times, not individual pipelined latencies. */
#define KEYS 10000
#define VALUE "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
static pthread_barrier_t ready;
static int redis_protocol, port, clients, operations, pipeline, workload;
static double seconds(void) {
    struct timespec t; clock_gettime(CLOCK_MONOTONIC, &t);
    return t.tv_sec + t.tv_nsec / 1e9;
}
static void fail(const char *what) { perror(what); exit(1); }
typedef struct { int fd; char data[65536]; size_t used, pos; } Reader;
static void bytes(Reader *r, char *out, size_t n) {
    while (n) {
        if (r->pos == r->used) {
            ssize_t got = recv(r->fd, r->data, sizeof(r->data), 0);
            if (got < 0 && errno == EINTR) continue;
            if (got <= 0) fail("recv/EOF");
            r->pos = 0; r->used = (size_t)got;
        }
        size_t take = r->used - r->pos;
        if (take > n) take = n;
        memcpy(out, r->data + r->pos, take);
        out += take; r->pos += take; n -= take;
    }
}
static void line(Reader *r, char *out, size_t cap) {
    for (size_t i = 0; i + 1 < cap; i++) {
        bytes(r, out + i, 1);
        if (out[i] == '\n') { out[i + 1] = 0; return; }
    }
    fprintf(stderr, "oversized reply\n"); exit(1);
}
static void reply(Reader *r, int get) {
    char b[1024]; line(r, b, sizeof(b));
    int valid;
    if (redis_protocol && get) {
        valid = strcmp(b, "$64\r\n") == 0;
        if (valid) { bytes(r, b, 66); valid = memcmp(b, VALUE "\r\n", 66) == 0; }
    } else if (get) {
        const char *prefix = "Found: " VALUE ", timestamp: ";
        char *end;
        valid = strncmp(b, prefix, strlen(prefix)) == 0;
        if (valid) {
            const char *start = b + strlen(prefix);
            long timestamp = strtol(start, &end, 10);
            valid = end != start && timestamp > 0 && strcmp(end, "\n") == 0;
        }
    } else valid = strcmp(b, redis_protocol ? "+OK\r\n" : "OK\n") == 0;
    if (!valid) { fprintf(stderr, "unexpected reply: %.100s\n", b); exit(1); }
}
static void send_all(int fd, const char *p, size_t n) {
    while (n) {
        ssize_t sent = send(fd, p, n, MSG_NOSIGNAL);
        if (sent < 0 && errno == EINTR) continue;
        if (sent <= 0) fail("send");
        p += sent; n -= (size_t)sent;
    }
}
typedef struct { int id; double *samples; size_t count; } Args;
static void *worker(void *arg) {
    Args *a = arg;
    int fd = socket(AF_INET, SOCK_STREAM, 0), one = 1;
    if (fd < 0) fail("socket");
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    struct timeval timeout = {.tv_sec = 20};
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
    struct sockaddr_in addr = {.sin_family=AF_INET, .sin_port=htons((uint16_t)port)};
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr))) fail("connect");
    Reader r = {.fd=fd};
    int n = operations / clients + (a->id < operations % clients);
    char *commands = malloc((size_t)n * 192);
    size_t *offsets = calloc((size_t)n + 1, sizeof(*offsets));
    unsigned char *gets = malloc((size_t)n);
    a->samples = malloc(((size_t)n / pipeline + 1) * sizeof(double));
    if (!commands || !offsets || !gets || !a->samples) fail("malloc");
    uint32_t random = 12345u + (uint32_t)a->id;
    for (int i = 0; i < n; i++) {
        random ^= random << 13; random ^= random >> 17; random ^= random << 5;
        unsigned key = workload == 3 ? (unsigned)i : random % KEYS;
        int get = workload == 1 || (workload == 2 && (random >> 16) % 2 == 0);
        gets[i] = (unsigned char)get;
        int len = snprintf(commands + offsets[i], 192, get ?
            (redis_protocol ? "*2\r\n$3\r\nGET\r\n$9\r\nk%08u\r\n" : "search k%08u\n") :
            (redis_protocol ? "*5\r\n$3\r\nSET\r\n$9\r\nk%08u\r\n$64\r\n" VALUE "\r\n$2\r\nEX\r\n$8\r\n31536000\r\n" : "write k%08u " VALUE " 31536000\n"), key);
        if (len < 0 || len >= 192) fail("command");
        offsets[i+1] = offsets[i] + (size_t)len;
    }
    pthread_barrier_wait(&ready);
    pthread_barrier_wait(&ready);
    for (int i = 0; i < n;) {
        int end = i + pipeline; if (end > n) end = n;
        double start = seconds();
        send_all(fd, commands + offsets[i], offsets[end] - offsets[i]);
        for (int j = i; j < end; j++) reply(&r, gets[j]);
        a->samples[a->count++] = (seconds() - start) * 1e6;
        i = end;
    }
    close(fd); free(commands); free(offsets); free(gets); return NULL;
}
static int compare(const void *a, const void *b) {
    double x=*(const double *)a, y=*(const double *)b; return (x>y)-(x<y);
}
int main(int argc, char **argv) {
    if (argc != 7) {
        fprintf(stderr, "usage: %s keyval|redis port clients operations pipeline set|get|mixed|load\n", argv[0]); return 1;
    }
    if (strcmp(argv[1], "redis") && strcmp(argv[1], "keyval")) return 1;
    redis_protocol = !strcmp(argv[1], "redis");
    port=atoi(argv[2]); clients=atoi(argv[3]); operations=atoi(argv[4]); pipeline=atoi(argv[5]);
    if (!strcmp(argv[6],"set")) workload=0;
    else if (!strcmp(argv[6],"get")) workload=1;
    else if (!strcmp(argv[6],"mixed")) workload=2;
    else if (!strcmp(argv[6],"load")) workload=3;
    else return 1;
    if (port<1 || port>65535 || clients<1 || clients>256 || operations<clients ||
        operations>100000000 || pipeline<1 || pipeline>4096 || (workload==3 && clients!=1)) return 1;
    pthread_t *threads=calloc((size_t)clients,sizeof(*threads));
    Args *args=calloc((size_t)clients,sizeof(*args));
    if (!threads || !args || pthread_barrier_init(&ready,NULL,(unsigned)clients+1)) fail("init");
    for (int i=0;i<clients;i++) {
        args[i].id=i;
        if (pthread_create(&threads[i],NULL,worker,&args[i])) fail("pthread_create");
    }
    /* Workers prepare commands and establish sockets before timing. */
    double start=seconds();
    pthread_barrier_wait(&ready);
    double running=seconds();
    pthread_barrier_wait(&ready);
    size_t count=0;
    for (int i=0;i<clients;i++) { pthread_join(threads[i],NULL); count+=args[i].count; }
    double elapsed=seconds()-running;
    double *samples=malloc(count*sizeof(*samples));
    if (!samples) fail("malloc");
    size_t pos=0;
    for(int i=0;i<clients;i++) {
        memcpy(samples+pos,args[i].samples,args[i].count*sizeof(*samples));
        pos+=args[i].count; free(args[i].samples);
    }
    qsort(samples,count,sizeof(*samples),compare);
    printf("{\"protocol\":\"%s\",\"clients\":%d,\"operations\":%d,\"pipeline\":%d,\"workload\":\"%s\",\"seconds\":%.6f,\"ops_per_second\":%.0f,\"batch_p50_us\":%.2f,\"batch_p95_us\":%.2f,\"batch_p99_us\":%.2f,\"setup_seconds\":%.6f,\"errors\":0}\n",
        argv[1],clients,operations,pipeline,argv[6],elapsed,operations/elapsed,
        samples[(count-1)*50/100],samples[(count-1)*95/100],samples[(count-1)*99/100],running-start);
    free(samples);free(args);free(threads);pthread_barrier_destroy(&ready);return 0;
}
