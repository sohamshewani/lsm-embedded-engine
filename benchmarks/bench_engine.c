#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <time.h>
#include <inttypes.h>
#include <unistd.h>
#include "lsm.h"

#define NUM_OPS 100000
#define WARMUP_OPS 10000
#define DB_FILE "bench_test.lsm"

static uint64_t get_time_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static int compare_uint64(const void *a, const void *b) {
    uint64_t fa = *(const uint64_t *)a;
    uint64_t fb = *(const uint64_t *)b;
    return (fa > fb) - (fa < fb);
}

static void run_benchmark(const char *op_name, uint64_t *latencies, size_t count, uint64_t total_ns) {
    double seconds = (double)total_ns / 1000000000.0;
    double ops_sec = (double)count / seconds;

    qsort(latencies, count, sizeof(uint64_t), compare_uint64);

    uint64_t p50 = latencies[(size_t)(count * 0.50)];
    uint64_t p99 = latencies[(size_t)(count * 0.99)];
    uint64_t p999 = latencies[(size_t)(count * 0.999)];

    printf("| %-12s | %12.2f | %10.2ns | %10.2ns | %11.2ns |\n",
           op_name, ops_sec, (double)p50, (double)p99, (double)p999);
}

int main(void) {
    lsm_db *db = NULL;
    int rc;

    unlink(DB_FILE);

    rc = lsm_new(NULL, &db);
    if (rc != LSM_OK) {
        fprintf(stderr, "lsm_new failed: %d\n", rc);
        return 1;
    }

    rc = lsm_open(db, DB_FILE);
    if (rc != LSM_OK) {
        fprintf(stderr, "lsm_open failed: %d\n", rc);
        lsm_close(db);
        return 1;
    }

    uint64_t *latencies = malloc(sizeof(uint64_t) * NUM_OPS);
    if (!latencies) {
        perror("malloc");
        lsm_close(db);
        return 1;
    }

    char key_buf[32];
    char val_buf[64];

    // Warmup Writes
    for (int i = 0; i < WARMUP_OPS; i++) {
        int klen = snprintf(key_buf, sizeof(key_buf), "key_%08d", i);
        int vlen = snprintf(val_buf, sizeof(val_buf), "val_%08d", i);
        lsm_insert(db, key_buf, klen, val_buf, vlen);
    }

    // Benchmark Writes (Inserts)
    uint64_t start_total = get_time_ns();
    for (int i = 0; i < NUM_OPS; i++) {
        int klen = snprintf(key_buf, sizeof(key_buf), "key_bench_%08d", i);
        int vlen = snprintf(val_buf, sizeof(val_buf), "val_bench_%08d", i);

        uint64_t start_op = get_time_ns();
        rc = lsm_insert(db, key_buf, klen, val_buf, vlen);
        uint64_t end_op = get_time_ns();

        if (rc != LSM_OK) {
            fprintf(stderr, "lsm_insert failed at %d: %d\n", i, rc);
        }
        latencies[i] = end_op - start_op;
    }
    uint64_t end_total = get_time_ns();

    printf("\n=== LSM-Embedded Engine Microbenchmark ===\n");
    printf("+--------------+--------------+------------+------------+-------------+\n");
    printf("| Operation    |     Ops/sec  |   p50 Lat  |   p99 Lat  |  p99.9 Lat  |\n");
    printf("+--------------+--------------+------------+------------+-------------+\n");
    
    run_benchmark("Write", latencies, NUM_OPS, end_total - start_total);

    // Benchmark Reads (Lookups)
    start_total = get_time_ns();
    for (int i = 0; i < NUM_OPS; i++) {
        int klen = snprintf(key_buf, sizeof(key_buf), "key_bench_%08d", i);
        const void *val_out = NULL;
        int vlen_out = 0;

        uint64_t start_op = get_time_ns();
        rc = lsm_fetch(db, key_buf, klen, &val_out, &vlen_out);
        uint64_t end_op = get_time_ns();

        if (rc != LSM_OK && rc != LSM_NOTFOUND) {
            fprintf(stderr, "lsm_fetch failed at %d: %d\n", i, rc);
        }
        latencies[i] = end_op - start_op;
    }
    end_total = get_time_ns();

    run_benchmark("Read", latencies, NUM_OPS, end_total - start_total);
    printf("+--------------+--------------+------------+------------+-------------+\n\n");

    free(latencies);
    lsm_close(db);
    unlink(DB_FILE);

    return 0;
}