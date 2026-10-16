#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/stat.h>

#define LSM_WAL_MAGIC        0x4C534D57 // "LSMW"
#define LSM_WAL_VERSION      1
#define LSM_WAL_HEADER_SIZE  24
#define LSM_WAL_BLOCK_SIZE   4096
#define LSM_RING_BUFFER_SIZE 1024

typedef enum {
    LSM_WAL_TYPE_PUT    = 1,
    LSM_WAL_TYPE_DELETE = 2
} lsm_wal_record_type_t;

typedef struct __attribute__((__packed__)) {
    uint32_t magic;
    uint32_t version;
    uint32_t type;
    uint32_t payload_len;
    uint64_t lsn;
    uint32_t crc32c;
} lsm_wal_header_t;

typedef struct {
    uint32_t key_len;
    uint32_t val_len;
    const char *key;
    const char *val;
    uint64_t lsn;
    lsm_wal_record_type_t type;
} lsm_wal_entry_t;

typedef struct {
    lsm_wal_entry_t entries[LSM_RING_BUFFER_SIZE];
    volatile uint64_t head;
    volatile uint64_t tail;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
} lsm_wal_ring_buffer_t;

struct lsm_wal {
    int fd;
    char *filepath;
    pthread_t worker_thread;
    volatile int worker_running;
    lsm_wal_ring_buffer_t ring_buffer;
    pthread_mutex_t append_mutex;
};

static uint32_t lsm_crc32c(uint32_t crc, const unsigned char *buf, size_t len) {
    // Castagnoli CRC32C implementation
    static uint32_t table[256];
    static int table_computed = 0;
    if (!table_computed) {
        for (int i = 0; i < 256; i++) {
            uint32_t c = i;
            for (int j = 0; j < 8; j++) {
                if (c & 1) {
                    c = 0x82F63B78 ^ (c >> 1);
                } else {
                    c = c >> 1;
                }
            }
            table[i] = c;
        }
        table_computed = 1;
    }
    crc = crc ^ 0xFFFFFFFF;
    for (size_t i = 0; i < len; i++) {
        crc = table[(crc ^ buf[i]) & 0xFF] ^ (crc >> 8);
    }
    return crc ^ 0xFFFFFFFF;
}

static void *lsm_wal_writer_thread(void *arg) {
    lsm_wal_t *wal = (lsm_wal_t *)arg;
    while (1) {
        pthread_mutex_lock(&wal->ring_buffer.mutex);
        while (wal->ring_buffer.head == wal->ring_buffer.tail && wal->worker_running) {
            pthread_cond_wait(&wal->ring_buffer.cond, &wal->ring_buffer.mutex);
        }

        if (!wal->worker_running && wal->ring_buffer.head == wal->ring_buffer.tail) {
            pthread_mutex_unlock(&wal->ring_buffer.mutex);
            break;
        }

        uint64_t head = wal->ring_buffer.head;
        uint64_t tail = wal->ring_buffer.tail;
        pthread_mutex_unlock(&wal->ring_buffer.mutex);

        if (head == tail) continue;

        // Batch group commit
        uint64_t batch_size = (tail > head) ? (tail - head) : (LSM_RING_BUFFER_SIZE - head);
        if (batch_size == 0) continue;

        for (uint64_t i = 0; i < batch_size; i++) {
            uint64_t idx = (head + i) % LSM_RING_BUFFER_SIZE;
            lsm_wal_entry_t *entry = &wal->ring_buffer.entries[idx];

            uint32_t payload_len = sizeof(uint32_t) + entry->key_len + sizeof(uint32_t) + entry->val_len;
            char *buf = malloc(LSM_WAL_HEADER_SIZE + payload_len);
            if (!buf) continue;

            lsm_wal_header_t header;
            header.magic = LSM_WAL_MAGIC;
            header.version = LSM_WAL_VERSION;
            header.type = (uint32_t)entry->type;
            header.payload_len = payload_len;
            header.lsn = entry->lsn;

            char *payload_ptr = buf + LSM_WAL_HEADER_SIZE;
            *(uint32_t *)payload_ptr = entry->key_len;
            memcpy(payload_ptr + sizeof(uint32_t), entry->key, entry->key_len);
            char *val_len_ptr = payload_ptr + sizeof(uint32_t) + entry->key_len;
            *(uint32_t *)val_len_ptr = entry->val_len;
            memcpy(val_len_ptr + sizeof(uint32_t), entry->val, entry->val_len);

            header.crc32c = lsm_crc32c(0, (unsigned char *)payload_ptr, payload_len);
            memcpy(buf, &header, LSM_WAL_HEADER_SIZE);

            ssize_t written = write(wal->fd, buf, LSM_WAL_HEADER_SIZE + payload_len);
            free(buf);
            if (written < 0) {
                // Handle write error critically
                perror("lsm_wal_writer_thread write error");
            }
        }

        fsync(wal->fd);

        pthread_mutex_lock(&wal->ring_buffer.mutex);
        wal->ring_buffer.head = (head + batch_size) % LSM_RING_BUFFER_SIZE;
        pthread_mutex_unlock(&wal->ring_buffer.mutex);
    }
    return NULL;
}

lsm_wal_t *lsm_wal_open(const char *filepath) {
    lsm_wal_t *wal = malloc(sizeof(lsm_wal_t));
    if (!wal) return NULL;

    wal->filepath = strdup(filepath);
    if (!wal->filepath) {
        free(wal);
        return NULL;
    }

    wal->fd = open(filepath, O_CREAT | O_RDWR | O_APPEND, 0644);
    if (wal->fd < 0) {
        free(wal->filepath);
        free(wal);
        return NULL;
    }

    wal->ring_buffer.head = 0;
    wal->ring_buffer.tail = 0;
    pthread_mutex_init(&wal->ring_buffer.mutex, NULL);
    pthread_cond_init(&wal->ring_buffer.cond, NULL);
    pthread_mutex_init(&wal->append_mutex, NULL);

    wal->worker_running = 1;
    if (pthread_create(&wal->worker_thread, NULL, lsm_wal_writer_thread, wal) != 0) {
        close(wal->fd);
        pthread_mutex_destroy(&wal->ring_buffer.mutex);
        pthread_cond_destroy(&wal->ring_buffer.cond);
        pthread_mutex_destroy(&wal->append_mutex);
        free(wal->filepath);
        free(wal);
        return NULL;
    }

    return wal;
}

int lsm_wal_append(lsm_wal_t *wal, uint64_t lsn, lsm_wal_record_type_t type, const char *key, uint32_t key_len, const char *val, uint32_t val_len) {
    pthread_mutex_lock(&wal->append_mutex);
    pthread_mutex_lock(&wal->ring_buffer.mutex);

    uint64_t next_tail = (wal->ring_buffer.tail + 1) % LSM_RING_BUFFER_SIZE;
    while (next_tail == wal->ring_buffer.head) {
        // Ring buffer full, yield or wait
        pthread_mutex_unlock(&wal->ring_buffer.mutex);
        usleep(1000);
        pthread_mutex_lock(&wal->ring_buffer.mutex);
    }

    lsm_wal_entry_t *entry = &wal->ring_buffer.entries[wal->ring_buffer.tail];
    entry->lsn = lsn;
    entry->type = type;
    entry->key_len = key_len;
    entry->val_len = val_len;
    entry->key = key;
    entry->val = val;

    wal->ring_buffer.tail = next_tail;
    pthread_cond_signal(&wal->ring_buffer.cond);
    pthread_mutex_unlock(&wal->ring_buffer.mutex);
    pthread_mutex_unlock(&wal->append_mutex);

    return 0;
}

int lsm_wal_sync(lsm_wal_t *wal) {
    // Wait until ring buffer is completely drained
    while (1) {
        pthread_mutex_lock(&wal->ring_buffer.mutex);
        uint64_t head = wal->ring_buffer.head;
        uint64_t tail = wal->ring_buffer.tail;
        pthread_mutex_unlock(&wal->ring_buffer.mutex);

        if (head == tail) break;
        usleep(1000);
    }
    return fsync(wal->fd);
}

void lsm_wal_close(lsm_wal_t *wal) {
    if (!wal) return;

    pthread_mutex_lock(&wal->ring_buffer.mutex);
    wal->worker_running = 0;
    pthread_cond_signal(&wal->ring_buffer.cond);
    pthread_mutex_unlock(&wal->ring_buffer.mutex);

    pthread_join(wal->worker_thread, NULL);

    close(wal->fd);
    pthread_mutex_destroy(&wal->ring_buffer.mutex);
    pthread_cond_destroy(&wal->ring_buffer.cond);
    pthread_mutex_destroy(&wal->append_mutex);

    free(wal->filepath);
    free(wal);
}

int lsm_wal_recover(const char *filepath, int (*callback)(uint64_t lsn, lsm_wal_record_type_t type, const char *key, uint32_t key_len, const char *val, uint32_t val_len)) {
    int fd = open(filepath, O_RDONLY);
    if (fd < 0) {
        if (errno == ENOENT) return 0;
        return -1;
    }

    while (1) {
        lsm_wal_header_t header;
        ssize_t n = read(fd, &header, LSM_WAL_HEADER_SIZE);
        if (n == 0) break; // EOF
        if (n < LSM_WAL_HEADER_SIZE) {
            // Corrupted tail record, truncate/discard
            break;
        }

        if (header.magic != LSM_WAL_MAGIC || header.version != LSM_WAL_VERSION) {
            break;
        }

        char *payload = malloc(header.payload_len);
        if (!payload) {
            close(fd);
            return -1;
        }

        n = read(fd, payload, header.payload_len);
        if (n < (ssize_t)header.payload_len) {
            free(payload);
            break; // Corrupted tail record
        }

        uint32_t computed_crc = lsm_crc32c(0, (unsigned char *)payload, header.payload_len);
        if (computed_crc != header.crc32c) {
            free(payload);
            break; // CRC mismatch, truncate tail
        }

        uint32_t key_len = *(uint32_t *)payload;
        const char *key = payload + sizeof(uint32_t);
        uint32_t val_len = *(uint32_t *)(payload + sizeof(uint32_t) + key_len);
        const char *val = payload + sizeof(uint32_t) + key_len + sizeof(uint32_t);

        if (callback) {
            if (callback(header.lsn, (lsm_wal_record_type_t)header.type, key, key_len, val, val_len) != 0) {
                free(payload);
                close(fd);
                return -1;
            }
        }

        free(payload);
    }

    close(fd);
    return 0;
}