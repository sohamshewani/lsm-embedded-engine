#ifndef LSM_TYPES_H
#define LSM_TYPES_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include <pthread.h>
#include <sys/types.h>

#define LSM_MAX_LEVELS 7
#define LSM_SKIPLIST_MAX_LEVEL 32
#define LSM_CACHE_LINE_SIZE 64
#define LSM_WAL_MAGIC 0x4C534D575553524EULL
#define LSM_MAX_KEY_SIZE 65536
#define LSM_MAX_VALUE_SIZE (1024 * 1024)

typedef enum {
    LSM_SUCCESS = 0,
    LSM_ERR_INVALID_ARGUMENT = -1,
    LSM_ERR_OUT_OF_MEMORY = -2,
    LSM_ERR_IO = -3,
    LSM_ERR_CORRUPTION = -4,
    LSM_ERR_NOT_FOUND = -5,
    LSM_ERR_FULL = -6,
    LSM_ERR_BUSY = -7
} lsm_status_t;

typedef enum {
    LSM_VAL_PUT = 1,
    LSM_VAL_DELETE = 2
} lsm_value_type_t;

typedef uint64_t lsm_lsn_t;

typedef struct {
    const char *data;
    size_t size;
} lsm_slice_t;

typedef struct {
    lsm_slice_t key;
    lsm_slice_t value;
    lsm_lsn_t lsn;
    lsm_value_type_t type;
} lsm_record_t;

typedef struct lsm_skiplist_node {
    lsm_lsn_t lsn;
    uint32_t type;
    uint32_t key_len;
    uint32_t val_len;
    uint32_t level;
    struct lsm_skiplist_node *forward[1];
} lsm_skiplist_node_t __attribute__((aligned(LSM_CACHE_LINE_SIZE)));

typedef struct {
    char *buffer;
    size_t capacity;
    _Atomic size_t offset;
    pthread_mutex_t lock;
} lsm_arena_t;

typedef struct {
    lsm_skiplist_node_t *head;
    lsm_arena_t arena;
    _Atomic int height;
    _Atomic size_t count;
    _Atomic size_t memory_usage;
} lsm_memtable_t;

typedef struct {
    lsm_memtable_t *active;
    lsm_memtable_t *immutable;
    pthread_rwlock_t rwlock;
    _Atomic bool flush_in_progress;
} lsm_active_memtables_t;

typedef struct {
    uint64_t magic;
    uint32_t payload_length;
    uint16_t type;
    uint16_b_reserved_t;
    uint32_t crc32c;
    lsm_lsn_t lsn;
} __attribute__((packed)) lsm_wal_header_t;

typedef struct lsm_wal_entry {
    lsm_wal_header_t header;
    struct lsm_wal_entry *next;
    char payload[];
} lsm_wal_entry_t;

typedef struct {
    _Atomic(lsm_wal_entry_t*) head;
    char pad0[LSM_CACHE_LINE_SIZE - sizeof(_Atomic(lsm_wal_entry_t*))];
    _Atomic(lsm_wal_entry_t*) tail;
    char pad1[LSM_CACHE_LINE_SIZE - sizeof(_Atomic(lsm_wal_entry_t*))];
    int fd;
    pthread_t worker_thread;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    _Atomic bool shutdown;
    lsm_lsn_t committed_lsn;
} lsm_wal_t;

typedef struct {
    uint32_t num_files;
    uint64_t *file_numbers;
} lsm_level_meta_t;

typedef struct {
    lsm_level_meta_t levels[LSM_MAX_LEVELS];
    uint64_t next_file_number;
    lsm_lsn_t last_lsn;
} lsm_manifest_t;

typedef struct {
    pthread_rwlock_t rwlock;
    lsm_manifest_t manifest;
    int manifest_fd;
    char *data_dir;
} lsm_version_set_t;

typedef struct {
    uint64_t file_number;
    uint32_t level;
    uint64_t file_size;
    lsm_slice_t smallest_key;
    lsm_slice_t largest_key;
} lsm_sstable_meta_t;

typedef struct {
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    bool stop;
    struct lsm_engine *engine;
} lsm_compaction_worker_t;

typedef struct lsm_engine {
    char *data_dir;
    lsm_active_memtables_t memtables;
    lsm_wal_t wal;
    lsm_version_set_t versions;
    lsm_compaction_worker_t worker;
    _Atomic lsm_lsn_t current_lsn;
    bool is_open;
} lsm_engine_t;

static inline lsm_slice_t lsm_slice_from(const char *data, size_t size) {
    lsm_slice_t s = { .data = data, .size = size };
    return s;
}

static inline int lsm_slice_compare(const lsm_slice_t *a, const lsm_slice_t *b) {
    size_t min_len = (a->size < b->size) ? a->size : b->size;
    int cmp = memcmp(a->data, b->data, min_len);
    if (cmp != 0) {
        return cmp;
    }
    if (a->size < b->size) return -1;
    if (a->size > b->size) return 1;
    return 0;
}

lsm_status_t lsm_engine_open(const char *data_dir, lsm_engine_t **out_engine);
lsm_status_t lsm_engine_close(lsm_engine_t *engine);
lsm_status_t lsm_engine_put(lsm_engine_t *engine, const lsm_slice_t *key, const lsm_slice_t *value);
lsm_status_t lsm_engine_get(lsm_engine_t *engine, const lsm_slice_t *key, lsm_arena_t *arena, lsm_slice_t *out_value);
lsm_status_t lsm_engine_delete(lsm_engine_t *engine, const lsm_slice_t *key);

#endif