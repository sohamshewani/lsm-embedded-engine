#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <stdbool.h>
#include <assert.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>

/* ============================================================================
 * EMBEDDED LSM ENGINE IMPLEMENTATION (Single-File Self-Contained)
 * ============================================================================ */

#define LSM_MAGIC 0x4C534D31 /* "LSM1" */
#define LSM_VERSION 1
#define MAX_KEY_SIZE 256
#define MAX_VAL_SIZE 1024
#define MEMTABLE_MAX_ENTRIES 128
#define SSTABLE_MAX_ENTRIES 512

typedef struct {
    uint32_t magic;
    uint32_t version;
    uint32_t count;
    uint32_t reserved;
} lsm_file_header_t;

typedef struct {
    char key[MAX_KEY_SIZE];
    char value[MAX_VAL_SIZE];
    uint32_t key_len;
    uint32_t val_len;
    uint8_t tombstone; /* 1 if deleted, 0 otherwise */
} lsm_entry_t;

typedef struct {
    lsm_entry_t entries[MEMTABLE_MAX_ENTRIES];
    int count;
} lsm_memtable_t;

struct lsm_db {
    char dir_path[512];
    lsm_memtable_t memtable;
    int sstable_count;
    char sstable_files[64][1024];
    pthread_rwlock_t rwlock;
    bool closed;
};

typedef struct lsm_db lsm_db_t;

/* Forward declarations */
static int lsm_flush_memtable(lsm_db_t *db);

static int compare_entries(const void *a, const void *b) {
    const lsm_entry_t *ea = (const lsm_entry_t *)a;
    const lsm_entry_t *eb = (const lsm_entry_t *)b;
    uint32_t min_len = ea->key_len < eb->key_len ? ea->key_len : eb->key_len;
    int cmp = memcmp(ea->key, eb->key, min_len);
    if (cmp != 0) return cmp;
    if (ea->key_len < eb->key_len) return -1;
    if (ea->key_len > eb->key_len) return 1;
    return 0;
}

lsm_db_t *lsm_open(const char *path) {
    if (!path) return NULL;
    lsm_db_t *db = (lsm_db_t *)malloc(sizeof(lsm_db_t));
    if (!db) return NULL;

    memset(db, 0, sizeof(lsm_db_t));
    strncpy(db->dir_path, path, sizeof(db->dir_path) - 1);
    
    if (pthread_rwlock_init(&db->rwlock, NULL) != 0) {
        free(db);
        return NULL;
    }

    /* Create directory if not exists */
    mkdir(path, 0777);

    /* Scan directory for existing SSTables */
    char path_buf[1024];
    for (int i = 0; i < 64; i++) {
        snprintf(path_buf, sizeof(path_buf), "%s/sst_%d.db", path, i);
        int fd = open(path_buf, O_RDONLY);
        if (fd >= 0) {
            lsm_file_header_t hdr;
            if (read(fd, &hdr, sizeof(hdr)) == sizeof(hdr) && hdr.magic == LSM_MAGIC) {
                strncpy(db->sstable_files[db->sstable_count], path_buf, sizeof(db->sstable_files[0]) - 1);
                db->sstable_count++;
            }
            close(fd);
        }
    }

    return db;
}

int lsm_close(lsm_db_t *db) {
    if (!db) return -1;
    pthread_rwlock_wrlock(&db->rwlock);
    if (db->closed) {
        pthread_rwlock_unlock(&db->rwlock);
        return -1;
    }
    lsm_flush_memtable(db);
    db->closed = true;
    pthread_rwlock_unlock(&db->rwlock);
    pthread_rwlock_destroy(&db->rwlock);
    free(db);
    return 0;
}

static int lsm_flush_memtable(lsm_db_t *db) {
    if (db->memtable.count == 0) return 0;

    /* Sort memtable entries */
    qsort(db->memtable.entries, db->memtable.count, sizeof(lsm_entry_t), compare_entries);

    char path_buf[1024];
    snprintf(path_buf, sizeof(path_buf), "%s/sst_%d.db", db->dir_path, db->sstable_count);

    int fd = open(path_buf, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) return -1;

    lsm_file_header_t hdr;
    hdr.magic = LSM_MAGIC;
    hdr.version = LSM_VERSION;
    hdr.count = db->memtable.count;
    hdr.reserved = 0;

    if (write(fd, &hdr, sizeof(hdr)) != sizeof(hdr)) {
        close(fd);
        return -1;
    }

    for (int i = 0; i < db->memtable.count; i++) {
        if (write(fd, &db->memtable.entries[i], sizeof(lsm_entry_t)) != sizeof(lsm_entry_t)) {
            close(fd);
            return -1;
        }
    }

    fsync(fd);
    close(fd);

    if (db->sstable_count < 64) {
        strncpy(db->sstable_files[db->sstable_count], path_buf, sizeof(db->sstable_files[0]) - 1);
        db->sstable_count++;
    }

    db->memtable.count = 0;
    return 0;
}

int lsm_put(lsm_db_t *db, const char *key, size_t key_len, const char *value, size_t val_len) {
    if (!db || !key || key_len == 0 || key_len >= MAX_KEY_SIZE || val_len >= MAX_VAL_SIZE)
        return -EINVAL;

    pthread_rwlock_wrlock(&db->rwlock);
    if (db->closed) {
        pthread_rwlock_unlock(&db->rwlock);
        return -EBADF;
    }

    if (db->memtable.count >= MEMTABLE_MAX_ENTRIES) {
        if (lsm_flush_memtable(db) < 0) {
            pthread_rwlock_unlock(&db->rwlock);
            return -EIO;
        }
    }

    lsm_entry_t *entry = &db->memtable.entries[db->memtable.count++];
    memset(entry, 0, sizeof(lsm_entry_t));
    memcpy(entry->key, key, key_len);
    entry->key_len = (uint32_t)key_len;
    if (value && val_len > 0) {
        memcpy(entry->value, value, val_len);
        entry->val_len = (uint32_t)val_len;
        entry->tombstone = 0;
    } else {
        entry->tombstone = 1;
    }

    pthread_rwlock_unlock(&db->rwlock);
    return 0;
}

int lsm_get(lsm_db_t *db, const char *key, size_t key_len, char *value_out, size_t val_max_len, size_t *val_len_out) {
    if (!db || !key || key_len == 0 || key_len >= MAX_KEY_SIZE)
        return -EINVAL;

    pthread_rwlock_rdlock(&db->rwlock);
    if (db->closed) {
        pthread_rwlock_unlock(&db->rwlock);
        return -EBADF;
    }

    /* 1. Search Memtable (Reverse order to find newest write) */
    for (int i = db->memtable.count - 1; i >= 0; i--) {
        lsm_entry_t *entry = &db->memtable.entries[i];
        if (entry->key_len == key_len && memcmp(entry->key, key, key_len) == 0) {
            if (entry->tombstone) {
                pthread_rwlock_unlock(&db->rwlock);
                return -ENOENT;
            }
            if (value_out && val_max_len > 0) {
                size_t copy_len = entry->val_len < val_max_len ? entry->val_len : val_max_len;
                memcpy(value_out, entry->value, copy_len);
            }
            if (val_len_out) *val_len_out = entry->val_len;
            pthread_rwlock_unlock(&db->rwlock);
            return 0;
        }
    }

    /* 2. Search SSTables (Newest to Oldest) */
    for (int s = db->sstable_count - 1; s >= 0; s--) {
        int fd = open(db->sstable_files[s], O_RDONLY);
        if (fd < 0) continue;

        lsm_file_header_t hdr;
        if (read(fd, &hdr, sizeof(hdr)) != sizeof(hdr) || hdr.magic != LSM_MAGIC) {
            close(fd);
            continue;
        }

        /* Binary search or linear scan over SSTable entries */
        lsm_entry_t entry;
        bool found = false;
        lsm_entry_t found_entry;

        /* SSTables are sorted by key, perform binary search */
        int low = 0, high = (int)hdr.count - 1;
        while (low <= high) {
            int mid = low + (high - low) / 2;
            off_t offset = sizeof(lsm_file_header_t) + (off_t)(mid * sizeof(lsm_entry_t));
            if (lseek(fd, offset, SEEK_SET) == (off_t)-1) break;
            if (read(fd, &entry, sizeof(lsm_entry_t)) != sizeof(lsm_entry_t)) break;

            uint32_t min_len = entry.key_len < key_len ? entry.key_len : key_len;
            int cmp = memcmp(entry.key, key, min_len);
            if (cmp == 0) {
                if (entry.key_len < key_len) cmp = -1;
                else if (entry.key_len > key_len) cmp = 1;
            }

            if (cmp == 0) {
                found_entry = entry;
                found = true;
                break;
            } else if (cmp < 0) {
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }
        close(fd);

        if (found) {
            if (found_entry.tombstone) {
                pthread_rwlock_unlock(&db->rwlock);
                return -ENOENT;
            }
            if (value_out && val_max_len > 0) {
                size_t copy_len = found_entry.val_len < val_max_len ? found_entry.val_len : val_max_len;
                memcpy