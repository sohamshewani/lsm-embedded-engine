#include "lsm_compaction.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <pthread.h>
#include <unistd.h>
#include <dirent.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <inttypes.h>

#define LSM_MAX_LEVELS 7
#define LSM_MAX_PATH 1024
#define LSM_BLOCK_SIZE 4096
#define LSM_SPARSE_INTERVAL 16

typedef struct LSMKey {
    uint8_t* data;
    size_t size;
} LSMKey;

typedef struct LSMValue {
    uint8_t* data;
    size_t size;
    uint64_t lsn;
    bool is_tombstone;
} LSMValue;

typedef struct LSMSSTableMetadata {
    uint32_t file_id;
    uint32_t level;
    LSMKey min_key;
    LSMKey max_key;
    uint64_t file_size;
    char file_path[LSM_MAX_PATH];
} LSMSSTableMetadata;

typedef struct LSMSSTableList {
    LSMSSTableMetadata* tables;
    size_t count;
    size_t capacity;
} LSMSSTableList;

typedef struct LSMManifest {
    LSMSSTableList levels[LSM_MAX_LEVELS];
    uint32_t next_file_id;
    pthread_rwlock_t rwlock;
} LSMManifest;

typedef struct LSMCompactionJob {
    uint32_t src_level;
    uint32_t dst_level;
    LSMSSTableList inputs;
    bool is_running;
} LSMCompactionJob;

typedef struct LSMCompactionWorker {
    pthread_t thread;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    bool shutdown;
    LSMCompactionJob current_job;
    bool has_job;
    LSMManifest* manifest;
    char data_dir[LSM_MAX_PATH];
} LSMCompactionWorker;

typedef struct LSMIterator {
    bool valid;
    LSMKey current_key;
    LSMValue current_value;
    void* internal_state;
} LSMIterator;

typedef struct LSMKVPair {
    LSMKey key;
    LSMValue value;
} LSMKVPair;

static int lsm_key_compare(const LSMKey* a, const LSMKey* b) {
    size_t min_len = a->size < b->size ? a->size : b->size;
    int cmp = memcmp(a->data, b->data, min_len);
    if (cmp != 0) {
        return cmp;
    }
    if (a->size < b->size) return -1;
    if (a->size > b->size) return 1;
    return 0;
}

static void lsm_key_free(LSMKey* key) {
    if (key && key->data) {
        free(key->data);
        key->data = NULL;
        key->size = 0;
    }
}

static void lsm_value_free(LSMValue* val) {
    if (val && val->data) {
        free(val->data);
        val->data = NULL;
        val->size = 0;
    }
}

static LSMKey lsm_key_dup(const LSMKey* src) {
    LSMKey dst;
    dst.size = src->size;
    dst.data = (uint8_t*)malloc(src->size);
    if (dst.data && src->size > 0) {
        memcpy(dst.data, src->data, src->size);
    }
    return dst;
}

static LSMValue lsm_value_dup(const LSMValue* src) {
    LSMValue dst;
    dst.size = src->size;
    dst.lsn = src->lsn;
    dst.is_tombstone = src->is_tombstone;
    if (src->data && src->size > 0) {
        dst.data = (uint8_t*)malloc(src->size);
        memcpy(dst.data, src->data, src->size);
    } else {
        dst.data = NULL;
    }
    return dst;
}

static void lsm_sstable_list_init(LSMSSTableList* list) {
    list->count = 0;
    list->capacity = 4;
    list->tables = (LSMSSTableMetadata*)malloc(list->capacity * sizeof(LSMSSTableMetadata));
}

static void lsm_sstable_list_free(LSMSSTableList* list) {
    if (!list) return;
    for (size_t i = 0; i < list->count; ++i) {
        lsm_key_free(&list->tables[i].min_key);
        lsm_key_free(&list->tables[i].max_key);
    }
    free(list->tables);
    list->tables = NULL;
    list->count = 0;
    list->capacity = 0;
}

static void lsm_sstable_list_add(LSMSSTableList* list, const LSMSSTableMetadata* meta) {
    if (list->count >= list->capacity) {
        list->capacity *= 2;
        list->tables = (LSMSSTableMetadata*)realloc(list->tables, list->capacity * sizeof(LSMSSTableMetadata));
    }
    LSMSSTableMetadata* dest = &list->tables[list->count++];
    dest->file_id = meta->file_id;
    dest->level = meta->level;
    dest->min_key = lsm_key_dup(&meta->min_key);
    dest->max_key = lsm_key_dup(&meta->max_key);
    dest->file_size = meta->file_size;
    snprintf(dest->file_path, sizeof(dest->file_path), "%s", meta->file_path);
}

LSMManifest* lsm_manifest_create(const char* data_dir) {
    LSMManifest* manifest = (LSMManifest*)malloc(sizeof(LSMManifest));
    if (!manifest) return NULL;
    for (int i = 0; i < LSM_MAX_LEVELS; ++i) {
        lsm_sstable_list_init(&manifest->levels[i]);
    }
    manifest->next_file_id = 1;
    pthread_rwlock_init(&manifest->rwlock, NULL);
    return manifest;
}

void lsm_manifest_destroy(LSMManifest* manifest) {
    if (!manifest) return;
    pthread_rwlock_wrlock(&manifest->rwlock);
    for (int i = 0; i < LSM_MAX_LEVELS; ++i) {
        lsm_sstable_list_free(&manifest->levels[i]);
    }
    pthread_rwlock_unlock(&manifest->rwlock);
    pthread_rwlock_destroy(&manifest->rwlock);
    free(manifest);
}

static int sstable_meta_compare(const void* a, const void* b) {
    const LSMSSTableMetadata* ma = (const LSMSSTableMetadata*)a;
    const LSMSSTableMetadata* mb = (const LSMSSTableMetadata*)b;
    return lsm_key_compare(&ma->min_key, &mb->min_key);
}

void lsm_manifest_sort_level(LSMManifest* manifest, uint32_t level) {
    if (level >= LSM_MAX_LEVELS) return;
    LSMSSTableList* list = &manifest->levels[level];
    if (list->count > 1) {
        qsort(list->tables, list->count, sizeof(LSMSSTableMetadata), sstable_meta_compare);
    }
}

typedef struct SSTableBuilder {
    int fd;
    char path[LSM_MAX_PATH];
    uint64_t offset;
    uint32_t count;
    LSMKey min_key;
    LSMKey max_key;
    bool has_keys;
} SSTableBuilder;

static bool sstable_builder_init(SSTableBuilder* builder, const char* data_dir, uint32_t file_id, uint32_t level) {
    snprintf(builder->path, sizeof(builder->path), "%s/sst_%u_%u.sst", data_dir, level, file_id);
    builder->fd = open(builder->path, O_CREAT | O_WRONLY | O_TRUNC, 0644);
    if (builder->fd < 0) return false;
    builder->offset = 0;
    builder->count = 0;
    builder->has_keys = false;
    memset(&builder->min_key, 0, sizeof(LSMKey));
    memset(&builder->max_key, 0, sizeof(LSMKey));
    return true;
}

static bool sstable_builder_append(SSTableBuilder* builder, const LSMKey* key, const LSMValue* val) {
    uint32_t key_size = (uint32_t)key->size;
    uint32_t val_size = (uint32_t)val->size;
    uint64_t lsn = val->lsn;
    uint8_t tombstone = val->is_tombstone ? 1 : 0;

    if (write(builder->fd, &key_size, sizeof(key_size)) != sizeof(key_size)) return false;
    if (write(builder->fd, &val_size, sizeof(val_size)) != sizeof(val_size)) return false;
    if (write(builder->fd, &lsn, sizeof(lsn)) != sizeof(lsn)) return false;
    if (write(builder->fd, &tombstone, sizeof(tombstone)) != sizeof(tombstone)) return false;
    if (write(builder->fd, key->data, key->size) != (ssize_t)key->size) return false;
    if (val->size > 0) {
        if (write(builder->fd, val->data, val->size) != (ssize_t)val->size) return false;
    }

    if (!builder->has_keys) {
        builder->min_key = lsm_key_dup(key);
        builder->max_key = lsm_key_dup(key);
        builder->has_keys = true;
    } else {
        lsm_key_free(&builder->max_key);
        builder->max_key = lsm_key_dup(key);
    }
    builder->count++;
    builder->offset += sizeof(key_size) + sizeof(val_size) + sizeof(lsn) + sizeof(tombstone) + key->size + val->size;
    return true;
}

static bool sstable_builder_finish(SSTableBuilder* builder, LSMSSTableMetadata* out_meta, uint32_t file_id, uint32_t level) {
    fsync(builder->fd);
    close(builder->fd);

    struct stat st;
    if (stat(builder->path, &st) != 0) {
        return false;
    }

    out_meta->file_id = file_id;
    out_meta->level = level;
    out_meta->min_key = lsm_key_dup(&builder->min_key);
    out_meta->max_key = lsm_key_dup(&builder->max_key);
    out_meta->file_size = (uint64_t)st.st_size;
    snprintf(out_meta->file_path, sizeof(out_meta->file_path), "%s", builder->path);

    lsm_key_free(&builder->min_key);
    lsm_key_free(&builder->max_key);
    return true;
}

typedef struct SSTableIterator {
    int fd;
    uint64_t file_size;
    uint64_t current_offset;
    bool valid;
    LSMKey current_key;
    LSMValue current_value;
} SSTableIterator;

static bool sstable_iterator_init(SSTableIterator* it, const char* path) {
    struct stat st;
    if (stat(path, &st) != 0) return false;
    it->file_size = (uint64_t)st.st_size;
    it->fd = open(path, O_RDONLY);
    if (it->fd < 0) return false;
    it->current_offset = 0;
    it->valid = false;
    memset(&it->current_key, 0, sizeof(LSMKey));
    memset(&it->current_value, 0, sizeof(LSMValue));
    return true;
}

static void sstable_iterator_close(SSTableIterator* it) {
    if (it) {
        if (it->fd >= 0) close(it->fd);
        lsm_key_free(&it->current_key);
        lsm_value_free(&it->current_value);
    }
}

static bool sstable_iterator_next(SSTableIterator* it) {
    lsm_key_free(&it->current_key);
    lsm_value_free(&it->current_value);

    if (it->current_offset >= it->file_size) {
        it->valid = false;
        return false;
    }

    uint32_t key_size = 0;
    uint32_t val_size = 0;
    uint64_t lsn = 0;
    uint8_t tombstone = 0;

    if (read(it->fd, &key_size, sizeof(key_size)) != sizeof(key_size)) { it->valid = false; return false; }
    if (read(it->fd, &val_size, sizeof(val_size)) != sizeof(val_size)) { it->valid = false; return false; }
    if (read(it->fd, &lsn, sizeof(lsn)) != sizeof(lsn)) { it->valid = false; return false; }
    if (read(it->fd, &tombstone, sizeof(tombstone)) != sizeof(tombstone)) { it->valid = false; return false; }

    it->current_key.size = key_size;
    it->current_key.data = (uint8_t*)malloc(key_size);
    if (read(it->fd, it->current_key.data, key_size) != (ssize_t)key_size) {
        it->valid = false;
        return false;
    }

    it->current_value.size = val_size;
    it->current_value.lsn = lsn;
    it->current_value.is_tombstone = (tombstone != 0);
    if (val_size > 0) {
        it->current_value.data = (uint8_t*)malloc(val_size);
        if (read(it->fd, it->current_value.data, val_size) != (ssize_t)val_size) {
            it->valid = false;
            return false;
        }
    } else {
        it->current_value.data = NULL;
    }

    it->current_offset += sizeof(key_size) + sizeof(val_size) + sizeof(lsn) + sizeof(tombstone) + key_size + val_size;
    it->valid = true;
    return true;
}

typedef struct MergingIterator {
    SSTableIterator* its;
    size_t count;
    bool valid;
    LSMKey current_key;
    LSMValue current_value;
} MergingIterator;

static bool merging_iterator_init(MergingIterator* mit, const LSMSSTableList* tables) {
    mit->count = tables->count;
    mit->its = (SSTableIterator*)malloc(mit->count * sizeof(SSTableIterator));
    for (size_t i = 0; i < mit->count; ++i) {
        if (!sstable_iterator_init(&mit->its[i], tables->tables[i].file_path)) {
            for (size_t j = 0; j