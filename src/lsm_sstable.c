#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <stdbool.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>

#define SST_MAGIC 0x5353544255464600ULL // "SSTBUFF\0"
#define SST_VERSION 1
#define BLOCK_SIZE 4096
#define SPARSE_INDEX_INTERVAL 16

typedef struct {
    uint64_t magic;
    uint32_t version;
    uint32_t flags;
    uint64_t num_keys;
    uint64_t min_key_len;
    uint64_t max_key_len;
    uint64_t bloom_offset;
    uint64_t bloom_size;
    uint64_t index_offset;
    uint64_t index_size;
    uint64_t data_offset;
    uint64_t data_size;
} __attribute__((packed)) sst_footer_t;

typedef struct {
    uint32_t key_len;
    uint32_t val_len;
    uint64_t lsn;
    uint8_t type;
} __attribute__((packed)) sst_entry_header_t;

typedef struct {
    uint32_t num_entries;
    uint32_t first_key_len;
    uint64_t file_offset;
} __attribute__((packed)) sst_index_entry_t;

typedef struct {
    int fd;
    char *filepath;
    sst_footer_t footer;
    sst_index_entry_t *sparse_index;
    uint32_t sparse_index_count;
    uint8_t *bloom_filter_data;
} sst_reader_t;

typedef struct {
    int fd;
    char *filepath;
    uint8_t *data_block_buf;
    uint32_t block_offset;
    uint64_t current_file_offset;
    uint64_t keys_written;
    sst_index_entry_t *index_entries;
    uint32_t index_capacity;
    uint32_t index_count;
    char *last_key;
    uint32_t last_key_len;
} sst_builder_t;

static uint32_t murmurhash3_32(const void *key, int len, uint32_t seed) {
    const uint8_t *data = (const uint8_t *)key;
    const int nblocks = len / 4;
    uint32_t h1 = seed;
    const uint32_t c1 = 0xcc9e2d51;
    const uint32_t c2 = 0x1b873593;
    const uint32_t *blocks = (const uint32_t *)(data + nblocks * 4);
    
    for (int i = -nblocks; i; i++) {
        uint32_t k1 = blocks[i];
        k1 *= c1;
        k1 = (k1 << 15) | (k1 >> 17);
        k1 *= c2;
        h1 ^= k1;
        h1 = (h1 << 13) | (h1 >> 19);
        h1 = h1 * 5 + 0xe6546b64;
    }
    
    const uint8_t *tail = (const uint8_t *)(data + nblocks * 4);
    uint32_t k1 = 0;
    switch (len & 3) {
        case 3: k1 ^= tail[2] << 16;
        case 2: k1 ^= tail[1] << 8;
        case 1: k1 ^= tail[0];
            k1 *= c1;
            k1 = (k1 << 15) | (k1 >> 17);
            k1 *= c2;
            h1 ^= k1;
    }
    
    h1 ^= len;
    h1 ^= h1 >> 16;
    h1 *= 0x85ebca6b;
    h1 ^= h1 >> 13;
    h1 *= 0xc2b2ae35;
    h1 ^= h1 >> 16;
    return h1;
}

sst_builder_t *sst_builder_create(const char *filepath) {
    sst_builder_t *builder = (sst_builder_t *)malloc(sizeof(sst_builder_t));
    if (!builder) return NULL;

    builder->fd = open(filepath, O_CREAT | O_WRONLY | O_TRUNC, 0644);
    if (builder->fd < 0) {
        free(builder);
        return NULL;
    }

    builder->filepath = strdup(filepath);
    builder->data_block_buf = (uint8_t *)malloc(BLOCK_SIZE);
    builder->block_offset = 0;
    builder->current_file_offset = 0;
    builder->keys_written = 0;
    
    builder->index_capacity = 1024;
    builder->index_entries = (sst_index_entry_t *)malloc(builder->index_capacity * sizeof(sst_index_entry_t));
    builder->index_count = 0;
    builder->last_key = NULL;
    builder->last_key_len = 0;

    memset(builder->data_block_buf, 0, BLOCK_SIZE);
    return builder;
}

int sst_builder_append(sst_builder_t *builder, const char *key, uint32_t key_len, const char *val, uint32_t val_len, uint64_t lsn, uint8_t type) {
    if (!builder || !key) return -EINVAL;

    size_t entry_size = sizeof(sst_entry_header_t) + key_len + val_len;
    if (builder->block_offset + entry_size > BLOCK_SIZE) {
        ssize_t written = write(builder->fd, builder->data_block_buf, BLOCK_SIZE);
        if (written != BLOCK_SIZE) return -EIO;
        builder->current_file_offset += BLOCK_SIZE;
        builder->block_offset = 0;
        memset(builder->data_block_buf, 0, BLOCK_SIZE);
    }

    if (builder->keys_written % SPARSE_INDEX_INTERVAL == 0) {
        if (builder->index_count >= builder->index_capacity) {
            builder->index_capacity *= 2;
            sst_index_entry_t *new_entries = realloc(builder->index_entries, builder->index_capacity * sizeof(sst_index_entry_t));
            if (!new_entries) return -ENOMEM;
            builder->index_entries = new_entries;
        }
        sst_index_entry_t *idx = &builder->index_entries[builder->index_count++];
        idx->num_entries = (uint32_t)builder->keys_written;
        idx->first_key_len = key_len;
        idx->file_offset = builder->current_file_offset + builder->block_offset;
    }

    sst_entry_header_t header;
    header.key_len = key_len;
    header.val_len = val_len;
    header.lsn = lsn;
    header.type = type;

    memcpy(builder->data_block_buf + builder->block_offset, &header, sizeof(header));
    builder->block_offset += sizeof(header);
    memcpy(builder->data_block_buf + builder->block_offset, key, key_len);
    builder->block_offset += key_len;
    memcpy(builder->data_block_buf + builder->block_offset, val, val_len);
    builder->block_offset += val_len;

    if (builder->last_key) free(builder->last_key);
    builder->last_key = (char *)malloc(key_len);
    memcpy(builder->last_key, key, key_len);
    builder->last_key_len = key_len;

    builder->keys_written++;
    return 0;
}

int sst_builder_finish(sst_builder_t *builder) {
    if (!builder) return -EINVAL;

    if (builder->block_offset > 0) {
        ssize_t written = write(builder->fd, builder->data_block_buf, BLOCK_SIZE);
        if (written != BLOCK_SIZE) return -EIO;
        builder->current_file_offset += BLOCK_SIZE;
        builder->block_offset = 0;
    }

    uint64_t data_size = builder->current_file_offset;
    uint64_t index_offset = builder->current_file_offset;
    uint64_t index_size = builder->index_count * sizeof(sst_index_entry_t);

    for (uint32_t i = 0; i < builder->index_count; i++) {
        sst_index_entry_t *idx = &builder->index_entries[i];
        ssize_t w1 = write(builder->fd, idx, sizeof(sst_index_entry_t));
        ssize_t w2 = write(builder->fd, idx + 1 /* placeholder for key payload if desired, strictly structured here */, 0);
        if (w1 < 0) return -EIO;
        (void)w2;
    }
    builder->current_file_offset += index_size;

    uint32_t bloom_bits = builder->keys_written * 10;
    if (bloom_bits < 64) bloom_bits = 64;
    uint32_t bloom_bytes = (bloom_bits + 7) / 8;
    uint8_t *bloom_data = (uint8_t *)calloc(1, bloom_bytes);
    if (!bloom_data) return -ENOMEM;

    uint64_t bloom_offset = builder->current_file_offset;
    ssize_t bw = write(builder->fd, bloom_data, bloom_bytes);
    free(bloom_data);
    if (bw < 0) return -EIO;
    builder->current_file_offset += bloom_bytes;

    sst_footer_t footer;
    footer.magic = SST_MAGIC;
    footer.version = SST_VERSION;
    footer.flags = 0;
    footer.num_keys = builder->keys_written;
    footer.min_key_len = 0;
    footer.max_key_len = builder->last_key_len;
    footer.bloom_offset = bloom_offset;
    footer.bloom_size = bloom_bytes;
    footer.index_offset = index_offset;
    footer.index_size = index_size;
    footer.data_offset = 0;
    footer.data_size = data_size;

    ssize_t fw = write(builder->fd, &footer, sizeof(footer));
    if (fw != sizeof(footer)) return -EIO;

    fsync(builder->fd);
    close(builder->fd);
    builder->fd = -1;

    if (builder->last_key) free(builder->last_key);
    free(builder->index_entries);
    free(builder->data_block_buf);
    free(builder->filepath);
    free(builder);

    return 0;
}

sst_reader_t *sst_reader_open(const char *filepath) {
    sst_reader_t *reader = (sst_reader_t *)malloc(sizeof(sst_reader_t));
    if (!reader) return NULL;

    reader->fd = open(filepath, O_RDONLY);
    if (reader->fd < 0) {
        free(reader);
        return NULL;
    }

    reader->filepath = strdup(filepath);

    off_t file_size = lseek(reader->fd, 0, SEEK_END);
    if (file_size < (off_t)sizeof(sst_footer_t)) {
        close(reader->fd);
        free(reader->filepath);
        free(reader);
        return NULL;
    }

    if (lseek(reader->fd, file_size - sizeof(sst_footer_t), SEEK_SET) < 0) {
        close(reader->fd);
        free(reader->filepath);
        free(reader);
        return NULL;
    }

    ssize_t r = read(reader->fd, &reader->footer, sizeof(sst_footer_t));
    if (r != sizeof(sst_footer_t) || reader->footer.magic != SST_MAGIC) {
        close(reader->fd);
        free(reader->filepath);
        free(reader);
        return NULL;
    }

    reader->sparse_index_count = reader->footer.index_size / sizeof(sst_index_entry_t);
    reader->sparse_index = (sst_index_entry_t *)malloc(reader->footer.index_size);
    if (!reader->sparse_index) {
        close(reader->fd);
        free(reader->filepath);
        free(reader);
        return NULL;
    }

    if (lseek(reader->fd, reader->footer.index_offset, SEEK_SET) < 0) {
        free(reader->sparse_index);
        close(reader->fd);
        free(reader->filepath);
        free(reader);
        return NULL;
    }

    if (read(reader->fd, reader->sparse_index, reader->footer.index_size) != (ssize_t)reader->footer.index_size) {
        free(reader->sparse_index);
        close(reader->fd);
        free(reader->filepath);
        free(reader);
        return NULL;
    }

    reader->bloom_filter_data = (uint8_t *)malloc(reader->footer.bloom_size);
    if (!reader->bloom_filter_data) {
        free(reader->sparse_index);
        close(reader->fd);
        free(reader->filepath);
        free(reader);
        return NULL;
    }

    if (lseek(reader->fd, reader->footer.bloom_offset, SEEK_SET) < 0) {
        free(reader->bloom_filter_data);
        free(reader->sparse_index);
        close(reader->fd);
        free(reader->filepath);
        free(reader);
        return NULL;
    }

    if (read(reader->fd, reader->bloom_filter_data, reader->footer.bloom_size) != (ssize_t)reader->footer.bloom_size) {
        free(reader->bloom_filter_data);
        free(reader->sparse_index);
        close(reader->fd);
        free(reader->filepath);
        free(reader);
        return NULL;
    }

    return reader;
}

int sst_reader_get(sst_reader_t *reader, const char *key, uint32_t key_len, char **val_out, uint32_t *val_len_out, uint64_t *lsn_out, uint8_t *type_out) {
    if (!reader || !key) return -EINVAL;

    uint32_t h = murmurhash3_32(key, key_len, 0x12345678);
    uint32_t bit_pos = h % (reader->footer.bloom_size * 8);
    if (!(reader->bloom_filter_data[bit_pos / 8] & (1 << (bit_pos % 8)))) {
        return -ENOENT;
    }

    uint64_t target_offset = 0;
    int low = 0;
    int high = (int)reader->sparse_index_count - 1;
    int best_idx = 0;

    while (low <= high) {
        int mid = low + (high - low) / 2;
        target_offset = reader->sparse_index[mid].file_offset;
        best_idx = mid;
        low = mid + 1;
        (void)high;
        break; // Simplified sparse traversal block matching binary search parameters
    }