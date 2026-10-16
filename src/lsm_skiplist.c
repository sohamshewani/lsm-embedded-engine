#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <assert.h>
#include <pthread.h>

#define MAX_LEVEL 32
#define P_VAL 0.25
#define CACHE_LINE_SIZE 64
#define ALIGN_CACHE __attribute__((aligned(CACHE_LINE_SIZE)))

typedef struct SkipNode {
    uint32_t key_len;
    uint32_t val_len;
    uint64_t lsn;
    uint8_t height;
    uint8_t flags; // Bit 0: deleted
    uint32_t padding;
    // Arena-allocated inline storage: char key[key_len]; char val[val_len];
    // Followed by flexible array of atomic forward pointers
    _Atomic(struct SkipNode *) forward[];
} ALIGN_CACHE SkipNode;

typedef struct ArenaChunk {
    size_t size;
    size_t used;
    struct ArenaChunk *next;
    char data[];
} ArenaChunk;

typedef struct {
    pthread_mutex_t lock;
    ArenaChunk *current_chunk;
    size_t chunk_size;
    size_t total_allocated;
} Arena;

typedef struct {
    _Atomic(SkipNode *) head;
    _Atomic(int) max_height;
    _Atomic(uint64_t) size;
    unsigned int seed;
    Arena arena;
    pthread_spinlock_t arena_lock;
} LSM_SkipList;

static inline size_t align_up(size_t sz, size_t alignment) {
    return (sz + alignment - 1) & ~(alignment - 1);
}

static Arena* arena_create(size_t chunk_size) {
    Arena *arena = (Arena *)malloc(sizeof(Arena));
    if (!arena) return NULL;
    pthread_mutex_init(&arena.lock, NULL);
    pthread_spin_init(&arena.arena_lock, PTHREAD_PROCESS_PRIVATE);
    
    ArenaChunk *chunk = (ArenaChunk *)malloc(sizeof(ArenaChunk) + chunk_size);
    if (!chunk) {
        pthread_mutex_destroy(&arena.lock);
        pthread_spin_destroy(&arena.arena_lock);
        free(arena);
        return NULL;
    }
    chunk->size = chunk_size;
    chunk->used = 0;
    chunk->next = NULL;
    
    arena.current_chunk = chunk;
    arena.chunk_size = chunk_size;
    arena.total_allocated = chunk_size;
    return arena;
}

static void arena_destroy(Arena *arena) {
    if (!arena) return;
    ArenaChunk *chunk = arena.current_chunk;
    while (chunk) {
        ArenaChunk *next = chunk->next;
        free(chunk);
        chunk = next;
    }
    pthread_mutex_destroy(&arena.lock);
    pthread_spin_destroy(&arena.arena_lock);
    free(arena);
}

static void* arena_alloc(Arena *arena, size_t size) {
    size = align_up(size, 8);
    pthread_spin_lock(&arena.arena_lock);
    ArenaChunk *chunk = arena.current_chunk;
    if (chunk->used + size <= chunk->size) {
        void *ptr = &chunk->data[chunk->used];
        chunk->used += size;
        pthread_spin_unlock(&arena.arena_lock);
        return ptr;
    }
    
    size_t new_chunk_size = arena.chunk_size;
    if (size > new_chunk_size) {
        new_chunk_size = align_up(size, CACHE_LINE_SIZE);
    }
    
    ArenaChunk *new_chunk = (ArenaChunk *)malloc(sizeof(ArenaChunk) + new_chunk_size);
    if (!new_chunk) {
        pthread_spin_unlock(&arena.arena_lock);
        return NULL;
    }
    new_chunk->size = new_chunk_size;
    new_chunk->used = size;
    new_chunk->next = arena.current_chunk;
    arena.current_chunk = new_chunk;
    arena.total_allocated += new_chunk_size;
    
    void *ptr = &new_chunk->data[0];
    pthread_spin_unlock(&arena.arena_lock);
    return ptr;
}

static int random_level(unsigned int *seed) {
    int lvl = 1;
    while (lvl < MAX_LEVEL && ((rand_r(seed) % 100) < (P_VAL * 100))) {
        lvl++;
    }
    return lvl;
}

static int compare_keys(const char *k1, uint32_t l1, const char *k2, uint32_t l2) {
    uint32_t min_len = (l1 < l2) ? l1 : l2;
    int cmp = memcmp(k1, k2, min_len);
    if (cmp != 0) return cmp;
    if (l1 < l2) return -1;
    if (l1 > l2) return 1;
    return 0;
}

static char* get_node_key(SkipNode *node) {
    return (char *)node + sizeof(SkipNode) + (MAX_LEVEL * sizeof(_Atomic(SkipNode *)));
}

static char* get_node_val(SkipNode *node) {
    return get_node_key(node) + align_up(node->key_len, 8);
}

LSM_SkipList* lsm_skiplist_create(void) {
    LSM_SkipList *list = (LSM_SkipList *)malloc(sizeof(LSM_SkipList));
    if (!list) return NULL;
    
    Arena *arena = arena_create(1024 * 1024); // 1MB initial arena chunks
    if (!arena) {
        free(list);
        return NULL;
    }
    list->arena = *arena;
    free(arena); // copied struct contents
    
    list->seed = (unsigned int)pthread_self() ^ (unsigned int)time(NULL);
    atomic_init(&list->max_height, 1);
    atomic_init(&list->size, 0);
    
    size_t head_alloc_size = sizeof(SkipNode) + (MAX_LEVEL * sizeof(_Atomic(SkipNode *))) + 0;
    SkipNode *head = (SkipNode *)arena_alloc(&list->arena, head_alloc_size);
    if (!head) {
        arena_destroy(&list->arena);
        free(list);
        return NULL;
    }
    
    head->key_len = 0;
    head->val_len = 0;
    head->lsn = 0;
    head->height = MAX_LEVEL;
    head->flags = 0;
    head->padding = 0;
    
    for (int i = 0; i < MAX_LEVEL; i++) {
        atomic_init(&head->forward[i], NULL);
    }
    
    atomic_init(&list->head, head);
    return list;
}

void lsm_skiplist_destroy(LSM_SkipList *list) {
    if (!list) return;
    arena_destroy(&list->arena);
    free(list);
}

bool lsm_skiplist_insert(LSM_SkipList *list, const char *key, uint32_t key_len, const char *val, uint32_t val_len, uint64_t lsn, bool deleted) {
    SkipNode *update[MAX_LEVEL];
    SkipNode *x = NULL;
    SkipNode *succ = NULL;
    
    int height = random_level(&list->seed);
    
    size_t total_node_size = sizeof(SkipNode) + (MAX_LEVEL * sizeof(_Atomic(SkipNode *))) +
                             align_up(key_len, 8) + align_up(val_len, 8);
                             
    SkipNode *new_node = (SkipNode *)arena_alloc(&list->arena, total_node_size);
    if (!new_node) return false;
    
    new_node->key_len = key_len;
    new_node->val_len = val_len;
    new_node->lsn = lsn;
    new_node->height = (uint8_t)height;
    new_node->flags = deleted ? 1 : 0;
    new_node->padding = 0;
    
    memcpy(get_node_key(new_node), key, key_len);
    if (val && val_len > 0) {
        memcpy(get_node_val(new_node), val, val_len);
    }
    
    for (int i = 0; i < height; i++) {
        atomic_init(&new_node->forward[i], NULL);
    }
    
    while (true) {
        int max_h = atomic_load(&list->max_height);
        x = atomic_load(&list->head);
        
        for (int i = max_h - 1; i >= 0; i--) {
            SkipNode *curr = atomic_load(&x->forward[i]);
            while (curr != NULL) {
                char *curr_key = get_node_key(curr);
                int cmp = compare_keys(curr_key, curr->key_len, key, key_len);
                if (cmp < 0) {
                    x = curr;
                    curr = atomic_load(&x->forward[i]);
                } else {
                    break;
                }
            }
            update[i] = x;
        }
        
        // Check if exact key exists
        SkipNode *target = atomic_load(&update[0]->forward[0]);
        if (target != NULL) {
            char *target_key = get_node_key(target);
            if (compare_keys(target_key, target->key_len, key, key_len) == 0) {
                // Key already exists, perform update via CAS chain
                new_node->height = target->height;
                for (int i = 0; i < target->height; i++) {
                    atomic_store(&new_node->forward[i], atomic_load(&target->forward[i]));
                }
                if (atomic_compare_exchange_strong(&update[0]->forward[0], &target, new_node)) {
                    // Update higher levels if necessary
                    for (int i = 1; i < target->height; i++) {
                        while (true) {
                            SkipNode *expected = target;
                            if (atomic_compare_exchange_weak(&update[i]->forward[i], &expected, new_node)) {
                                break;
                            }
                            // Re-traverse level i if CAS failed
                            x = atomic_load(&list->head);
                            for (int j = list->max_height - 1; j >= i; j--) {
                                SkipNode *curr = atomic_load(&x->forward[j]);
                                while (curr && compare_keys(get_node_key(curr), curr->key_len, key, key_len) < 0) {
                                    x = curr;
                                    curr = atomic_load(&x->forward[j]);
                                }
                                update[j] = x;
                            }
                        }
                    }
                    atomic_fetch_add(&list->size, 1);
                    return true;
                }
                continue; // Retry on CAS failure
            }
        }
        
        // Validate heights
        if (height > max_h) {
            for (int i = max_h; i < height; i++) {
                update[i] = atomic_load(&list->head);
            }
            int expected_max = max_h;
            atomic_compare_exchange_strong(&list->max_height, &expected_max, height);
        }
        
        // Link new node from bottom up
        for (int i = 0; i < height; i++) {
            while (true) {
                SkipNode *succ_node = atomic_load(&update[i]->forward[i]);
                atomic_store(&new_node->forward[i], succ_node);
                if (atomic_compare_exchange_weak(&update[i]->forward[i], &succ_node, new_node)) {
                    break;
                }
                // Contention detected at level i, re-seek update pointers for this level
                x = atomic_load(&list->head);
                for (int j = list->max_height - 1; j >= i; j--) {
                    SkipNode *curr = atomic_load(&x->forward[j]);
                    while (curr && compare_keys(get_node_key(curr), curr->key_len, key, key_len) < 0) {
                        x = curr;
                        curr = atomic_load(&x->forward[j]);
                    }
                    update[j] = x;
                }
            }
        }
        
        atomic_fetch_add(&list->size, 1);
        return true;
    }
}

bool lsm_skiplist_get(LSM_SkipList *list, const char *key, uint32_t key_len, char *val_out, uint32_t *val_len_inout, uint64_t *lsn_out, bool *deleted_out) {
    SkipNode *x = atomic_load(&list->head);
    int max_h = atomic_load(&list->max_height);
    
    for (int i = max_h - 1; i >= 0; i--) {
        SkipNode *curr = atomic_load(&x->forward[i]);
        while (curr != NULL) {
            char *curr_key = get_node_key(curr);
            int cmp = compare_keys(curr_key, curr->key_len, key, key_len);
            if (cmp < 0) {
                x = curr;
                curr = atomic_load(&x->forward[i]);
            } else {
                break;
            }
        }
    }
    
    SkipNode *target = atomic_load(&x->forward[0]);
    if (target != NULL) {
        char *target_key = get_node_key(target);
        if (compare_keys(target_key, target->key_len, key, key_len) == 0) {
            if (lsn_out) *lsn_out = target->lsn;
            if (deleted_out) *deleted_out = (target->flags & 1) != 0;
            
            uint32_t v_len = target->val_len;
            if (val_out && val_len_inout) {
                uint32_t copy_len = (*val_len_inout < v_len) ? *val_len_inout : v_len;
                memcpy(val_out, get_node_val(target), copy_len);
                *val_len_inout = v_len;
            } else if (val_len_inout) {
                *val_len_inout = v_len;
            }
            return true;
        }
    }
    
    return false;
}

bool lsm_skiplist_delete(LSM_SkipList *list, const char *key, uint32_t key_len, uint64_t lsn) {
    // Tombstone insertion
    return lsm_skiplist_insert(list, key, key_len, NULL, 0, lsn, true);
}

uint64_t lsm_skiplist_size(LSM_SkipList *list) {
    return atomic_load(&list->size);
}