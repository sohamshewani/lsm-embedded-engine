#define _GNU_SOURCE
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <stdatomic.h>
#include <pthread.h>
#include <sys/mman.h>
#include <unistd.h>
#include <errno.h>

#define LSM_CACHE_LINE_SIZE 64
#define LSM_ARENA_BLOCK_SIZE (2 * 1024 * 1024) // 2MB blocks
#define LSM_ALIGN_UP(val, align) (((val) + ((align) - 1)) & ~((align) - 1))

typedef struct ArenaBlock {
    char *memory;
    size_t capacity;
    _Atomic size_t offset;
    struct ArenaBlock *next;
} ArenaBlock;

typedef struct {
    ArenaBlock *head;
    ArenaBlock *current;
    pthread_spinlock_t lock;
    size_t total_allocated;
} lsm_arena_t;

lsm_arena_t* lsm_arena_create(void) {
    lsm_arena_t *arena = (lsm_arena_t *)malloc(sizeof(lsm_arena_t));
    if (!arena) return NULL;

    ArenaBlock *block = (ArenaBlock *)malloc(sizeof(ArenaBlock));
    if (!block) {
        free(arena);
        return NULL;
    }

    // Allocate memory aligned to cache line
    if (posix_memalign((void **)&block->memory, LSM_CACHE_LINE_SIZE, LSM_ARENA_BLOCK_SIZE) != 0) {
        free(block);
        free(arena);
        return NULL;
    }

    block->capacity = LSM_ARENA_BLOCK_SIZE;
    atomic_init(&block->offset, 0);
    block->next = NULL;

    arena->head = block;
    arena->current = block;
    arena->total_allocated = LSM_ARENA_BLOCK_SIZE;

    if (pthread_spin_init(&arena->lock, PTHREAD_PROCESS_PRIVATE) != 0) {
        free(block->memory);
        free(block);
        free(arena);
        return NULL;
    }

    return arena;
}

void* lsm_arena_alloc(lsm_arena_t *arena, size_t size) {
    if (!arena || size == 0) return NULL;

    // Align request size to 8 bytes for safety
    size_t aligned_size = LSM_ALIGN_UP(size, 8);
    
    pthread_spin_lock(&arena->lock);

    ArenaBlock *curr = arena->current;
    size_t old_offset = atomic_load_explicit(&curr->offset, memory_order_relaxed);

    while (1) {
        if (old_offset + aligned_size <= curr->capacity) {
            // Try to claim space atomically
            if (atomic_compare_exchange_weak_explicit(&curr->offset, &old_offset, old_offset + aligned_size,
                                                      memory_order_relaxed, memory_order_relaxed)) {
                void *ptr = (void *)(curr->memory + old_offset);
                pthread_spin_unlock(&arena->lock);
                return ptr;
            }
            // If CAS failed, old_offset is updated, loop and re-check
            continue;
        }

        // Current block is full, check if we need to allocate a new block
        if (curr->next == NULL) {
            size_t alloc_size = (size > LSM_ARENA_BLOCK_SIZE) ? LSM_ALIGN_UP(size, LSM_ARENA_BLOCK_SIZE) : LSM_ARENA_BLOCK_SIZE;
            ArenaBlock *new_block = (ArenaBlock *)malloc(sizeof(ArenaBlock));
            if (!new_block) {
                pthread_spin_unlock(&arena->lock);
                return NULL;
            }

            if (posix_memalign((void **)&new_block->memory, LSM_CACHE_LINE_SIZE, alloc_size) != 0) {
                free(new_block);
                pthread_spin_unlock(&arena->lock);
                return NULL;
            }

            new_block->capacity = alloc_size;
            atomic_init(&new_block->offset, 0);
            new_block->next = NULL;

            arena->total_allocated += alloc_size;
            curr->next = new_block;
        }

        arena->current = curr->next;
        curr = arena->current;
        old_offset = atomic_load_explicit(&curr->offset, memory_order_relaxed);
    }
}

void lsm_arena_destroy(lsm_arena_t *arena) {
    if (!arena) return;

    pthread_spin_lock(&arena->lock);
    ArenaBlock *curr = arena->head;
    while (curr) {
        ArenaBlock *next = curr->next;
        free(curr->memory);
        free(curr);
        curr = next;
    }
    pthread_spin_unlock(&arena->lock);

    pthread_spin_destroy(&arena->lock);
    free(arena);
}

size_t lsm_arena_memory_usage(const lsm_arena_t *arena) {
    if (!arena) return 0;
    // Note: returning total capacity allocated, thread-safe snapshot via relaxed loads
    return arena->total_allocated;
}