/**
 * @file   tm.cpp
 * @author TL2 Implementation
 *
 * @section DESCRIPTION
 *
 * Transactional Locking II (TL2) implementation for CS453 project.
 * This implementation follows the TL2 algorithm with global version clock,
 * versioned write-locks, and commit-time locking.
**/

#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <cstring>
#include <unordered_map>
#include <unordered_set>
#include <vector>

extern "C" {
#include <tm.h>
}
#include "macros.h"

// TL2 Constants
static constexpr uintptr_t LOCK_BIT = 1UL << 63;
static constexpr uintptr_t VERSION_MASK = ~LOCK_BIT;

// Versioned lock structure
struct VersionedLock {
    std::atomic<uintptr_t> value{0}; // MSB: lock bit, rest: version number
};

// Global version clock (one per shared memory region would be better, but using global for simplicity)
static std::atomic<uintptr_t> global_clock{0};

// Read set entry
struct ReadEntry {
    void* addr;
    size_t lock_idx;
    uintptr_t version;
};

// Write set entry
struct WriteEntry {
    void* addr;
    size_t size;
    std::vector<uint8_t> data;
    size_t lock_idx;
};

// Allocated segment metadata
struct SegmentNode {
    SegmentNode* prev;
    SegmentNode* next;
};

// Transaction descriptor
struct Transaction {
    uintptr_t rv;  // Read version number
    uintptr_t wv;  // Write version number
    bool is_ro;    // Read-only transaction flag
    std::vector<ReadEntry> read_set;
    std::vector<WriteEntry> write_set;
    std::unordered_set<void*> alloc_set;
    std::unordered_set<void*> free_set;
    std::unordered_set<size_t> locked_indices;
};

// Shared memory region
struct Region {
    void* start;
    size_t size;
    size_t align;
    SegmentNode* allocs;
    size_t num_locks;
    VersionedLock* locks;
    std::atomic<uintptr_t> local_clock{0}; // Per-region clock for better scalability
};

// Map address to lock index using stripe-based locking
static inline size_t addr_to_lock_index(Region* region, void const* addr) {
    uintptr_t offset = (uintptr_t)addr - (uintptr_t)region->start;
    size_t word_index = offset / region->align;
    return word_index % region->num_locks;
}

// Validate read set
static bool validate_read_set(Transaction* tx, Region* region) {
    for (const auto& entry : tx->read_set) {
        // Skip validation for locks we hold
        if (tx->locked_indices.find(entry.lock_idx) != tx->locked_indices.end()) {
            continue;
        }
        
        uintptr_t lock_val = region->locks[entry.lock_idx].value.load(std::memory_order_acquire);
        
        // Check if locked by another transaction
        if ((lock_val & LOCK_BIT) != 0) {
            return false;
        }
        
        // Check if version changed
        uintptr_t version = lock_val & VERSION_MASK;
        if (version != entry.version) {
            return false;
        }
    }
    return true;
}

shared_t tm_create(size_t size, size_t align) {
    Region* region = new Region();
    if (unlikely(!region)) {
        return invalid_shared;
    }
    
    if (posix_memalign(&region->start, align, size) != 0) {
        delete region;
        return invalid_shared;
    }
    
    region->size = size;
    region->align = align;
    region->allocs = nullptr;
    
    // Create lock table (stripe-based)
    // Use a fixed modest size that scales well for all memory sizes
    size_t desired_locks = std::min(size / 1024, (size_t)(1UL << 16));
    region->num_locks = std::max(desired_locks, (size_t)256);
    region->locks = new VersionedLock[region->num_locks]();
    
    return region;
}

void tm_destroy(shared_t shared) {
    Region* region = (Region*)shared;
    
    // Free all allocated segments
    while (region->allocs) {
        SegmentNode* next = region->allocs->next;
        free(region->allocs);
        region->allocs = next;
    }
    
    delete[] region->locks;
    free(region->start);
    delete region;
}

void* tm_start(shared_t shared) {
    return ((Region*)shared)->start;
}

size_t tm_size(shared_t shared) {
    return ((Region*)shared)->size;
}

size_t tm_align(shared_t shared) {
    return ((Region*)shared)->align;
}

tx_t tm_begin(shared_t shared, bool is_ro) {
    Region* region = (Region*)shared;
    Transaction* tx = new Transaction();
    if (unlikely(!tx)) {
        return invalid_tx;
    }
    
    tx->is_ro = is_ro;
    // Sample global version clock
    tx->rv = region->local_clock.load(std::memory_order_acquire);
    tx->wv = 0;
    
    return (tx_t)tx;
}

// Lock write set in sorted order to avoid deadlock
static bool lock_write_set(Transaction* tx, Region* region) {
    // Collect unique lock indices
    std::vector<size_t> indices;
    for (const auto& entry : tx->write_set) {
        indices.push_back(entry.lock_idx);
    }
    
    // Sort and remove duplicates
    std::sort(indices.begin(), indices.end());
    indices.erase(std::unique(indices.begin(), indices.end()), indices.end());
    
    // Try to acquire locks in order
    for (size_t idx : indices) {
        if (tx->locked_indices.count(idx)) {
            continue; // Already locked
        }
        
        uintptr_t expected = region->locks[idx].value.load(std::memory_order_acquire);
        while (true) {
            // Check if locked
            if (expected & LOCK_BIT) {
                return false;
            }
            
            // Check if version is too new
            if ((expected & VERSION_MASK) > tx->rv) {
                return false;
            }
            
            // Try to acquire lock
            uintptr_t desired = expected | LOCK_BIT;
            if (region->locks[idx].value.compare_exchange_weak(
                    expected, desired,
                    std::memory_order_acquire,
                    std::memory_order_acquire)) {
                tx->locked_indices.insert(idx);
                break;
            }
        }
    }
    return true;
}

// Release all locks held by transaction
static void unlock_write_set(Transaction* tx, Region* region) {
    for (size_t idx : tx->locked_indices) {
        uintptr_t lock_val = region->locks[idx].value.load(std::memory_order_relaxed);
        region->locks[idx].value.store(lock_val & VERSION_MASK, std::memory_order_release);
    }
    tx->locked_indices.clear();
}

bool tm_end(shared_t shared, tx_t tx_id) {
    Transaction* tx = (Transaction*)tx_id;
    Region* region = (Region*)shared;
    
    // Read-only transaction: just validate and commit
    if (tx->is_ro) {
        bool valid = validate_read_set(tx, region);
        delete tx;
        return valid;
    }
    
    // Empty write set: validate and commit
    if (tx->write_set.empty() && tx->alloc_set.empty() && tx->free_set.empty()) {
        bool valid = validate_read_set(tx, region);
        delete tx;
        return valid;
    }
    
    // Lock write set
    if (!lock_write_set(tx, region)) {
        unlock_write_set(tx, region);
        delete tx;
        return false;
    }
    
    // Increment global clock
    tx->wv = region->local_clock.fetch_add(1, std::memory_order_acq_rel) + 1;
    
    // Validate read set
    if (!validate_read_set(tx, region)) {
        unlock_write_set(tx, region);
        delete tx;
        return false;
    }
    
    // Commit writes
    for (const auto& entry : tx->write_set) {
        memcpy(entry.addr, entry.data.data(), entry.size);
    }
    
    // Process frees
    for (void* addr : tx->free_set) {
        SegmentNode* node = (SegmentNode*)((uintptr_t)addr - sizeof(SegmentNode));
        if (node->prev) node->prev->next = node->next;
        else region->allocs = node->next;
        if (node->next) node->next->prev = node->prev;
        free(node);
    }
    
    // Release locks with new version
    for (size_t idx : tx->locked_indices) {
        region->locks[idx].value.store(tx->wv, std::memory_order_release);
    }
    
    delete tx;
    return true;
}

bool tm_read(shared_t shared, tx_t tx_id, void const* source, size_t size, void* target) {
    Transaction* tx = (Transaction*)tx_id;
    Region* region = (Region*)shared;
    
    // Check write set for read-after-write
    uintptr_t read_start = (uintptr_t)source;
    uintptr_t read_end = read_start + size;
    
    for (const auto& entry : tx->write_set) {
        uintptr_t write_start = (uintptr_t)entry.addr;
        uintptr_t write_end = write_start + entry.size;
        
        // Check for overlap
        if (read_start < write_end && write_start < read_end) {
            // Exact match
            if (read_start == write_start && size == entry.size) {
                memcpy(target, entry.data.data(), size);
                return true;
            }
            // Partial overlap - abort for safety
            return false;
        }
    }
    
    // Read from shared memory with version validation
    size_t lock_idx = addr_to_lock_index(region, source);
    
    while (true) {
        uintptr_t lock_val_before = region->locks[lock_idx].value.load(std::memory_order_acquire);
        
        // Wait if locked
        if ((lock_val_before & LOCK_BIT) != 0) {
            continue;
        }
        
        // Perform the read
        memcpy(target, source, size);
        
        // Validate version didn't change
        uintptr_t lock_val_after = region->locks[lock_idx].value.load(std::memory_order_acquire);
        
        if (lock_val_before == lock_val_after) {
            uintptr_t version = lock_val_after & VERSION_MASK;
            
            // Check version against read version
            if (version > tx->rv) {
                return false; // Abort
            }
            
            // Add to read set (if not read-only or small transaction)
            if (!tx->is_ro) {
                tx->read_set.push_back({(void*)source, lock_idx, version});
            }
            
            return true;
        }
    }
}

bool tm_write(shared_t shared, tx_t tx_id, void const* source, size_t size, void* target) {
    Transaction* tx = (Transaction*)tx_id;
    Region* region = (Region*)shared;
    
    size_t lock_idx = addr_to_lock_index(region, target);
    
    // Check if already in write set
    for (auto& entry : tx->write_set) {
        if (entry.addr == target && entry.size == size) {
            memcpy(entry.data.data(), source, size);
            return true;
        }
    }
    
    // Add to write set
    WriteEntry entry;
    entry.addr = target;
    entry.size = size;
    entry.lock_idx = lock_idx;
    entry.data.resize(size);
    memcpy(entry.data.data(), source, size);
    tx->write_set.push_back(std::move(entry));
    
    return true;
}

alloc_t tm_alloc(shared_t shared, tx_t tx_id, size_t size, void** target) {
    Transaction* tx = (Transaction*)tx_id;
    Region* region = (Region*)shared;
    
    size_t align = region->align;
    if (align < sizeof(void*)) {
        align = sizeof(void*);
    }
    
    SegmentNode* node;
    if (posix_memalign((void**)&node, align, sizeof(SegmentNode) + size) != 0) {
        return nomem_alloc;
    }
    
    // Link into allocation list
    node->prev = nullptr;
    node->next = region->allocs;
    if (node->next) node->next->prev = node;
    region->allocs = node;
    
    void* segment = (void*)((uintptr_t)node + sizeof(SegmentNode));
    memset(segment, 0, size);
    *target = segment;
    
    tx->alloc_set.insert(segment);
    
    return success_alloc;
}

bool tm_free(shared_t shared, tx_t tx_id, void* target) {
    Transaction* tx = (Transaction*)tx_id;
    Region* region = (Region*)shared;
    
    // Check if allocated in this transaction
    if (tx->alloc_set.count(target)) {
        // Remove from alloc set and free immediately
        tx->alloc_set.erase(target);
        SegmentNode* node = (SegmentNode*)((uintptr_t)target - sizeof(SegmentNode));
        if (node->prev) node->prev->next = node->next;
        else region->allocs = node->next;
        if (node->next) node->next->prev = node->prev;
        free(node);
        return true;
    }
    
    // Defer free until commit
    tx->free_set.insert(target);
    return true;
}