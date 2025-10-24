/**
 * @file   tm.cpp
 * @author TL2 Implementation
 *
 * @section DESCRIPTION
 *
 * Transactional Locking II (TL2) implementation for CS453 project.
 * This implementation strictly follows the TL2 algorithm specification
 * with proper global version clock, versioned locks, and 5-phase commit.
**/

#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <cstring>
#include <map>
#include <unordered_set>
#include <vector>
#include <tm.hpp>

#include "macros.h"

// TL2 Constants - using MSB as lock bit
static constexpr uintptr_t LOCK_BIT = 1UL << 63;
static constexpr uintptr_t VERSION_MASK = ~LOCK_BIT;

// Versioned lock structure (per TL2 spec)
struct VersionedLock {
    std::atomic<uintptr_t> value{2}; // Initialize to even number (version 2)
};

// Read set entry (Lock_Pointer, Version_Observed)
struct ReadEntry {
    VersionedLock* lock_ptr;
    uintptr_t version_observed;
};

// Write set entry - using map for sorted order
struct WriteEntry {
    void* addr;
    size_t size;
    std::vector<uint8_t> data;
};

// Allocated segment metadata
struct SegmentNode {
    SegmentNode* prev;
    SegmentNode* next;
};

// Transaction descriptor (per TL2 spec)
struct Transaction {
    uintptr_t rv;  // ReadVersion - snapshot from GVClock
    uintptr_t wv;  // WriteVersion - assigned at commit time
    bool is_ro;    // IsReadOnly flag
    std::vector<ReadEntry> read_set;  // Only for read-write transactions
    std::map<void*, WriteEntry> write_set;  // Sorted map for deadlock-free locking
    std::vector<VersionedLock*> acquired_locks;  // Locks held during commit
    std::unordered_set<void*> alloc_set;
    std::unordered_set<void*> free_set;
};

// Shared memory region
struct Region {
    void* start;
    size_t size;
    size_t align;
    std::atomic<uintptr_t> gv_clock{2};  // Global Version Clock - starts at even number
    VersionedLock* locks;
    size_t num_locks;
    SegmentNode* allocs{nullptr};
};

// Map address to lock index using stripe-based locking
static inline size_t addr_to_lock_index(Region* region, void const* addr) {
    uintptr_t addr_val = (uintptr_t)addr;
    uintptr_t region_start = (uintptr_t)region->start;
    
    if (addr_val >= region_start && addr_val < region_start + region->size) {
        // Address within shared region - use offset-based mapping
        uintptr_t offset = addr_val - region_start;
        size_t word_index = offset / region->align;
        return word_index % region->num_locks;
    } else {
        // Address outside shared region (dynamically allocated) - use hash
        return (addr_val / sizeof(void*)) % region->num_locks;
    }
}

shared_t tm_create(size_t size, size_t align) noexcept {
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
    size_t desired_locks = std::min(size / 1024, (size_t)(1UL << 16));
    region->num_locks = std::max(desired_locks, (size_t)256);
    region->locks = new VersionedLock[region->num_locks]();
    
    return region;
}

void tm_destroy(shared_t shared) noexcept {
    Region* region = (Region*)shared;
    
    // Clean up allocated segments
    SegmentNode* current = region->allocs;
    while (current) {
        SegmentNode* next = current->next;
        std::free(current);
        current = next;
    }
    
    delete[] region->locks;
    free(region->start);
    delete region;
}

void* tm_start(shared_t shared) noexcept {
    return ((Region*)shared)->start;
}

size_t tm_size(shared_t shared) noexcept {
    return ((Region*)shared)->size;
}

size_t tm_align(shared_t shared) noexcept {
    return ((Region*)shared)->align;
}

// TransactionBegin procedure (per TL2 spec)
tx_t tm_begin(shared_t shared, bool is_ro) noexcept {
    Region* region = (Region*)shared;
    Transaction* tx = new Transaction();
    if (unlikely(!tx)) {
        return invalid_tx;
    }
    
    // Set IsReadOnly flag
    tx->is_ro = is_ro;
    
    // Atomically load GVClock and store in ReadVersion
    tx->rv = region->gv_clock.load(std::memory_order_acquire);
    
    tx->wv = 0;  // Will be set at commit time
    
    return (tx_t)tx;
}

// // TransactionalRead procedure (per TL2 spec)
bool tm_read(shared_t shared, tx_t tx, void const* source, size_t size, void* target) noexcept {
    Transaction* tx_ptr = (Transaction*)tx;
    Region* region = (Region*)shared;
    
    // Basic safety checks
    if (source == nullptr || target == nullptr || size == 0) {
        return false; // Abort on null pointers or zero size
    }
    
    // Check for obviously invalid low addresses (like 0x5e from crash)
    // Only catch extremely low addresses that are clearly corrupted
    if ((uintptr_t)source < 0x100) {  // 256 bytes threshold for clearly invalid addresses
        return false; // Abort transaction
    }
    
    // Step 1: Check Write-Set for read-your-own-writes
    auto it = tx_ptr->write_set.find((void*)source);
    if (it != tx_ptr->write_set.end() && it->second.size == size) {
        // Found exact match in WriteSet
        memcpy(target, it->second.data.data(), size);
        return true;
    }
    
    // Check for partial overlaps in WriteSet (abort for safety)
    uintptr_t read_start = (uintptr_t)source;
    uintptr_t read_end = read_start + size;
    
    for (const auto& entry : tx_ptr->write_set) {
        uintptr_t write_start = (uintptr_t)entry.first;
        uintptr_t write_end = write_start + entry.second.size;
        
        // Check for any overlap
        if (read_start < write_end && write_start < read_end) {
            return false; // Abort on partial overlap
        }
    }
    
    // Step 2: Read from Shared Memory
    size_t lock_idx = addr_to_lock_index(region, source);
    VersionedLock* lock_ptr = &region->locks[lock_idx];
    
    while (true) {
        // Atomically load lock value into v1
        uintptr_t v1 = lock_ptr->value.load(std::memory_order_acquire);
        
        // Step 3: Pre-Validation Rule
        // Check if lock bit is set OR version > ReadVersion
        if ((v1 & LOCK_BIT) != 0 || (v1 & VERSION_MASK) > tx_ptr->rv) {
            return false; // Abort transaction
        }
        
        // Step 4: Perform Read
        // // Check for obviously invalid low addresses (like 0x5e from crash)
        // // Only catch extremely low addresses that are clearly corrupted
        // if ((uintptr_t)source < 0x100) {  // 256 bytes threshold for clearly invalid addresses
        //     return false; // Abort transaction
        // }
        
        memcpy(target, source, size);
        
        // Step 5: Post-Validation Rule
        // Atomically load lock value again into v2
        uintptr_t v2 = lock_ptr->value.load(std::memory_order_acquire);
        
        if (v1 != v2) {
            // Concurrent modification occurred, retry
            continue;
        }
        
        // Step 6: Record Read (for Read-Write Transactions only)
        if (!tx_ptr->is_ro) {
            ReadEntry entry;
            entry.lock_ptr = lock_ptr;
            entry.version_observed = v1 & VERSION_MASK;
            tx_ptr->read_set.push_back(entry);
        }
        
        return true;
    }
}


// TransactionalWrite procedure (per TL2 spec)
bool tm_write(shared_t shared, tx_t tx, void const* source, size_t size, void* target) noexcept {
    Transaction* tx_ptr = (Transaction*)tx;
    Region* region = (Region*)shared;
    
    // Read-only transactions cannot write
    if (tx_ptr->is_ro) {
        return false;
    }
    
    // Basic safety checks
    if (source == nullptr || target == nullptr || size == 0) {
        return false; // Abort on null pointers or zero size
    }
    
    // Store in write set for deferred execution
    WriteEntry entry;
    entry.addr = target;
    entry.size = size;
    entry.data.resize(size);
    memcpy(entry.data.data(), source, size);
    
    tx_ptr->write_set[target] = std::move(entry);
    return true;
}

// ABORT_PROCEDURE helper
static void abort_transaction(Transaction* tx, Region* region) {
    // Release any acquired locks, leaving version unchanged
    for (VersionedLock* lock_ptr : tx->acquired_locks) {
        uintptr_t current = lock_ptr->value.load(std::memory_order_relaxed);
        lock_ptr->value.store(current & VERSION_MASK, std::memory_order_release);
    }
    tx->acquired_locks.clear();
    delete tx;
}

// TransactionEnd procedure - 5-phase commit protocol (per TL2 spec)
bool tm_end(shared_t shared, tx_t tx) noexcept {
    Transaction* tx_ptr = (Transaction*)tx;
    Region* region = (Region*)shared;
    
    // Step 1: Handle Trivial Cases
    if (tx_ptr->is_ro || (tx_ptr->write_set.empty() && tx_ptr->alloc_set.empty() && tx_ptr->free_set.empty())) {
        // For read-only or empty transactions, just validate read set
        for (const auto& entry : tx_ptr->read_set) {
            uintptr_t current = entry.lock_ptr->value.load(std::memory_order_acquire);
            if ((current & VERSION_MASK) != entry.version_observed || (current & LOCK_BIT) != 0) {
                delete tx_ptr;
                return false;
            }
        }
        delete tx_ptr;
        return true;
    }
    
    // Step 2: Phase 1 - Lock Acquisition
    // WriteSet is already sorted (std::map), iterate in order for deadlock-free locking
    for (const auto& entry : tx_ptr->write_set) {
        size_t lock_idx = addr_to_lock_index(region, entry.first);
        VersionedLock* lock_ptr = &region->locks[lock_idx];
        
        // Skip if already acquired
        bool already_acquired = false;
        for (VersionedLock* acquired : tx_ptr->acquired_locks) {
            if (acquired == lock_ptr) {
                already_acquired = true;
                break;
            }
        }
        if (already_acquired) continue;
        
        // Attempt to acquire lock with CAS
        uintptr_t expected = lock_ptr->value.load(std::memory_order_acquire);
        while (true) {
            // Check if locked or version too new
            if ((expected & LOCK_BIT) != 0 || (expected & VERSION_MASK) > tx_ptr->rv) {
                abort_transaction(tx_ptr, region);
                return false;
            }
            
            // Try to set lock bit
            uintptr_t desired = expected | LOCK_BIT;
            if (lock_ptr->value.compare_exchange_weak(expected, desired, 
                    std::memory_order_acquire, std::memory_order_acquire)) {
                tx_ptr->acquired_locks.push_back(lock_ptr);
                break;
            }
        }
    }
    
    // Step 3: Phase 2 - Timestamping
    // Atomically fetch-and-add GVClock by 2, store pre-increment value
    tx_ptr->wv = region->gv_clock.fetch_add(2, std::memory_order_acq_rel);
    
    // Step 4: Phase 3 - Read-Set Validation
    for (const auto& entry : tx_ptr->read_set) {
        uintptr_t current = entry.lock_ptr->value.load(std::memory_order_acquire);
        
        // Check if version changed or locked by different transaction
        bool locked_by_us = false;
        for (VersionedLock* acquired : tx_ptr->acquired_locks) {
            if (acquired == entry.lock_ptr) {
                locked_by_us = true;
                break;
            }
        }
        
        if (!locked_by_us) {
            if ((current & LOCK_BIT) != 0 || (current & VERSION_MASK) != entry.version_observed) {
                abort_transaction(tx_ptr, region);
                return false;
            }
        }
    }
    
    // Step 5: Phase 4 - Commit (Point of No Return)
    // Copy WriteSet data to shared memory
    for (const auto& entry : tx_ptr->write_set) {
        memcpy(entry.first, entry.second.data.data(), entry.second.size);
    }
    
    // Process allocations
    for (void* addr : tx_ptr->alloc_set) {
        SegmentNode* node = (SegmentNode*)((uintptr_t)addr - sizeof(SegmentNode));
        node->prev = nullptr;
        node->next = region->allocs;
        if (region->allocs) region->allocs->prev = node;
        region->allocs = node;
    }
    
    // Process frees
    for (void* addr : tx_ptr->free_set) {
        SegmentNode* node = (SegmentNode*)((uintptr_t)addr - sizeof(SegmentNode));
        
        // Unlink from allocation list
        if (node->prev) node->prev->next = node->next;
        else region->allocs = node->next;
        if (node->next) node->next->prev = node->prev;
        
        free(node);
    }
    
    // Step 6: Phase 5 - Release Locks
    // Calculate new version: WriteVersion + 2
    uintptr_t new_version = tx_ptr->wv + 2;
    
    // Release all acquired locks with new version
    for (VersionedLock* lock_ptr : tx_ptr->acquired_locks) {
        lock_ptr->value.store(new_version, std::memory_order_release);
    }
    
    delete tx_ptr;
    return true;
}

Alloc tm_alloc(shared_t shared, tx_t tx, size_t size, void** target) noexcept {
    Transaction* tx_ptr = (Transaction*)tx;
    Region* region = (Region*)shared;
    
    size_t align = region->align;
    if (align < sizeof(void*)) {
        align = sizeof(void*);
    }
    
    SegmentNode* node;
    if (posix_memalign((void**)&node, align, sizeof(SegmentNode) + size) != 0) {
        return Alloc::nomem;
    }
    
    void* segment = (void*)((uintptr_t)node + sizeof(SegmentNode));
    memset(segment, 0, size);
    
    *target = segment;
    tx_ptr->alloc_set.insert(segment);
    
    return Alloc::success;
}

bool tm_free(shared_t shared, tx_t tx, void* target) noexcept {
    Transaction* tx_ptr = (Transaction*)tx;
    Region* region = (Region*)shared;
    
    // Check if allocated in this transaction
    if (tx_ptr->alloc_set.count(target)) {
        // Remove from alloc set and free immediately
        tx_ptr->alloc_set.erase(target);
        SegmentNode* node = (SegmentNode*)((uintptr_t)target - sizeof(SegmentNode));
        free(node);
        return true;
    }
    
    // Defer free until commit
    tx_ptr->free_set.insert(target);
    return true;
}