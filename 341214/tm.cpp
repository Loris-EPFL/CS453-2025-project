/**
 * @file   tm.cpp
 * @author Loris Tran, sciper 341214
 *
 * @section DESCRIPTION
 *
 * Transactional Locking II (TL2) implementation for the CS453 project.
 * This implementation follows the TL2 algorithm specification
 * with proper global version clock, versioned locks, and 5-phase commit from the TL2 paper.
**/

#include <atomic>
#include <cstdlib>
#include <cstring>
#include <map>
#include <mutex>
#include <unordered_set>
#include <vector>
#include <tm.hpp>

#include "macros.h"

// Lock bit is stored on Most Significant Bit, version is everything else
static constexpr uintptr_t LOCK_BIT = 1UL << 63;
static constexpr uintptr_t VERSION_MASK = ~LOCK_BIT;

// Versioned lock structure (per TL2 spec)
struct VersionedLock {
    std::atomic<uintptr_t> value{2};  // starts at version 2
};

// Read set entry (Lock_Pointer, Version_Observed)
struct ReadEntry {
    VersionedLock* lock_ptr; //Pointer to the lock protecting the memory location read
    uintptr_t version_observed; //Version observed at read time
};

// Write set entry
struct WriteEntry {
    void* addr; //Pointer to the shared memory where data will be written
    size_t size; //	Number of bytes to write
    std::vector<uint8_t> data; // Copy of the data to write (buffered locally until commit)
};

// Double Linked list node for tracking allocated segments
// node  placed before the actual segment data in memory. visible pointer is node + sizeof(SegmentNode)
struct SegmentNode {
    SegmentNode* prev; //Previous element in the list
    SegmentNode* next; //Next element in the list
};

// Transaction structure for TL2
struct Transaction {
    uintptr_t rv;  // ReadVersion snapshot from Global Versionning Clock taken in tm_begin. All reads must see versions <= this value.
    uintptr_t wv;  // WriteVersion assigned at commit time on Global Versionning Clock. Written to locks when releasing them.
    bool is_ro;    // IsReadOnly flag
    std::vector<ReadEntry> read_set;  // All reads performed are validated at commit to ensure no concurrent modifications
    std::map<void*, WriteEntry> write_set;  // Buffered writes set, sorted by address so locks are acquired to prevents deadlock.
    std::unordered_set<VersionedLock*> acquired_locks;  // Locks we've acquired during commit. Use unordered_set for O(1) lookup
    std::unordered_set<void*> alloc_set; // Segments allocated in this transaction (not yet committed)
    std::unordered_set<void*> free_set; // Segments to free at commit
    std::map<void*, size_t> alloc_sizes;  // Track sizes of allocated segments
    

    //Util function to clean up a whole transaction statet (put everything to 0, clear all sets)
    void reset() {
        rv = wv = 0;
        is_ro = false;
        read_set.clear();
        for (auto& entry : write_set) {
            entry.second.data.clear();
        }
        write_set.clear();
        acquired_locks.clear();
        alloc_set.clear();
        free_set.clear();
        alloc_sizes.clear();
    }
};

// Thread-local transaction for each thread to avoid heap allocation at tm_begin
static thread_local Transaction tl_transaction;

// Shared memory region from the project description (non overlappin shared memory segments)
struct Region {
    void* start; //Region start pointer  to beginning of the shared memory
    size_t size; //Region total size (sum of all segments)
    size_t align; //Region alignment
    std::atomic<uintptr_t> gv_clock{2};  // Global Version Clock :  starts at even number, monotonically increasing, incremented by 2 on each commit per TL2 requirements
    VersionedLock* locks; // version lock array pointer , i.e lock table
    size_t num_locks; //Total number of locksq in the table
    SegmentNode* allocs{nullptr}; //Head of linked list of dynamically allocated segments
    std::atomic<size_t> segment_count{0};  // Track number of allocated segments (max 65536)
    std::mutex alloc_mutex;                // Protect allocation list  during concurrent modifications
};

/**
Maps a memory address to its protecting lock in the lock table.
    @param region Shared memory region associated with the transaction
    @param addr Pointer to the first word of the allocated segment
    @return Lock index corresponding to the address
*/
static inline size_t addr_to_lock_index(Region* region, void const* addr) {
    uintptr_t addr_val = (uintptr_t)addr;
    uintptr_t region_start = (uintptr_t)region->start;

    //If address is within shared region
    if (addr_val >= region_start && addr_val < region_start + region->size) {
        // Address within shared region with offset-based mapping
        uintptr_t offset = addr_val - region_start;
        size_t word_index = offset / region->align;
        return word_index % region->num_locks;
    }

    // Else Address outside shared region (dynamically allocated)
    return (addr_val / sizeof(void*)) % region->num_locks;
    
}


/**
Create (i.e. allocate + init) a new shared memory region, with one first allocated segment of the requested size and alignment.
    @param size Size of the first allocated segment of memory (in bytes), must be a positive multiple of the alignment and at most 2^48
    @param align Alignment (in bytes, must be a power of 2) that the shared memory region must provide, and that each memory access made on this shared memory region will have to satisfy
    @return Opaque shared memory region handle, invalid shared on failure.
    @note - The requested alignment in that function will be the alignment assumed in every subsequent memory operation.
          - The first allocated segment must be initialized with zeroes.
          - The first allocated segment cannot be freed with tm free.
          - The alignment also defines the size of words, and thus the granularity of the transactional memory.
          - This function can be called concurrently.
*/
shared_t tm_create(size_t size, size_t align) noexcept {
    // Validate alignment is a power of 2
    if (align == 0 || (align & (align - 1)) != 0) {
        return invalid_shared;
    }
    
    // Validate size is a positive multiple of alignment and at most 2^48
    if (size == 0 || size % align != 0 || size > (1ULL << 48)) {
        return invalid_shared;
    }
    
    // Allocate a new region
    Region* region = new Region(); 
    if (unlikely(!region)) {
        return invalid_shared;
    }
    //error in memory alloc returns 0
    if (posix_memalign(&region->start, align, size) != 0) {
        delete region;
        return invalid_shared;
    }
    
    // Initialize the first allocated segment with zeroes as per specification
    memset(region->start, 0, size);
    
    region->size = size;
    region->align = align;
    region->allocs = nullptr;
    
    // Create lock table : use many locks to minimize false conflicts
    size_t desired_locks = std::min(size / region->align, (size_t)(1UL << 20));
    region->num_locks = std::max(desired_locks, (size_t)4096);
    region->locks = new VersionedLock[region->num_locks]();
    
    return region;
}

/**
Destroy (i.e. clean-up + free) a given shared memory region.
    @param shared Handle of the shared memory region to destroy
    @note - No concurrent call for the same shared memory region.
          - It is guaranteed that when this function is called the associated shared memory region has not been destroyed yet.
          - It is guaranteed that no transaction is running on the shared memory region when this function is called.
          - The first allocated segment, and all the segments that were allocated with tm alloc but not freed with tm free at the time of the call, must be freed by this function.
*/
void tm_destroy(shared_t shared) noexcept {
    Region* region = (Region*)shared;
    
    // free all allocated segments
    SegmentNode* current = region->allocs;
    while (current) {
        SegmentNode* next = current->next;
        std::free(current);
        current = next;
    }
    
    //Delete lock table
    delete[] region->locks;
    
    //Free shared memory region
    free(region->start);
    
    //Delete region
    delete region;
}

/**
Get a pointer in shared memory to the first allocated segment of the shared memory region.
    @param shared Handle of the shared memory region to query
    @return Pointer in shared memory to the first word of the first allocated segment.
    @note - This function can be called concurrently.
          - The returned address must be aligned on the shared region alignment.
          - This function never fails: it must always return the address of the first allocated segment, which is not free-able.
          - The returned pointer must not be NULL (or nullptr in C++), and must not change between invocations.
*/
void* tm_start(shared_t shared) noexcept {
    return ((Region*)shared)->start; //Return pointer to first allocated segment
}

/**
Get the size to the first allocated segment of the shared memory region.
    @param shared Handle of the shared memory region to query
    @return Size (in bytes) of the first allocated segment.
    @note - This function can be called concurrently.
          - The returned size must be a multiple of the shared region alignment.
          - This function never fails: it must always return the size of the first allocated segment, which is not free-able.
          - The size of the first allocated segment is a constant, set with tm create.
*/
size_t tm_size(shared_t shared) noexcept {
    return ((Region*)shared)->size; //Return size of first allocated segment
}

/**
Get the required alignment for memory accesses on the shared memory region.
    @param shared Handle of the shared memory region to query
    @return Alignment used for this shared memory region (in bytes).
    @note - This function can be called concurrently.
          - The alignment of the shared memory region is a constant, set with tm create.
*/
size_t tm_align(shared_t shared) noexcept {
    return ((Region*)shared)->align; //Return alignment of shared memory region
}


/**
Begin a new transaction on the given shared memory region.
    @param shared Shared memory region to begin a transaction on
    @param is_ro Whether the transaction will only perform read(s)
    @return Opaque transaction handle, invalid tx on failure.
    @note - This function can be called concurrently.
          - There is no concept of nested transactions, i.e. one transaction begun in another transaction.
          - If is ro is set to true, then only tm read will be called from this transaction.
*/
tx_t tm_begin(shared_t shared, bool is_ro) noexcept {
    Region* region = (Region*)shared;
    
    // Use thread-local transaction , first we reset it for reuse
    tl_transaction.reset();
    
    // Set IsReadOnly flag
    tl_transaction.is_ro = is_ro;
    
    // Atomically load Global Versionning Clock and store in ReadVersion set
    tl_transaction.rv = region->gv_clock.load(std::memory_order_acquire);
    
    tl_transaction.wv = 0;  // Will be set at commit time
    
    return (tx_t)&tl_transaction;
}


// ABORT_PROCEDURE helper
static void abort_transaction(Transaction* tx, Region* region) {
    // Release all acquired locks with their original versions
    for (VersionedLock* lock_ptr : tx->acquired_locks) {
        // Clear lock bit, keep version
        uintptr_t current = lock_ptr->value.load(std::memory_order_acquire);
        lock_ptr->value.store(current & VERSION_MASK, std::memory_order_release);
    }
    
    // Clean up allocated segments that were not committed
    for (void* addr : tx->alloc_set) {
        SegmentNode* node = (SegmentNode*)((uintptr_t)addr - sizeof(SegmentNode));
        std::free(node);
        region->segment_count.fetch_sub(1, std::memory_order_relaxed);
    }
}

/**
End the given transaction.
    @param shared Shared memory region associated with the transaction
    @param tx Transaction to end
    @return true: the whole transaction committed, false: the transaction must be retried
    @note - This function can be called concurrently, but concurrent calls must be made with at least a different shared parameter or a different tx parameter.
          - This function will not be called by the user (e.g. the grading tool) if any of tm read, tm write, tm alloc, tm free already notified that tx was aborted.
*/
bool tm_end(shared_t shared, tx_t tx) noexcept {
    Transaction* tx_ptr = (Transaction*)tx;
    Region* region = (Region*)shared;
    
    // Step 1: Handle Read only or empty Cases
    if (tx_ptr->is_ro || (tx_ptr->write_set.empty() && tx_ptr->alloc_set.empty() && tx_ptr->free_set.empty())) {
        // For read-only or empty transactions, just validate read set
        for (const auto& entry : tx_ptr->read_set) {
            uintptr_t current = entry.lock_ptr->value.load(std::memory_order_acquire);
            if ((current & VERSION_MASK) != entry.version_observed || (current & LOCK_BIT) != 0) {
                return false; // Transaction aborted because of concurrent modification
            }
        }
        return true;
    }
    
    // Step 2: Phase 1 - Lock Acquisition
    // WriteSet is already sorted (std::map), iterate in order for deadlock-free locking
    for (const auto& entry : tx_ptr->write_set) {
        size_t lock_idx = addr_to_lock_index(region, entry.first);
        VersionedLock* lock_ptr = &region->locks[lock_idx];
        
        // Skip if already acquired (O(1) lookup with unordered_set)
        if (tx_ptr->acquired_locks.count(lock_ptr)) {
            continue;
        }
        
        // TL2: Spin briefly waiting for lock, then abort
        // This allows the holding transaction to complete, improving throughput
        bool acquired = false;
        for (int spin = 0; spin < 64; spin++) {
            uintptr_t expected = lock_ptr->value.load(std::memory_order_acquire);
            
            // Check if version too new - abort immediately (stale snapshot)
            if ((expected & VERSION_MASK) > tx_ptr->rv) {
                abort_transaction(tx_ptr, region);
                return false;
            }
            
            // If locked, spin-wait (busy wait to keep CPU active)
            if ((expected & LOCK_BIT) != 0) {
                // Brief pause to reduce memory bus contention
                for (volatile int i = 0; i < 8; i++) {}
                continue;
            }
            
            // Try to set lock bit
            uintptr_t desired = expected | LOCK_BIT;
            if (lock_ptr->value.compare_exchange_weak(expected, desired, 
                    std::memory_order_acquire, std::memory_order_relaxed)) {
                tx_ptr->acquired_locks.insert(lock_ptr);
                acquired = true;
                break;
            }
            // CAS failed - retry
        }
        
        if (!acquired) {
            abort_transaction(tx_ptr, region);
            return false;
        }
    }
    
    // Step 3: Phase 2 - Timestamping
    // Atomically fetch-and-add Global Version Clock by 2, wv is the NEW value
    tx_ptr->wv = region->gv_clock.fetch_add(2, std::memory_order_acq_rel) + 2;
    
    // Step 4: Phase 3 - Read-Set Validation
    for (const auto& entry : tx_ptr->read_set) {
        uintptr_t current = entry.lock_ptr->value.load(std::memory_order_acquire);
        
        // Check if version changed or locked by different transaction
        bool locked_by_us = tx_ptr->acquired_locks.count(entry.lock_ptr);
        
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
    {
        std::lock_guard<std::mutex> lock(region->alloc_mutex); //Need to lock to avoid concurrent allocations
        for (void* addr : tx_ptr->alloc_set) {
            SegmentNode* node = (SegmentNode*)((uintptr_t)addr - sizeof(SegmentNode));
            node->prev = nullptr;
            node->next = region->allocs;
            if (region->allocs) region->allocs->prev = node;
            region->allocs = node;
        }
    }
    
    // Step 6: Phase 5 - Release Locks
    // Release all acquired locks with wv as new version
    for (VersionedLock* lock_ptr : tx_ptr->acquired_locks) {
        lock_ptr->value.store(tx_ptr->wv, std::memory_order_release);
    }
    
    // Process frees AFTER releasing locks : now safe to free
    // TL2 guarantees no concurrent transaction can access freed memory after commit
    for (void* addr : tx_ptr->free_set) {
        SegmentNode* node = (SegmentNode*)((uintptr_t)addr - sizeof(SegmentNode));
        
        // Unlink from allocation list
        {
            std::lock_guard<std::mutex> lock(region->alloc_mutex);
            if (node->prev) node->prev->next = node->next;
            else region->allocs = node->next;
            if (node->next) node->next->prev = node->prev;
        }
        
        // Decrement segment count
        region->segment_count.fetch_sub(1, std::memory_order_relaxed);
        
        // Free immediately : safe after locks released
        free(node);
    }
    
    return true;
}


/**
Read operation in the transaction, source in the shared region and target in a private region.
    @param shared Shared memory region associated with the transaction
    @param tx Transaction to use
    @param source Source (aligned) start address (in shared memory)
    @param size Length to copy (in bytes)
    @param target Target (aligned) start address (in private memory)
    @return true: the transaction can continue, false: the transaction has aborted
    @note   - This function can be called concurrently, but concurrent calls must be made with at least a different shared parameter or a different tx parameter.
            - The private buffer target can only be dereferenced for the duration of the call.
            - The length size must be a positive multiple of the shared memory region's alignment, otherwise the behavior is undefined.
            - The length of the buffers source and target must be at least size, otherwise the behavior is undefined.
            - The source and target addresses must be a positive multiple of the shared memory region's alignment, otherwise the behavior is undefined.
*/
bool tm_read(shared_t shared, tx_t tx, void const* source, size_t size, void* target) noexcept {
    Transaction* tx_ptr = (Transaction*)tx;
    Region* region = (Region*)shared;
    
    // Basic safety checks
    if (source == nullptr || target == nullptr || size == 0) {
        return false;
    }
    
    // Check write-set for read-your-own-writes first
    auto it = tx_ptr->write_set.find(const_cast<void*>(source));
    if (it != tx_ptr->write_set.end() && it->second.size == size) {
        memcpy(target, it->second.data.data(), size);
        return true;
    }
    
    // For read-only transactions, perform simple read with validation
    if (tx_ptr->is_ro) {
        size_t lock_index = addr_to_lock_index(region, source);
        VersionedLock* lock_ptr = &region->locks[lock_index];
        
        uintptr_t v1 = lock_ptr->value.load(std::memory_order_acquire);
        if ((v1 & LOCK_BIT) != 0 || (v1 & VERSION_MASK) > tx_ptr->rv) {
            return false;
        }
        
        memcpy(target, source, size);
        
        uintptr_t v2 = lock_ptr->value.load(std::memory_order_acquire);
        return (v1 == v2); // If both versions match, return true because no changes occured while reading, else false
    }
    
    // For read-write transactions, use full TL2 protocol
    size_t lock_index = addr_to_lock_index(region, source);
    VersionedLock* lock_ptr = &region->locks[lock_index];
    
    // Single read attempt - abort on contention (TL2 principle)
    uintptr_t v1 = lock_ptr->value.load(std::memory_order_acquire);
    
    // Pre-validation: check if locked or version too new
    if ((v1 & LOCK_BIT) != 0 || (v1 & VERSION_MASK) > tx_ptr->rv) {
        return false;
    }
    
    // Copy the data
    memcpy(target, source, size);
    
    // Post-validation: check if lock changed
    uintptr_t v2 = lock_ptr->value.load(std::memory_order_acquire);
    if (v1 != v2) {
        return false;  // Abort - concurrent modification
    }
    
    // Record read for validation at commit time
    ReadEntry entry;
    entry.lock_ptr = lock_ptr;
    entry.version_observed = v1 & VERSION_MASK;
    tx_ptr->read_set.push_back(entry);
    
    return true;
}

/**
Write operation in the transaction, source in a private region and target in the shared region.
    @param shared Shared memory region associated with the transaction
    @param tx Transaction to use
    @param source Source (aligned) start address (in private memory)
    @param size Length to copy (in bytes)
    @param target Target (aligned) start address (in shared memory)
    @return true: the transaction can continue, false: the transaction has aborted
    @note   - This function can be called concurrently, but concurrent calls must be made with at least a different shared parameter or a different tx parameter.
            - The private buffer source can only be dereferenced for the duration of the call.
            - The length size must be a positive multiple of the shared memory region's alignment, otherwise the behavior is undened.
            - The length of the buffers source and target must be at least size, otherwise the behavior is undened.
            - The source and target addresses must be a positive multiple of the shared memory region's alignment, otherwise the behavior is undened.
*/
bool tm_write(shared_t shared, tx_t tx, void const* source, size_t size, void* target) noexcept {
    (void)shared;
    Transaction* tx_ptr = (Transaction*)tx;
    
    // Read-only transactions cannot write
    if (tx_ptr->is_ro) {
        return false;
    }
    
    // Basic safety checks
    if (source == nullptr || target == nullptr || size == 0) {
        return false;
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



/**
Shared memory segment allocation in the transaction.
    @param shared Shared memory region associated with the transaction
    @param tx Transaction to use
    @param size Alloction requested size (in bytes) that is at most 2^48
    @param target Pointer in private memory receiving the address of the first word of the newly allocated, aligned segment
    @return success alloc: the allocation was successful and transaction can continue,
            abort alloc: the transaction has aborted,
            nomem alloc: the memory allocation failed (e.g. not enough memory)
    @note - This function can be called concurrently, but concurrent calls must be made with at least a different shared parameter or a different tx parameter.
          - The pointer target can only be dereferenced for the duration of the call.
          - The value of *target is defined only if success alloc was returned.
          - The value of *target after the call if success alloc was returned must not be NULL (or nullptr in C++).
          - When nomem alloc is returned, the transaction is not aborted.
          - The allocated segment must be initialized with zeroes.
          - Only tm free can be used to free the allocated segment.
          - The length size must be a positive multiple of the shared memory region's alignment, otherwise the behavior is undefined.
*/
Alloc tm_alloc(shared_t shared, tx_t tx, size_t size, void** target) noexcept {
    Transaction* tx_ptr = (Transaction*)tx;
    Region* region = (Region*)shared;
    
    // Size alignment validation as per specification
    size_t align = region->align;
    
    // Check size alignment - must be a positive multiple of alignment
    if (size == 0 || size % align != 0) {
        return Alloc::nomem; // Return nomem for invalid size
    }
    
    // Check size limit (2^48 bytes)
    if (size > (1ULL << 48)) {
        return Alloc::nomem; // Return nomem for size too large
    }
    
    // Check segment limit (max 65536 segments)
    if (region->segment_count.load(std::memory_order_acquire) >= 65536) {
        return Alloc::nomem; // Return nomem if segment limit reached
    }
    
    if (align < sizeof(void*)) {
        align = sizeof(void*); //Correct alignment
    }
    
    SegmentNode* node;
    if (posix_memalign((void**)&node, align, sizeof(SegmentNode) + size) != 0) {
        return Alloc::nomem; // Return nomem if allocation failed
    }
    
    void* segment = (void*)((uintptr_t)node + sizeof(SegmentNode));
    
    // Ensure allocated segment is initialized with zeroes
    memset(segment, 0, size);
    
    *target = segment;
    tx_ptr->alloc_set.insert(segment); //Add to allocation set
    tx_ptr->alloc_sizes[segment] = size;  // Track the size of this segment
    
    // Increment segment count
    region->segment_count.fetch_add(1, std::memory_order_acq_rel);
    
    return Alloc::success;
}


/**
Shared memory segment deallocation in the transaction.
    @param shared Shared memory region associated with the transaction
    @param tx Transaction to use
    @param target Pointer to the first word of the allocated segment to deallocate
    @return true: the transaction can continue, false: the transaction has aborted
    @note - This function can be called concurrently, but concurrent calls must be made with at least a different shared parameter or a different tx parameter.
          - This function must not be called with target as the first allocated segment (the address returned by tm start).
*/
bool tm_free(shared_t shared, tx_t tx, void* target) noexcept {
    Transaction* tx_ptr = (Transaction*)tx;
    Region* region = (Region*)shared;
    
    // Safety Check
    if (target == nullptr) {
        return false;
    }
    
    // Check if allocated in this transaction : if so, remove from alloc set
    if (tx_ptr->alloc_set.count(target)) {
        tx_ptr->alloc_set.erase(target);
        tx_ptr->alloc_sizes.erase(target);  // Also remove from size tracking
        // Decrement segment count since we're canceling the allocation
        region->segment_count.fetch_sub(1, std::memory_order_acq_rel);
        
        // Free immediately since it was never committed
        SegmentNode* node = (SegmentNode*)((uintptr_t)target - sizeof(SegmentNode));
        free(node);
        return true;
    }
    
    // Defer the free until commit : validation happens at commit time
    tx_ptr->free_set.insert(target);
    return true;
}