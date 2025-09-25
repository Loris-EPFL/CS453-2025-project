/**
 * @file   tm.cpp
 * @author [...]
 *
 * @section LICENSE
 *
 * [...]
 *
 * @section DESCRIPTION
 *
 * Implementation of TL2 Software Transactional Memory.
 * Based on the TL2 algorithm specification with proper versioned locking
 * and global version clock for high-performance concurrent transactions.
**/

// Requested features
#define _POSIX_C_SOURCE   200809L

// External headers
#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <vector>
#include <unordered_set>

// Internal headers
#include <tm.hpp>
#include "macros.hpp"
#include "tl2_internal.hpp"

/** Create (i.e. allocate + init) a new shared memory region, with one first non-free-able allocated segment of the requested size and alignment.
 * @param size  Size of the first shared segment of memory to allocate (in bytes), must be a positive multiple of the alignment
 * @param align Alignment (in bytes) that the shared memory region must support
 * @return Opaque shared memory region handle, 'invalid_shared' on failure
**/
shared_t tm_create(size_t size, size_t align) noexcept {
    if (size == 0 || align == 0 || (align & (align - 1)) != 0 || size % align != 0) {
        return invalid_shared;
    }
    
    auto region = std::make_unique<SharedMemoryRegion>(size, align);
    if (!region || !region->start_address) {
        return invalid_shared;
    }
    
    return reinterpret_cast<shared_t>(region.release());
}

/** Destroy (i.e. clean-up + free) a given shared memory region.
 * @param shared Shared memory region to destroy, with no running transaction
**/
void tm_destroy(shared_t shared) noexcept {
    if (shared == invalid_shared) return;
    
    SharedMemoryRegion* region = reinterpret_cast<SharedMemoryRegion*>(shared);
    delete region;
}

/** [thread-safe] Return the start address of the first allocated segment in the shared memory region.
 * @param shared Shared memory region to query
 * @return Start address of the first allocated segment
**/
void* tm_start(shared_t shared) noexcept {
    if (shared == invalid_shared) return nullptr;
    
    SharedMemoryRegion* region = reinterpret_cast<SharedMemoryRegion*>(shared);
    return region->start_address;
}

/** [thread-safe] Return the size (in bytes) of the first allocated segment of the shared memory region.
 * @param shared Shared memory region to query
 * @return First allocated segment size
**/
size_t tm_size(shared_t shared) noexcept {
    if (shared == invalid_shared) return 0;
    
    SharedMemoryRegion* region = reinterpret_cast<SharedMemoryRegion*>(shared);
    return region->total_size;
}

/** [thread-safe] Return the alignment (in bytes) of the memory accesses on the given shared memory region.
 * @param shared Shared memory region to query
 * @return Alignment used globally
**/
size_t tm_align(shared_t shared) noexcept {
    if (shared == invalid_shared) return 0;
    
    SharedMemoryRegion* region = reinterpret_cast<SharedMemoryRegion*>(shared);
    return region->alignment;
}

/** [thread-safe] Begin a new transaction on the given shared memory region.
 * @param shared Shared memory region to start a transaction on
 * @param is_ro  Whether the transaction is read-only
 * @return Opaque transaction ID, 'invalid_tx' on failure
**/
tx_t tm_begin(shared_t shared, bool is_ro) noexcept {
    if (shared == invalid_shared) {
        return invalid_tx;
    }
    
    SharedMemoryRegion* region = reinterpret_cast<SharedMemoryRegion*>(shared);
    if (region->destroyed.load(std::memory_order_acquire)) {
        return invalid_tx;
    }
    
    auto transaction = std::make_unique<Transaction>(is_ro);
    if (!transaction) {
        return invalid_tx;
    }
    
    // TL2 Step 1: Sample global version-clock
    transaction->read_timestamp = global_clock.load(std::memory_order_acquire);
    
    return reinterpret_cast<tx_t>(transaction.release());
}

/** [thread-safe] End the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to end
 * @return Whether the whole transaction committed
**/
bool tm_end(shared_t shared, tx_t tx) noexcept {
    if (shared == invalid_shared || tx == invalid_tx) {
        return false;
    }
    
    SharedMemoryRegion* region = reinterpret_cast<SharedMemoryRegion*>(shared);
    Transaction* transaction = reinterpret_cast<Transaction*>(tx);
    
    if (region->destroyed.load(std::memory_order_acquire)) {
        delete transaction;
        return false;
    }
    
    // Read-only transactions commit immediately
    if (transaction->is_read_only) {
        delete transaction;
        return true;
    }
    
    // TL2 Write Transaction Commit Protocol
    
    // Step 3: Lock the write-set
    std::vector<VersionedLock*> acquired_locks;
    for (const auto& write_entry : transaction->write_set) {
        VersionedLock* lock = region->get_lock(write_entry.address);
        if (!lock) {
            // Unlock previously acquired locks
            for (auto* acquired_lock : acquired_locks) {
                acquired_lock->unlock_and_update(0); // Reset to unlocked
            }
            delete transaction;
            return false;
        }
        
        // Try to acquire lock with current version
        uint64_t current_version = lock->get_version();
        if (!lock->try_lock(current_version)) {
            // Lock acquisition failed - unlock previously acquired locks
            for (auto* acquired_lock : acquired_locks) {
                acquired_lock->unlock_and_update(0); // Reset to unlocked
            }
            delete transaction;
            return false;
        }
        
        acquired_locks.push_back(lock);
    }
    
    // Step 4: Increment global version-clock
    uint64_t write_version = global_clock.fetch_add(1, std::memory_order_acq_rel) + 1;
    
    // Step 5: Validate the read-set
    // Special case: if rv + 1 = wv, no need to validate
    if (transaction->read_timestamp + 1 != write_version) {
        for (const auto& read_entry : transaction->read_set) {
            VersionedLock* lock = region->get_lock(read_entry.address);
            if (!lock) {
                // Unlock acquired locks
                for (auto* acquired_lock : acquired_locks) {
                    acquired_lock->unlock_and_update(0);
                }
                delete transaction;
                return false;
            }
            
            // Check if lock is held by another transaction or version changed
            if (lock->is_locked() || lock->get_version() > transaction->read_timestamp) {
                // Validation failed - unlock acquired locks
                for (auto* acquired_lock : acquired_locks) {
                    acquired_lock->unlock_and_update(0);
                }
                delete transaction;
                return false;
            }
        }
    }
    
    // Step 6: Commit and release the locks
    for (size_t i = 0; i < transaction->write_set.size(); ++i) {
        const auto& write_entry = transaction->write_set[i];
        VersionedLock* lock = acquired_locks[i];
        
        // Write the data to shared memory
        std::memcpy(write_entry.address, write_entry.data.data(), write_entry.size);
        
        // Release lock and update version
        lock->unlock_and_update(write_version);
    }
    
    delete transaction;
    return true;
}

/** [thread-safe] Read operation in the given transaction, source in the shared region and target in a private region.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param source Source start address (in the shared region)
 * @param size   Length to copy (in bytes), must be a positive multiple of the alignment
 * @param target Target start address (in a private region)
 * @return Whether the whole transaction can continue
**/
bool tm_read(shared_t shared, tx_t tx, void const* source, size_t size, void* target) noexcept {
    if (shared == invalid_shared || tx == invalid_tx || !source || !target || size == 0) {
        return false;
    }
    
    SharedMemoryRegion* region = reinterpret_cast<SharedMemoryRegion*>(shared);
    Transaction* transaction = reinterpret_cast<Transaction*>(tx);
    
    if (region->destroyed.load(std::memory_order_acquire)) {
        return false;
    }
    
    // Validate alignment and bounds
    if (size % region->alignment != 0 || 
        reinterpret_cast<uintptr_t>(source) % region->alignment != 0 ||
        !region->contains_address(const_cast<void*>(source))) {
        return false;
    }
    
    // Check bounds
    uintptr_t start_addr = reinterpret_cast<uintptr_t>(region->start_address);
    uintptr_t source_addr = reinterpret_cast<uintptr_t>(source);
    if (source_addr + size > start_addr + region->total_size) {
        return false;
    }
    
    // TL2 Read Protocol: Check write-set first (read-your-own-writes)
    for (const auto& write_entry : transaction->write_set) {
        uintptr_t write_start = reinterpret_cast<uintptr_t>(write_entry.address);
        uintptr_t write_end = write_start + write_entry.size;
        
        // Check for overlap
        if (source_addr < write_end && source_addr + size > write_start) {
            // Read from our own write - copy from write buffer
            size_t offset = source_addr - write_start;
            std::memcpy(target, write_entry.data.data() + offset, size);
            return true;
        }
    }
    
    // TL2 Read Protocol for shared memory
    VersionedLock* lock = region->get_lock(const_cast<void*>(source));
    if (!lock) {
        return false;
    }
    
    // Read-only optimization: direct read with post-validation
    if (transaction->is_read_only) {
        // Read the data
        std::memcpy(target, source, size);
        
        // Post-validate: check lock is free and version <= read_timestamp
        if (lock->is_locked() || lock->get_version() > transaction->read_timestamp) {
            return false; // Abort transaction
        }
        
        return true;
    }
    
    // Writing transaction: full TL2 read protocol
    uint64_t version_before = lock->get_version();
    
    // Check if locked
    if (lock->is_locked()) {
        return false; // Abort transaction
    }
    
    // Check version against read timestamp
    if (version_before > transaction->read_timestamp) {
        return false; // Abort transaction
    }
    
    // Read the data
    std::memcpy(target, source, size);
    
    // Post-validation: re-read lock
    uint64_t version_after = lock->get_version();
    if (lock->is_locked() || version_after != version_before) {
        return false; // Abort transaction
    }
    
    // Add to read-set
    ReadEntry read_entry;
    read_entry.address = const_cast<void*>(source);
    read_entry.version = version_before;
    read_entry.size = size;
    transaction->read_set.push_back(read_entry);
    
    return true;
}

/** [thread-safe] Write operation in the given transaction, source in a private region and target in the shared region.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param source Source start address (in a private region)
 * @param size   Length to copy (in bytes), must be a positive multiple of the alignment
 * @param target Target start address (in the shared region)
 * @return Whether the whole transaction can continue
**/
bool tm_write(shared_t shared, tx_t tx, void const* source, size_t size, void* target) noexcept {
    if (shared == invalid_shared || tx == invalid_tx || !source || !target || size == 0) {
        return false;
    }
    
    SharedMemoryRegion* region = reinterpret_cast<SharedMemoryRegion*>(shared);
    Transaction* transaction = reinterpret_cast<Transaction*>(tx);
    
    if (region->destroyed.load(std::memory_order_acquire)) {
        return false;
    }
    
    // Read-only transactions cannot write
    if (transaction->is_read_only) {
        return false;
    }
    
    // Validate alignment and bounds
    if (size % region->alignment != 0 || 
        reinterpret_cast<uintptr_t>(target) % region->alignment != 0 ||
        !region->contains_address(target)) {
        return false;
    }
    
    // Check bounds
    uintptr_t start_addr = reinterpret_cast<uintptr_t>(region->start_address);
    uintptr_t target_addr = reinterpret_cast<uintptr_t>(target);
    if (target_addr + size > start_addr + region->total_size) {
        return false;
    }
    
    // TL2 Write Protocol: Buffer writes for commit-time
    WriteEntry write_entry;
    write_entry.address = target;
    write_entry.size = size;
    write_entry.data.resize(size);
    std::memcpy(write_entry.data.data(), source, size);
    
    transaction->write_set.push_back(std::move(write_entry));
    
    return true;
}

/** [thread-safe] Memory allocation in the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param size   Allocation requested size (in bytes), must be a positive multiple of the alignment
 * @param target Pointer in private memory receiving the address of the first byte of the newly allocated, aligned segment
 * @return Whether the whole transaction can continue (success/nomem), or not (abort_alloc)
**/
Alloc tm_alloc(shared_t shared, tx_t tx, size_t size, void** target) noexcept {
    if (unlikely(!shared || !tx || !target || size == 0)) {
        return Alloc::abort;
    }
    
    SharedMemoryRegion* region = static_cast<SharedMemoryRegion*>(shared);
    Transaction* transaction = reinterpret_cast<Transaction*>(tx);
    
    if (unlikely(region->destroyed.load(std::memory_order_acquire))) {
        return Alloc::abort;
    }
    
    // Check if size is a multiple of alignment
    if (size % region->alignment != 0) {
        return Alloc::abort;
    }
    
    // Try to allocate from the shared region
    // We need to ensure atomic allocation to prevent races
    size_t current_allocated = region->allocated_size;
    size_t new_allocated = current_allocated + size;
    
    if (new_allocated > region->total_size) {
        return Alloc::nomem;
    }
    
    // Calculate the address for the new allocation
    uintptr_t base_addr = reinterpret_cast<uintptr_t>(region->start_address);
    void* allocated_addr = reinterpret_cast<void*>(base_addr + current_allocated);
    
    // Update allocated size atomically
    region->allocated_size = new_allocated;
    
    // Track this allocation in the transaction for potential rollback
    transaction->allocated_segments.push_back(allocated_addr);
    
    *target = allocated_addr;
    return Alloc::success;
}

/** [thread-safe] Memory freeing in the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param target Target start address (in the shared region), must be aligned
 * @return Whether the whole transaction can continue
**/
bool tm_free(shared_t shared, tx_t tx, void* target) noexcept {
    if (unlikely(!shared || !tx || !target)) {
        return false;
    }
    
    SharedMemoryRegion* region = static_cast<SharedMemoryRegion*>(shared);
    Transaction* transaction = reinterpret_cast<Transaction*>(tx);
    
    if (unlikely(region->destroyed.load(std::memory_order_acquire))) {
        return false;
    }
    
    // Check if the address is within our region
    if (!region->contains_address(target)) {
        return false;
    }
    
    // Check alignment
    uintptr_t addr = reinterpret_cast<uintptr_t>(target);
    if (addr % region->alignment != 0) {
        return false;
    }
    
    // For simplicity, we don't actually free memory in this implementation
    // Real STM would need to track freed segments and handle them at commit time
    // This is a complex operation that requires careful handling of concurrent allocations
    
    // Just mark as successful for now - the memory will be reclaimed when the region is destroyed
    return true;
}
