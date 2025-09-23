/**
 * @file   tl2_internal.hpp
 * @author [...]
 *
 * @section LICENSE
 *
 * [...]
 *
 * @section DESCRIPTION
 *
 * Internal data structures and declarations for TL2 STM implementation.
 * This file contains the core TL2 algorithm components.
**/

#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <vector>
#include <unordered_map>
#include <memory>

// TL2 Configuration Constants
namespace tl2 {
    static constexpr size_t CACHE_LINE_SIZE = 64;
    static constexpr uintptr_t LOCK_BIT = 1UL;
    static constexpr uintptr_t VERSION_MASK = ~LOCK_BIT;
}

// Forward declarations
struct VersionedLock;
struct Transaction;
struct SharedMemoryRegion;

// Global version clock for TL2
extern std::atomic<uint64_t> global_clock;

/**
 * @brief Versioned lock structure for TL2 algorithm
 * 
 * Each memory word is protected by a versioned lock that combines
 * a lock bit with a version number for efficient conflict detection.
 */
struct VersionedLock {
    std::atomic<uintptr_t> version_and_lock{0};
    
    /**
     * @brief Check if the lock is currently held
     * @return true if locked, false otherwise
     */
    bool is_locked() const {
        return (version_and_lock.load(std::memory_order_acquire) & tl2::LOCK_BIT) != 0;
    }
    
    /**
     * @brief Get the current version number
     * @return Version number (without lock bit)
     */
    uint64_t get_version() const {
        return version_and_lock.load(std::memory_order_acquire) & tl2::VERSION_MASK;
    }
    
    /**
     * @brief Attempt to acquire the lock with expected version
     * @param expected_version Expected version number
     * @return true if lock acquired successfully
     */
    bool try_lock(uint64_t expected_version) {
        uintptr_t expected = expected_version & tl2::VERSION_MASK;
        uintptr_t desired = expected | tl2::LOCK_BIT;
        return version_and_lock.compare_exchange_strong(expected, desired, std::memory_order_acq_rel);
    }
    
    /**
     * @brief Release lock and update to new version
     * @param new_version New version number to set
     */
    void unlock_and_update(uint64_t new_version) {
        version_and_lock.store(new_version & tl2::VERSION_MASK, std::memory_order_release);
    }
};

/**
 * @brief Entry in transaction's read set
 */
struct ReadEntry {
    void* address;      ///< Memory address read
    uint64_t version;   ///< Version observed during read
    size_t size;        ///< Size of the read operation
};

/**
 * @brief Entry in transaction's write set
 */
struct WriteEntry {
    void* address;              ///< Memory address to write
    std::vector<uint8_t> data;  ///< Buffered write data
    size_t size;                ///< Size of the write operation
};

/**
 * @brief Transaction descriptor for TL2
 * 
 * Contains all state needed to track a transaction's progress,
 * including read/write sets and timestamps.
 */
struct Transaction {
    uint64_t read_timestamp;                    ///< Timestamp acquired at transaction start
    std::vector<ReadEntry> read_set;            ///< Set of memory locations read
    std::vector<WriteEntry> write_set;          ///< Set of memory locations to write
    std::vector<void*> allocated_segments;      ///< Segments allocated in this transaction
    bool is_read_only;                          ///< True if transaction is read-only
    
    /**
     * @brief Constructor
     * @param ro True if this is a read-only transaction
     */
    explicit Transaction(bool ro);
    
    /**
     * @brief Clear all transaction state for reuse
     */
    void reset(bool ro);
};

/**
 * @brief Shared memory region structure
 * 
 * Manages a region of shared memory with associated versioned locks
 * and allocation tracking.
 */
struct SharedMemoryRegion {
    void* start_address;                                                    ///< Start of allocated memory
    size_t total_size;                                                      ///< Total size of the region
    size_t alignment;                                                       ///< Required alignment
    size_t allocated_size;                                                  ///< Currently allocated size
    std::unordered_map<uintptr_t, std::unique_ptr<VersionedLock>> locks;   ///< Versioned locks per word
    std::atomic<bool> destroyed{false};                                     ///< Destruction flag
    
    /**
     * @brief Constructor
     * @param size Size of memory region to allocate
     * @param align Required alignment
     */
    SharedMemoryRegion(size_t size, size_t align);
    
    /**
     * @brief Destructor - frees allocated memory
     */
    ~SharedMemoryRegion();
    
    /**
     * @brief Get or create versioned lock for memory address
     * @param address Memory address to get lock for
     * @return Pointer to versioned lock
     */
    VersionedLock* get_lock(void* address);
    
    /**
     * @brief Check if address is within this region
     * @param address Address to check
     * @return true if address is in region
     */
    bool contains_address(void* address) const;
};

// Utility functions
namespace tl2 {
    /**
     * @brief Get word-aligned address for given address
     * @param address Input address
     * @param alignment Alignment requirement
     * @return Word-aligned address
     */
    uintptr_t get_word_address(void* address, size_t alignment);
    
    /**
     * @brief Acquire next global timestamp
     * @return New timestamp
     */
    uint64_t acquire_timestamp();
}