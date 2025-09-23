/**
 * @file   tl2_internal.cpp
 * @author [...]
 *
 * @section LICENSE
 *
 * [...]
 *
 * @section DESCRIPTION
 *
 * Implementation of TL2 STM internal data structures and utility functions.
**/

#include "tl2_internal.hpp"
#include <cstdlib>
#include <cstring>

// Global version clock definition
std::atomic<uint64_t> global_clock{0};

// Transaction implementation
Transaction::Transaction(bool ro) : read_timestamp(0), is_read_only(ro) {}

void Transaction::reset(bool ro) {
    read_timestamp = 0;
    read_set.clear();
    write_set.clear();
    allocated_segments.clear();
    is_read_only = ro;
}

// SharedMemoryRegion implementation
SharedMemoryRegion::SharedMemoryRegion(size_t size, size_t align) 
    : start_address(nullptr), total_size(size), alignment(align), allocated_size(0) {
    
    // Allocate aligned memory
    if (posix_memalign(&start_address, align, size) != 0) {
        start_address = nullptr;
        total_size = 0;
        return;
    }
    
    // Initialize memory to zero
    std::memset(start_address, 0, size);
}

SharedMemoryRegion::~SharedMemoryRegion() {
    if (start_address) {
        std::free(start_address);
        start_address = nullptr;
    }
    destroyed.store(true, std::memory_order_release);
}

VersionedLock* SharedMemoryRegion::get_lock(void* address) {
    if (!contains_address(address)) {
        return nullptr;
    }
    
    // Get word-aligned address for this memory location
    uintptr_t word_addr = tl2::get_word_address(address, alignment);
    
    // Try to find existing lock
    auto it = locks.find(word_addr);
    if (it != locks.end()) {
        return it->second.get();
    }
    
    // Create new lock for this word
    auto new_lock = std::make_unique<VersionedLock>();
    VersionedLock* lock_ptr = new_lock.get();
    locks[word_addr] = std::move(new_lock);
    
    return lock_ptr;
}

bool SharedMemoryRegion::contains_address(void* address) const {
    uintptr_t addr = reinterpret_cast<uintptr_t>(address);
    uintptr_t start = reinterpret_cast<uintptr_t>(start_address);
    return addr >= start && addr < start + total_size;
}

// TL2 utility functions implementation
namespace tl2 {
    
uint64_t acquire_timestamp() {
    return global_clock.fetch_add(1, std::memory_order_acq_rel);
}

uintptr_t get_word_address(void* address, size_t alignment) {
    return reinterpret_cast<uintptr_t>(address) & ~(alignment - 1);
}

} // namespace tl2