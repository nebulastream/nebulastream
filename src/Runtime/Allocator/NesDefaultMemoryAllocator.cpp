#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>


namespace NES::Runtime {

void* NesDefaultMemoryAllocator::do_allocate(size_t bytes, size_t alignment) {
    void* tmp = nullptr;
    NES_ASSERT(posix_memalign(&tmp, alignment, bytes) == 0, "memory allocation failed with alignment");
    return tmp;
}

void NesDefaultMemoryAllocator::do_deallocate(void* p, size_t, size_t) {
    std::free(p);
}

}