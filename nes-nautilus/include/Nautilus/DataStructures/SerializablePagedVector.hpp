/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <vector>
#include <rfl.hpp>
#include <MemoryLayout/MemoryLayout.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>

namespace NES::DataStructures {

using MemoryLayout = Memory::MemoryLayouts::MemoryLayout;
using TupleBuffer = Memory::TupleBuffer;
using AbstractBufferProvider = Memory::AbstractBufferProvider;

/// compatible with reflect-cpp
struct PagedVectorState {
    struct PageData {
        std::vector<uint8_t> buffer;    // Page buffer data
        uint64_t bufferSize;            // Size of buffer
        uint64_t numberOfEntries = 0;  // Number of entries in this page
        uint64_t cumulativeSum = 0;     // Cumulative sum up to this page
    };
    
    std::vector<PageData> pages;
    uint64_t totalEntries = 0;
    uint64_t entrySize = 0;
    uint64_t pageSize = 0;
    bool hasMemoryLayout = false;
    
    // Serialized memory layout (if present)
    std::vector<uint8_t> memoryLayoutData;
};

/// Serializable version of PagedVector that maintains compatibility with existing interface
/// while supporting zero-copy serialization via reflect-cpp
class SerializablePagedVector {
public:
    struct TupleBufferWithCumulativeSum {
        explicit TupleBufferWithCumulativeSum(TupleBuffer buffer) : buffer(std::move(buffer)) { }

        size_t cumulativeSum{0};
        TupleBuffer buffer;
    };

    /// Default constructor
    SerializablePagedVector() = default;
    
    /// Construct from existing state (zero-copy restoration)
    explicit SerializablePagedVector(PagedVectorState& state);
    
    // Non-copyable due to internal unique_ptr caches
    SerializablePagedVector(const SerializablePagedVector&) = delete;
    SerializablePagedVector& operator=(const SerializablePagedVector&) = delete;
    
    /// Zero-copy construction from serialized state
    static SerializablePagedVector fromState(PagedVectorState& state) {
        return SerializablePagedVector(state);
    }
    
    /// Extract serializable state (no copy - return reference)
    PagedVectorState& extractState() { return state; }
    const PagedVectorState& extractState() const { return state; }
    
    /// Existing PagedVector interface compatibility
    void appendPageIfFull(AbstractBufferProvider* bufferProvider, const MemoryLayout* memoryLayout);
    void moveAllPages(SerializablePagedVector& other);
    void copyFrom(const SerializablePagedVector& other);
    
    [[nodiscard]] const TupleBuffer* getTupleBufferForEntry(uint64_t entryPos) const;
    [[nodiscard]] std::optional<uint64_t> getBufferPosForEntry(uint64_t entryPos) const;
    
    [[nodiscard]] uint64_t getTotalNumberOfEntries() const;
    [[nodiscard]] uint64_t getNumberOfPages() const { return state.pages.size(); }
    
    /// Get page data for entry operations
    [[nodiscard]] const TupleBuffer& getLastPage() const;
    [[nodiscard]] const TupleBuffer& getFirstPage() const;
    
    /// Access to specific pages for iteration
    [[nodiscard]] const PagedVectorState::PageData& getPageData(size_t index) const;
    [[nodiscard]] size_t findPageIndex(uint64_t entryPos) const;
    
    /// Memory layout management
    void setMemoryLayout(const MemoryLayout* layout);
    [[nodiscard]] std::unique_ptr<MemoryLayout> getMemoryLayout() const;
    
private:
    PagedVectorState state;
    
    // Cached TupleBuffers for compatibility (created on demand)
    mutable std::vector<std::unique_ptr<TupleBuffer>> cachedBuffers;
    mutable std::unique_ptr<MemoryLayout> cachedMemoryLayout;
    
    /// Internal helpers
    void updateCumulativeSums();
    void createTupleBuffer(size_t pageIndex) const;
    void serializeMemoryLayout(const MemoryLayout* layout);
};

}
