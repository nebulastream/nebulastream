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

#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <span>
#include <utility>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>

namespace NES
{
/// A schema-agnostic, append-only order-statistic B-tree.
///
/// Every key occupies a fixed-size slot. Variable-sized payloads can be allocated through
/// allocateSpaceForVarSized() and referenced from such a slot (for example with VariableSizedAccess).
/// BTreeRef provides the record-layout-facing interface.
class BTree
{
public:
    using Comparator = std::function<bool(std::span<const std::byte>, std::span<const std::byte>)>;

    struct VarSizedAllocation
    {
        ChildBufferIndex childIndex;
        uint64_t offset;
        std::span<std::byte> memory;
    };

    static void init(TupleBuffer buffer, uint64_t pageSize, uint64_t entrySize);
    [[nodiscard]] static BTree load(const TupleBuffer& buffer);
    [[nodiscard]] static uint64_t getMainBufferSize();

    [[nodiscard]] uint64_t size() const;
    void append(std::span<const std::byte> entry, AbstractBufferProvider* bufferProvider, const Comparator& comparator);
    [[nodiscard]] std::span<std::byte> at(uint64_t index) const;

    VarSizedAllocation allocateSpaceForVarSized(AbstractBufferProvider* bufferProvider, uint64_t size);

private:
    static constexpr uint64_t VALID_BTREE = 8254667332726569;
    static constexpr uint32_t INVALID_CHILD_INDEX = std::numeric_limits<uint32_t>::max();

    struct Header
    {
        uint64_t status;
        uint64_t numberOfEntries;
        uint64_t pageSize;
        uint64_t entrySize;
        uint64_t maxKeysPerNode;
        uint32_t rootIndex;
        uint32_t varSizedPageIndex;
    };

    struct NodeHeader
    {
        uint64_t subtreeSize;
        uint32_t numberOfKeys;
        bool leaf;
    };

    explicit BTree(TupleBuffer buffer) : buffer(std::move(buffer)) { }

    [[nodiscard]] uint64_t getEntrySize() const;
    [[nodiscard]] uint64_t getPageSize() const;
    [[nodiscard]] uint64_t getMaxKeysPerNode() const;
    [[nodiscard]] Header& header();
    [[nodiscard]] const Header& header() const;
    [[nodiscard]] static uint64_t calculateMaxKeys(uint64_t pageSize, uint64_t entrySize);
    [[nodiscard]] TupleBuffer node(uint32_t nodeIndex) const;
    [[nodiscard]] static NodeHeader& nodeHeader(TupleBuffer& nodeBuffer);
    [[nodiscard]] static const NodeHeader& nodeHeader(const TupleBuffer& nodeBuffer);
    [[nodiscard]] std::byte* key(TupleBuffer& nodeBuffer, uint64_t keyIndex) const;
    [[nodiscard]] const std::byte* key(const TupleBuffer& nodeBuffer, uint64_t keyIndex) const;
    [[nodiscard]] uint32_t child(const TupleBuffer& nodeBuffer, uint64_t childIndex) const;
    void setChild(TupleBuffer& nodeBuffer, uint64_t childIndex, uint32_t value) const;
    [[nodiscard]] uint64_t calculateSubtreeSize(const TupleBuffer& nodeBuffer) const;
    [[nodiscard]] uint64_t findInsertPosition(uint32_t nodeIndex, std::span<const std::byte> entry, const Comparator& comparator) const;

    [[nodiscard]] uint32_t allocateNode(AbstractBufferProvider* bufferProvider, bool leaf);
    [[nodiscard]] uint32_t ensureRoot(AbstractBufferProvider* bufferProvider);
    [[nodiscard]] bool isLeaf(uint32_t nodeIndex) const;
    [[nodiscard]] bool isFull(uint32_t nodeIndex) const;
    [[nodiscard]] uint32_t childIndex(uint32_t nodeIndex, uint64_t childPosition) const;
    void incrementSubtreeSize(uint32_t nodeIndex);
    void insertKey(uint32_t nodeIndex, uint64_t keyPosition, std::span<const std::byte> entry);
    void splitRoot(AbstractBufferProvider* bufferProvider);
    void splitChild(uint32_t parentIndex, uint64_t childPosition, AbstractBufferProvider* bufferProvider);

    TupleBuffer buffer;
};
}
