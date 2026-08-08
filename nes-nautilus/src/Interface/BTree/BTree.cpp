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

#include <Interface/BTree/BTree.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <span>
#include <utility>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
uint64_t BTree::calculateMaxKeys(const uint64_t pageSize, const uint64_t entrySize)
{
    PRECONDITION(entrySize > 0, "BTree entry size must be greater than zero");
    PRECONDITION(pageSize > sizeof(NodeHeader) + sizeof(uint32_t), "BTree page size is too small");

    /// Each key needs one fixed-size slot and one child index. There is one additional child index.
    const auto available = pageSize - sizeof(NodeHeader) - sizeof(uint32_t);
    auto maxKeys = available / (entrySize + sizeof(uint32_t));
    if (maxKeys > 0 and maxKeys % 2 == 0)
    {
        --maxKeys;
    }
    PRECONDITION(maxKeys >= 3, "BTree page of {} bytes cannot hold three {}-byte entries", pageSize, entrySize);
    return maxKeys;
}

void BTree::init(TupleBuffer buffer, const uint64_t pageSize, const uint64_t entrySize)
{
    PRECONDITION(buffer.getBufferSize() >= getMainBufferSize(), "BTree main buffer is too small");
    new (buffer.getAvailableMemoryArea<Header>().data()) Header{
        .status = VALID_BTREE,
        .numberOfEntries = 0,
        .pageSize = pageSize,
        .entrySize = entrySize,
        .maxKeysPerNode = calculateMaxKeys(pageSize, entrySize),
        .rootIndex = INVALID_CHILD_INDEX,
        .varSizedPageIndex = INVALID_CHILD_INDEX};
}

BTree BTree::load(const TupleBuffer& buffer)
{
    BTree tree{buffer};
    PRECONDITION(tree.header().status == VALID_BTREE, "Invalid BTree status: {}", tree.header().status);
    return tree;
}

uint64_t BTree::getMainBufferSize()
{
    return sizeof(Header);
}

uint64_t BTree::size() const
{
    return header().numberOfEntries;
}

uint64_t BTree::getEntrySize() const
{
    return header().entrySize;
}

uint64_t BTree::getPageSize() const
{
    return header().pageSize;
}

uint64_t BTree::getMaxKeysPerNode() const
{
    return header().maxKeysPerNode;
}

BTree::Header& BTree::header()
{
    return *buffer.getAvailableMemoryArea<Header>().data();
}

const BTree::Header& BTree::header() const
{
    return *buffer.getAvailableMemoryArea<const Header>().data();
}

TupleBuffer BTree::node(const uint32_t nodeIndex) const
{
    PRECONDITION(nodeIndex != INVALID_CHILD_INDEX, "Invalid BTree node index");
    return buffer.loadChildBuffer(ChildBufferIndex{nodeIndex});
}

BTree::NodeHeader& BTree::nodeHeader(TupleBuffer& nodeBuffer)
{
    return *nodeBuffer.getAvailableMemoryArea<NodeHeader>().data();
}

const BTree::NodeHeader& BTree::nodeHeader(const TupleBuffer& nodeBuffer)
{
    return *nodeBuffer.getAvailableMemoryArea<const NodeHeader>().data();
}

std::byte* BTree::key(TupleBuffer& nodeBuffer, const uint64_t keyIndex) const
{
    PRECONDITION(keyIndex < getMaxKeysPerNode(), "BTree key slot out of bounds");
    return nodeBuffer.getAvailableMemoryArea<>().data() + sizeof(NodeHeader) + (keyIndex * getEntrySize());
}

const std::byte* BTree::key(const TupleBuffer& nodeBuffer, const uint64_t keyIndex) const
{
    PRECONDITION(keyIndex < getMaxKeysPerNode(), "BTree key slot out of bounds");
    return nodeBuffer.getAvailableMemoryArea<const std::byte>().data() + sizeof(NodeHeader) + (keyIndex * getEntrySize());
}

uint32_t BTree::child(const TupleBuffer& nodeBuffer, const uint64_t childIndex) const
{
    PRECONDITION(childIndex <= getMaxKeysPerNode(), "BTree child slot out of bounds");
    const auto offset = sizeof(NodeHeader) + (getMaxKeysPerNode() * getEntrySize()) + (childIndex * sizeof(uint32_t));
    uint32_t result = INVALID_CHILD_INDEX;
    std::memcpy(&result, nodeBuffer.getAvailableMemoryArea<const std::byte>().data() + offset, sizeof(result));
    return result;
}

void BTree::setChild(TupleBuffer& nodeBuffer, const uint64_t childIndex, const uint32_t value) const
{
    PRECONDITION(childIndex <= getMaxKeysPerNode(), "BTree child slot out of bounds");
    const auto offset = sizeof(NodeHeader) + (getMaxKeysPerNode() * getEntrySize()) + (childIndex * sizeof(uint32_t));
    std::memcpy(nodeBuffer.getAvailableMemoryArea<>().data() + offset, &value, sizeof(value));
}

uint64_t BTree::calculateSubtreeSize(const TupleBuffer& nodeBuffer) const
{
    const auto& nodeHdr = nodeHeader(nodeBuffer);
    auto result = static_cast<uint64_t>(nodeHdr.numberOfKeys);
    if (not nodeHdr.leaf)
    {
        for (uint64_t i = 0; i <= nodeHdr.numberOfKeys; ++i)
        {
            result += nodeHeader(node(child(nodeBuffer, i))).subtreeSize;
        }
    }
    return result;
}

uint64_t BTree::findInsertPosition(const uint32_t nodeIndex, const std::span<const std::byte> entry, const Comparator& comparator) const
{
    const auto nodeBuffer = node(nodeIndex);
    const auto numberOfKeys = nodeHeader(nodeBuffer).numberOfKeys;
    uint64_t position = 0;
    while (position < numberOfKeys)
    {
        const std::span<const std::byte> existing{key(nodeBuffer, position), getEntrySize()};
        if (comparator(entry, existing))
        {
            break;
        }
        ++position;
    }
    return position;
}

uint32_t BTree::allocateNode(AbstractBufferProvider* bufferProvider, const bool leaf)
{
    PRECONDITION(bufferProvider != nullptr, "BTree buffer provider must not be null");
    auto nodeBuffer = bufferProvider->getUnpooledBuffer(getPageSize());
    if (not nodeBuffer.has_value())
    {
        throw BufferAllocationFailure("No unpooled TupleBuffer available for a BTree node");
    }
    new (nodeBuffer->getAvailableMemoryArea<NodeHeader>().data()) NodeHeader{.subtreeSize = 0, .numberOfKeys = 0, .leaf = leaf};
    const auto index = buffer.storeChildBuffer(*nodeBuffer).getRawValue();
    PRECONDITION(index != INVALID_CHILD_INDEX, "BTree node child index is invalid");
    return index;
}

uint32_t BTree::ensureRoot(AbstractBufferProvider* bufferProvider)
{
    if (header().rootIndex == INVALID_CHILD_INDEX)
    {
        header().rootIndex = allocateNode(bufferProvider, true);
    }
    return header().rootIndex;
}

bool BTree::isLeaf(const uint32_t nodeIndex) const
{
    const auto nodeBuffer = node(nodeIndex);
    return nodeHeader(nodeBuffer).leaf;
}

bool BTree::isFull(const uint32_t nodeIndex) const
{
    const auto nodeBuffer = node(nodeIndex);
    return nodeHeader(nodeBuffer).numberOfKeys == getMaxKeysPerNode();
}

uint32_t BTree::childIndex(const uint32_t nodeIndex, const uint64_t childPosition) const
{
    const auto nodeBuffer = node(nodeIndex);
    PRECONDITION(not nodeHeader(nodeBuffer).leaf, "A leaf BTree node has no children");
    PRECONDITION(childPosition <= nodeHeader(nodeBuffer).numberOfKeys, "BTree child position out of bounds");
    return child(nodeBuffer, childPosition);
}

void BTree::incrementSubtreeSize(const uint32_t nodeIndex)
{
    auto nodeBuffer = node(nodeIndex);
    ++nodeHeader(nodeBuffer).subtreeSize;
}

void BTree::insertKey(const uint32_t nodeIndex, const uint64_t keyPosition, const std::span<const std::byte> entry)
{
    auto nodeBuffer = node(nodeIndex);
    auto& nodeHdr = nodeHeader(nodeBuffer);
    PRECONDITION(nodeHdr.leaf, "BTree keys are inserted directly only into leaves");
    PRECONDITION(nodeHdr.numberOfKeys < getMaxKeysPerNode(), "Cannot insert into a full BTree node");
    PRECONDITION(keyPosition <= nodeHdr.numberOfKeys, "BTree insertion position out of bounds");
    const auto keysToMove = nodeHdr.numberOfKeys - keyPosition;
    if (keysToMove > 0)
    {
        std::memmove(key(nodeBuffer, keyPosition + 1), key(nodeBuffer, keyPosition), keysToMove * getEntrySize());
    }
    std::memcpy(key(nodeBuffer, keyPosition), entry.data(), getEntrySize());
    ++nodeHdr.numberOfKeys;
    ++nodeHdr.subtreeSize;
    ++header().numberOfEntries;
}

void BTree::append(const std::span<const std::byte> entry, AbstractBufferProvider* bufferProvider, const Comparator& comparator)
{
    PRECONDITION(entry.size() == getEntrySize(), "BTree entry has {} bytes, expected {}", entry.size(), getEntrySize());
    PRECONDITION(static_cast<bool>(comparator), "BTree comparator must not be empty");

    auto current = ensureRoot(bufferProvider);
    if (isFull(current))
    {
        splitRoot(bufferProvider);
        current = header().rootIndex;
    }

    while (not isLeaf(current))
    {
        auto position = findInsertPosition(current, entry, comparator);
        auto childNode = childIndex(current, position);
        if (isFull(childNode))
        {
            splitChild(current, position, bufferProvider);
            auto parent = node(current);
            const std::span<const std::byte> promoted{key(parent, position), getEntrySize()};
            if (not comparator(entry, promoted))
            {
                ++position;
            }
            childNode = childIndex(current, position);
        }
        incrementSubtreeSize(current);
        current = childNode;
    }

    insertKey(current, findInsertPosition(current, entry, comparator), entry);
}

void BTree::splitRoot(AbstractBufferProvider* bufferProvider)
{
    const auto oldRoot = ensureRoot(bufferProvider);
    PRECONDITION(isFull(oldRoot), "BTree root must be full before splitting");
    const auto newRoot = allocateNode(bufferProvider, false);
    auto newRootBuffer = node(newRoot);
    setChild(newRootBuffer, 0, oldRoot);
    nodeHeader(newRootBuffer).subtreeSize = nodeHeader(node(oldRoot)).subtreeSize;
    header().rootIndex = newRoot;
    splitChild(newRoot, 0, bufferProvider);
}

void BTree::splitChild(const uint32_t parentIndex, const uint64_t childPosition, AbstractBufferProvider* bufferProvider)
{
    auto parent = node(parentIndex);
    auto& parentHeader = nodeHeader(parent);
    PRECONDITION(parentHeader.numberOfKeys < getMaxKeysPerNode(), "Cannot split into a full BTree parent");
    PRECONDITION(childPosition <= parentHeader.numberOfKeys, "BTree split child position out of bounds");

    auto left = node(child(parent, childPosition));
    auto& leftHeader = nodeHeader(left);
    PRECONDITION(leftHeader.numberOfKeys == getMaxKeysPerNode(), "BTree split child must be full");

    const auto minimumDegree = (getMaxKeysPerNode() + 1) / 2;
    const auto rightIndex = allocateNode(bufferProvider, leftHeader.leaf);
    auto right = node(rightIndex);
    auto& rightHeader = nodeHeader(right);
    rightHeader.numberOfKeys = static_cast<uint32_t>(minimumDegree - 1);
    std::memcpy(key(right, 0), key(left, minimumDegree), rightHeader.numberOfKeys * getEntrySize());

    if (not leftHeader.leaf)
    {
        for (uint64_t i = 0; i < minimumDegree; ++i)
        {
            setChild(right, i, child(left, i + minimumDegree));
        }
    }

    for (uint64_t i = parentHeader.numberOfKeys + 1; i > childPosition + 1; --i)
    {
        setChild(parent, i, child(parent, i - 1));
    }
    setChild(parent, childPosition + 1, rightIndex);

    const auto parentKeysToMove = parentHeader.numberOfKeys - childPosition;
    if (parentKeysToMove > 0)
    {
        std::memmove(key(parent, childPosition + 1), key(parent, childPosition), parentKeysToMove * getEntrySize());
    }
    std::memcpy(key(parent, childPosition), key(left, minimumDegree - 1), getEntrySize());
    ++parentHeader.numberOfKeys;

    leftHeader.numberOfKeys = static_cast<uint32_t>(minimumDegree - 1);
    leftHeader.subtreeSize = calculateSubtreeSize(left);
    rightHeader.subtreeSize = calculateSubtreeSize(right);
    parentHeader.subtreeSize = calculateSubtreeSize(parent);
}

std::span<std::byte> BTree::at(uint64_t index) const
{
    PRECONDITION(index < size(), "BTree index {} out of bounds for size {}", index, size());
    auto current = node(header().rootIndex);
    while (true)
    {
        const auto& currentHeader = nodeHeader(current);
        for (uint64_t keyIndex = 0; keyIndex < currentHeader.numberOfKeys; ++keyIndex)
        {
            if (not currentHeader.leaf)
            {
                auto leftChild = node(child(current, keyIndex));
                const auto leftSize = nodeHeader(leftChild).subtreeSize;
                if (index < leftSize)
                {
                    current = std::move(leftChild);
                    goto next_node; /// NOLINT(cppcoreguidelines-avoid-goto): exits the key scan into the next tree level.
                }
                index -= leftSize;
            }
            if (index == 0)
            {
                return {key(current, keyIndex), getEntrySize()};
            }
            --index;
        }
        PRECONDITION(not currentHeader.leaf, "BTree subtree sizes are inconsistent");
        current = node(child(current, currentHeader.numberOfKeys));
    next_node:;
    }
}

void BTree::initializeIterator(IteratorState& state, uint64_t index) const
{
    PRECONDITION(index <= size(), "BTree iterator index {} out of bounds for size {}", index, size());
    state.depth = 0;
    if (index == size())
    {
        return;
    }

    auto currentIndex = header().rootIndex;
    while (true)
    {
        PRECONDITION(state.depth < MAX_TREE_HEIGHT, "BTree exceeds the maximum supported iterator height");
        const auto current = node(currentIndex);
        const auto& currentHeader = nodeHeader(current);
        if (currentHeader.leaf)
        {
            PRECONDITION(index < currentHeader.numberOfKeys, "BTree subtree sizes are inconsistent");
            state.frames[state.depth++] = IteratorFrame{.nodeIndex = currentIndex, .keyIndex = static_cast<uint32_t>(index)};
            return;
        }

        bool descended = false;
        for (uint32_t keyIndex = 0; keyIndex < currentHeader.numberOfKeys; ++keyIndex)
        {
            const auto leftSize = nodeHeader(node(child(current, keyIndex))).subtreeSize;
            if (index < leftSize)
            {
                state.frames[state.depth++] = IteratorFrame{.nodeIndex = currentIndex, .keyIndex = keyIndex};
                currentIndex = child(current, keyIndex);
                descended = true;
                break;
            }
            index -= leftSize;
            if (index == 0)
            {
                state.frames[state.depth++] = IteratorFrame{.nodeIndex = currentIndex, .keyIndex = keyIndex};
                return;
            }
            --index;
        }
        if (not descended)
        {
            state.frames[state.depth++] = IteratorFrame{.nodeIndex = currentIndex, .keyIndex = currentHeader.numberOfKeys};
            currentIndex = child(current, currentHeader.numberOfKeys);
        }
    }
}

bool BTree::iteratorValid(const IteratorState& state)
{
    return state.depth > 0;
}

std::span<std::byte> BTree::iteratorValue(const IteratorState& state) const
{
    PRECONDITION(iteratorValid(state), "Cannot dereference the BTree end iterator");
    const auto& frame = state.frames[state.depth - 1];
    auto nodeBuffer = node(frame.nodeIndex);
    PRECONDITION(frame.keyIndex < nodeHeader(nodeBuffer).numberOfKeys, "BTree iterator does not reference a key");
    return {key(nodeBuffer, frame.keyIndex), getEntrySize()};
}

void BTree::advanceIterator(IteratorState& state) const
{
    PRECONDITION(iteratorValid(state), "Cannot increment the BTree end iterator");
    auto& currentFrame = state.frames[state.depth - 1];
    auto current = node(currentFrame.nodeIndex);
    const auto& currentHeader = nodeHeader(current);
    if (not currentHeader.leaf)
    {
        ++currentFrame.keyIndex;
        auto currentIndex = child(current, currentFrame.keyIndex);
        while (true)
        {
            PRECONDITION(state.depth < MAX_TREE_HEIGHT, "BTree exceeds the maximum supported iterator height");
            current = node(currentIndex);
            const auto& header = nodeHeader(current);
            state.frames[state.depth++] = IteratorFrame{.nodeIndex = currentIndex, .keyIndex = 0};
            if (header.leaf)
            {
                break;
            }
            currentIndex = child(current, 0);
        }
    }
    else
    {
        ++currentFrame.keyIndex;
    }

    while (state.depth > 0)
    {
        const auto& frame = state.frames[state.depth - 1];
        if (frame.keyIndex < nodeHeader(node(frame.nodeIndex)).numberOfKeys)
        {
            return;
        }
        --state.depth;
    }
}

BTree::Iterator BTree::begin() const
{
    return iteratorAt(0);
}

BTree::Iterator BTree::iteratorAt(const uint64_t index) const
{
    IteratorState state{};
    initializeIterator(state, index);
    return Iterator{buffer, std::move(state)};
}

BTree::Sentinel BTree::end() const
{
    return {};
}

std::span<std::byte> BTree::Iterator::operator*() const
{
    return BTree::load(buffer).iteratorValue(state);
}

BTree::Iterator& BTree::Iterator::operator++()
{
    BTree::load(buffer).advanceIterator(state);
    return *this;
}

bool BTree::Iterator::operator==(Sentinel) const
{
    return not BTree::iteratorValid(state);
}

bool BTree::Iterator::operator!=(const Sentinel sentinel) const
{
    return not(*this == sentinel);
}

uint64_t BTree::bound(const std::span<const std::byte> entry, const Comparator& comparator, const bool upper) const
{
    PRECONDITION(entry.size() == getEntrySize(), "BTree entry has {} bytes, expected {}", entry.size(), getEntrySize());
    PRECONDITION(static_cast<bool>(comparator), "BTree comparator must not be empty");

    uint64_t rank = 0;
    auto currentIndex = header().rootIndex;
    while (currentIndex != INVALID_CHILD_INDEX)
    {
        const auto current = node(currentIndex);
        const auto& currentHeader = nodeHeader(current);
        uint64_t keyIndex = 0;
        for (; keyIndex < currentHeader.numberOfKeys; ++keyIndex)
        {
            const std::span<const std::byte> existing{key(current, keyIndex), getEntrySize()};
            const auto belongsBeforeBound = upper ? not comparator(entry, existing) : comparator(existing, entry);
            if (not belongsBeforeBound)
            {
                break;
            }
            if (not currentHeader.leaf)
            {
                rank += nodeHeader(node(child(current, keyIndex))).subtreeSize;
            }
            ++rank;
        }
        if (currentHeader.leaf)
        {
            return rank;
        }
        currentIndex = child(current, keyIndex);
    }
    return rank;
}

uint64_t BTree::lowerBound(const std::span<const std::byte> entry, const Comparator& comparator) const
{
    return bound(entry, comparator, false);
}

uint64_t BTree::upperBound(const std::span<const std::byte> entry, const Comparator& comparator) const
{
    return bound(entry, comparator, true);
}

BTree::VarSizedAllocation BTree::allocateSpaceForVarSized(AbstractBufferProvider* bufferProvider, const uint64_t size)
{
    PRECONDITION(bufferProvider != nullptr, "BTree buffer provider must not be null");
    /// The record layout requires a non-null destination even for memcpy(..., 0). Reserve one byte for an empty value,
    /// while the VariableSizedAccess still records its logical size as zero.
    const auto storageSize = std::max<uint64_t>(size, 1);
    if (header().varSizedPageIndex != INVALID_CHILD_INDEX)
    {
        auto page = buffer.loadChildBuffer(ChildBufferIndex{header().varSizedPageIndex});
        const auto offset = page.getNumberOfTuples();
        if (offset + storageSize <= page.getBufferSize())
        {
            page.setNumberOfTuples(offset + storageSize);
            return {
                .childIndex = ChildBufferIndex{header().varSizedPageIndex},
                .offset = offset,
                .memory = page.getAvailableMemoryArea<>().subspan(offset, storageSize)};
        }
    }

    TupleBuffer page;
    if (storageSize <= bufferProvider->getBufferSize())
    {
        page = bufferProvider->getBufferBlocking();
    }
    else
    {
        auto unpooledPage = bufferProvider->getUnpooledBuffer(storageSize);
        if (not unpooledPage.has_value())
        {
            throw BufferAllocationFailure("No unpooled TupleBuffer available for oversized BTree variable-sized data");
        }
        page = std::move(*unpooledPage);
    }
    page.setNumberOfTuples(storageSize);
    const auto pageIndex = buffer.storeChildBuffer(page);
    header().varSizedPageIndex = pageIndex.getRawValue();
    page = buffer.loadChildBuffer(pageIndex);
    return {.childIndex = pageIndex, .offset = 0, .memory = page.getAvailableMemoryArea<>().first(storageSize)};
}
}
