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
#include <DataTypes/DataType.hpp>
#include <Interface/PagedVector/PagedVector.hpp>
#include <Interface/Record.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <nautilus/Engine.hpp>
#include <DataStructureTestUtils.hpp>

namespace NES::TestUtils
{

/// Mirrors std::vector<AnyVec> but internally operates on a real PagedVector.
/// insert and readAt are Nautilus-compiled and invoked via function-pointer dispatch.
class TestablePagedVector
{
public:
    /// @param pageBufferSize page size the PagedVector is initialized with. Pass bufferManager.getBufferSize() for provider-buffer-sized
    /// pages, or a smaller value for the smaller-pages configuration the hash-join build uses.
    TestablePagedVector(
        const std::vector<DataType>& fieldTypes, AbstractBufferProvider& bufferManager, EngineMode mode, uint64_t pageBufferSize);

    ~TestablePagedVector() = default;
    TestablePagedVector(const TestablePagedVector&) = delete;
    TestablePagedVector& operator=(const TestablePagedVector&) = delete;
    TestablePagedVector(TestablePagedVector&&) = default;
    TestablePagedVector& operator=(TestablePagedVector&&) = delete;

    void pushBack(const AnyVec& record);

    AnyVec readAt(uint64_t index);

    void replaceAt(uint64_t index, const AnyVec& record);

    std::vector<AnyVec> toVector();

    void concatMove(TestablePagedVector& other);

    void concatCopy(TestablePagedVector& other);

    [[nodiscard]] uint64_t size() const;

    PagedVector raw();

    /// Every page must be exactly the init-time page size, not the provider's buffer size.
    [[nodiscard]] bool pagesMatchInitPageSize() const;

private:
    std::vector<DataType> dataTypes;
    TupleBuffer pagedVector;
    /// NOLINTNEXTLINE(cppcoreguidelines-avoid-const-or-ref-data-members)
    AbstractBufferProvider& bufferManager;
    std::vector<Record::RecordFieldIdentifier> projections;
    std::unique_ptr<nautilus::engine::NautilusEngine> engine;
    std::optional<nautilus::engine::CompiledFunction<void(TupleBuffer*, AbstractBufferProvider*, AnyVec*)>> pushbackFn;
    std::optional<nautilus::engine::CompiledFunction<void(TupleBuffer*, uint64_t, AnyVec*)>> readAtFn;
    std::optional<nautilus::engine::CompiledFunction<void(TupleBuffer*, uint64_t, AbstractBufferProvider*, AnyVec*)>> replaceAtFn;
    std::optional<nautilus::engine::CompiledFunction<void(TupleBuffer*, std::vector<AnyVec>*)>> readAll;
};

}
