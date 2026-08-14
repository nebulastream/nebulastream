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

#include <TestablePagedVector.hpp>

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <ranges>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundSchema.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/PagedVector/PagedVector.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <Interface/Record.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <nautilus/Engine.hpp>
#include <DataStructureTestUtils.hpp>
#include <options.hpp>
#include <val_arith.hpp>
#include <val_concepts.hpp>
#include <val_ptr.hpp>

namespace NES::TestUtils
{

/// NOLINTBEGIN(bugprone-unchecked-optional-access, performance-unnecessary-value-param)
TestablePagedVector::TestablePagedVector(
    const std::vector<DataType>& fieldTypes, AbstractBufferProvider& bufferManager, EngineMode mode, uint64_t pageBufferSize)
    : dataTypes(fieldTypes), bufferManager(bufferManager)
{
    const auto schema = createSchemaFromDataTypes(dataTypes);
    projections
        = schema | std::views::transform([](const auto& field) { return field.getFullyQualifiedName(); }) | std::ranges::to<std::vector>();
    auto layout = std::make_shared<DefaultPagedVectorTupleLayout>(schema);

    nautilus::engine::Options options;
    options.setOption("engine.Compilation", true);
    options.setOption("mlir.enableMultithreading", false);
    engine = std::make_unique<nautilus::engine::NautilusEngine>(makeEngine(mode));

    pagedVector = bufferManager.getUnpooledBuffer(PagedVector::getMainBufferSize()).value();
    PagedVector::init(pagedVector, pageBufferSize, getSizeInBytes(layout->getSchema()));

    pushbackFn.emplace(engine->registerFunction(std::function(
        [layout, dataTypes = dataTypes, projections = projections](
            nautilus::val<TupleBuffer*> pagedVector, nautilus::val<AbstractBufferProvider*> bm, nautilus::val<AnyVec*> rec)
        {
            const Record record = buildRecordFromAnyVec(rec, projections, dataTypes);
            PagedVectorRef pvRef{BorrowedNautilusBuffer::from(pagedVector), layout};
            pvRef.pushBack(record, bm);
        })));

    readAtFn.emplace(engine->registerFunction(std::function(
        [layout, dataTypes = dataTypes, projections = projections](
            nautilus::val<TupleBuffer*> pagedVector, nautilus::val<uint64_t> index, nautilus::val<AnyVec*> out)
        {
            const PagedVectorRef pvRef{BorrowedNautilusBuffer::from(pagedVector), layout};
            auto record = pvRef.at(index);
            storeRecordToAnyVec(out, record, projections, dataTypes);
        })));

    readAll.emplace(engine->registerFunction(std::function(
        [layout, dataTypes = dataTypes, projections = projections](
            nautilus::val<TupleBuffer*> pagedVector, nautilus::val<std::vector<AnyVec>*> outVector)
        {
            const PagedVectorRef pvRef{BorrowedNautilusBuffer::from(pagedVector), layout};
            for (const auto& record : pvRef)
            {
                auto out = anyVecPushBack(outVector, nautilus::val<size_t>(std::ranges::size(layout->getSchema())));
                storeRecordToAnyVec(out, record, projections, dataTypes);
            }
        })));
}

/// NOLINTEND(bugprone-unchecked-optional-access, performance-unnecessary-value-param)

void TestablePagedVector::pushBack(const AnyVec& record)
{
    /// const_cast: pushbackFn's signature requires AnyVec* even though the trace lambda only reads from it.
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast, bugprone-unchecked-optional-access)
    (*pushbackFn)(&pagedVector, &bufferManager, const_cast<AnyVec*>(&record));
}

AnyVec TestablePagedVector::readAt(uint64_t index)
{
    AnyVec out(dataTypes.size());
    /// NOLINTNEXTLINE(bugprone-unchecked-optional-access)
    (*readAtFn)(&pagedVector, index, &out);
    return out;
}

std::vector<AnyVec> TestablePagedVector::toVector()
{
    const auto numEntries = PagedVector::load(pagedVector).getTotalNumberOfRecords();
    std::vector<AnyVec> out;
    out.reserve(numEntries);
    /// NOLINTNEXTLINE(bugprone-unchecked-optional-access)
    (*readAll)(&pagedVector, &out);
    return out;
}

void TestablePagedVector::concatMove(TestablePagedVector& other)
{
    auto otherPagedVector = PagedVector::load(other.pagedVector);
    PagedVector::load(pagedVector).movePagesFrom(otherPagedVector);
}

void TestablePagedVector::concatCopy(TestablePagedVector& other)
{
    auto otherPagedVector = PagedVector::load(other.pagedVector);
    PagedVector::load(pagedVector).copyPagesFrom(bufferManager, otherPagedVector);
}

uint64_t TestablePagedVector::size() const
{
    return PagedVector::load(pagedVector).getTotalNumberOfRecords();
}

PagedVector TestablePagedVector::raw()
{
    return PagedVector::load(pagedVector);
}

bool TestablePagedVector::pagesMatchInitPageSize() const
{
    const auto loaded = PagedVector::load(pagedVector);
    const auto pageBufferSize = loaded.getPageBufferSize();
    for (uint64_t pageIdx = 0; pageIdx < loaded.getNumberOfPages(); ++pageIdx)
    {
        if (pagedVector.loadChildBuffer(ChildBufferIndex{static_cast<uint32_t>(pageIdx)}).getBufferSize() != pageBufferSize)
        {
            return false;
        }
    }
    return true;
}

}
