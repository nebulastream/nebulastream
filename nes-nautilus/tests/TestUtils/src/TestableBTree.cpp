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

#include <TestableBTree.hpp>

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <ranges>
#include <tuple>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <DataTypes/VarVal.hpp>
#include <DataTypes/VariableSizedData.hpp>
#include <Interface/BTree/BTree.hpp>
#include <Interface/BTree/BTreeRef.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/Record.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <nautilus/Engine.hpp>
#include <nautilus/function.hpp>
#include <nautilus/select.hpp>
#include <DataStructureTestUtils.hpp>
#include <ErrorHandling.hpp>
#include <static.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_ptr.hpp>

namespace NES::TestUtils
{
namespace
{
std::pair<nautilus::val<bool>, nautilus::val<bool>> compareNonNullValues(const VarVal& lhs, const VarVal& rhs, const DataType::Type type)
{
    if (type == DataType::Type::VARSIZED)
    {
        const auto lhsValue = lhs.getRawValueAs<VariableSizedData>();
        const auto rhsValue = rhs.getRawValueAs<VariableSizedData>();
        const auto comparison = nautilus::invoke(
            +[](const int8_t* lhsData, const uint64_t lhsSize, const int8_t* rhsData, const uint64_t rhsSize) -> int8_t
            {
                const auto commonSize = std::min(lhsSize, rhsSize);
                if (commonSize > 0)
                {
                    if (const auto prefixComparison = std::memcmp(lhsData, rhsData, commonSize); prefixComparison != 0)
                    {
                        return prefixComparison < 0 ? int8_t{-1} : int8_t{1};
                    }
                }
                if (lhsSize == rhsSize)
                {
                    return 0;
                }
                return lhsSize < rhsSize ? int8_t{-1} : int8_t{1};
            },
            lhsValue.getContent(),
            lhsValue.getSize(),
            rhsValue.getContent(),
            rhsValue.getSize());
        return {comparison < 0, comparison == 0};
    }

    auto less = (lhs < rhs).getRawValueAs<nautilus::val<bool>>();
    auto equal = (lhs == rhs).getRawValueAs<nautilus::val<bool>>();
    if (type == DataType::Type::FLOAT32)
    {
        const auto lhsIsNan = nautilus::invoke(+[](float value) { return std::isnan(value); }, lhs.getRawValueAs<nautilus::val<float>>());
        const auto rhsIsNan = nautilus::invoke(+[](float value) { return std::isnan(value); }, rhs.getRawValueAs<nautilus::val<float>>());
        less = nautilus::select(lhsIsNan, nautilus::val<bool>{false}, nautilus::select(rhsIsNan, nautilus::val<bool>{true}, less));
        equal = (lhsIsNan and rhsIsNan) or (not lhsIsNan and not rhsIsNan and equal);
    }
    else if (type == DataType::Type::FLOAT64)
    {
        const auto lhsIsNan = nautilus::invoke(+[](double value) { return std::isnan(value); }, lhs.getRawValueAs<nautilus::val<double>>());
        const auto rhsIsNan = nautilus::invoke(+[](double value) { return std::isnan(value); }, rhs.getRawValueAs<nautilus::val<double>>());
        less = nautilus::select(lhsIsNan, nautilus::val<bool>{false}, nautilus::select(rhsIsNan, nautilus::val<bool>{true}, less));
        equal = (lhsIsNan and rhsIsNan) or (not lhsIsNan and not rhsIsNan and equal);
    }
    return {less, equal};
}

std::pair<nautilus::val<bool>, nautilus::val<bool>> compareValues(const VarVal& lhs, const VarVal& rhs, const DataType::Type type)
{
    const auto lhsIsNull = lhs.isNull();
    const auto rhsIsNull = rhs.isNull();
    const auto bothNonNull = not lhsIsNull and not rhsIsNull;
    const auto [valueLess, valueEqual] = compareNonNullValues(lhs, rhs, type);
    return {
        nautilus::select(bothNonNull, valueLess, lhsIsNull and not rhsIsNull), (lhsIsNull and rhsIsNull) or (bothNonNull and valueEqual)};
}

BTreeComparator::RecordComparator makeComparator(std::vector<Record::RecordFieldIdentifier> projections, std::vector<DataType> dataTypes)
{
    std::vector<std::tuple<Record::RecordFieldIdentifier, DataType>> fields;
    fields.reserve(projections.size());
    for (size_t index = 0; index < projections.size(); ++index)
    {
        fields.emplace_back(std::move(projections[index]), dataTypes[index]);
    }
    return [fields = std::move(fields)](const Record& lhs, const Record& rhs) -> nautilus::val<bool>
    {
        nautilus::val<bool> result = false;
        nautilus::val<bool> equalSoFar = true;
        for (const auto& [field, type] : nautilus::static_iterable(fields))
        {
            const auto [fieldLess, fieldEqual] = compareValues(lhs.read(field), rhs.read(field), type.type);
            result = nautilus::select(equalSoFar, fieldLess, result);
            equalSoFar = equalSoFar and fieldEqual;
        }
        return result;
    };
}
}

/// NOLINTBEGIN(bugprone-unchecked-optional-access, performance-unnecessary-value-param)
TestableBTree::TestableBTree(const std::vector<DataType>& fieldTypes, AbstractBufferProvider& bufferManager, const EngineMode mode)
    : dataTypes(fieldTypes), bufferManager(bufferManager)
{
    const auto schema = createSchemaFromDataTypes(dataTypes);
    const auto projections
        = schema | std::views::transform([](const auto& field) { return field.getFullyQualifiedName(); }) | std::ranges::to<std::vector>();
    auto layout = std::make_shared<DefaultBTreeTupleLayout>(schema);
    const auto recordComparator = makeComparator(projections, dataTypes);

    engine = std::make_unique<nautilus::engine::NautilusEngine>(makeEngine(mode));
    CompilationContext compilationContext{engine->createModule()};
    comparator = std::make_shared<BTreeComparator>(compilationContext, layout, "testableBTreeComparator", recordComparator);
    tree = bufferManager.getUnpooledBuffer(BTree::getMainBufferSize()).value();
    BTree::init(tree, bufferManager.getBufferSize(), schema.getSizeInBytes());

    appendFn.emplace(compilationContext.registerFunction(
        std::function(
            [layout, comparator = comparator, projections, dataTypes = dataTypes](
                nautilus::val<TupleBuffer*> tree, nautilus::val<AbstractBufferProvider*> provider, nautilus::val<AnyVec*> values)
            {
                const BTreeRef ref{BorrowedNautilusBuffer::from(tree), layout};
                ref.append(buildRecordFromAnyVec(values, projections, dataTypes), provider, *comparator);
            }),
        "testableBTreeAppend"));

    atFn.emplace(compilationContext.registerFunction(
        std::function(
            [layout, projections, dataTypes = dataTypes](
                nautilus::val<TupleBuffer*> tree, nautilus::val<uint64_t> index, nautilus::val<AnyVec*> output)
            {
                const BTreeRef ref{BorrowedNautilusBuffer::from(tree), layout};
                storeRecordToAnyVec(output, ref.at(index), projections, dataTypes);
            }),
        "testableBTreeAt"));

    if (std::ranges::none_of(dataTypes, [](const auto type) { return type.type == DataType::Type::VARSIZED; }))
    {
        lowerBoundFn.emplace(compilationContext.registerFunction(
            std::function(
                [layout, comparator = comparator, projections, dataTypes = dataTypes](
                    nautilus::val<TupleBuffer*> tree, nautilus::val<AnyVec*> values)
                {
                    const BTreeRef ref{BorrowedNautilusBuffer::from(tree), layout};
                    return ref.lowerBound(buildRecordFromAnyVec(values, projections, dataTypes), *comparator);
                }),
            "testableBTreeLowerBound"));

        upperBoundFn.emplace(compilationContext.registerFunction(
            std::function(
                [layout, comparator = comparator, projections, dataTypes = dataTypes](
                    nautilus::val<TupleBuffer*> tree, nautilus::val<AnyVec*> values)
                {
                    const BTreeRef ref{BorrowedNautilusBuffer::from(tree), layout};
                    return ref.upperBound(buildRecordFromAnyVec(values, projections, dataTypes), *comparator);
                }),
            "testableBTreeUpperBound"));
    }

    readAllFn.emplace(compilationContext.registerFunction(
        std::function(
            [layout, projections, dataTypes = dataTypes](nautilus::val<TupleBuffer*> tree, nautilus::val<std::vector<AnyVec>*> output)
            {
                const BTreeRef ref{BorrowedNautilusBuffer::from(tree), layout};
                for (auto iterator = ref.begin(); iterator != ref.end(); ++iterator)
                {
                    auto record = anyVecPushBack(output, nautilus::val<size_t>{dataTypes.size()});
                    storeRecordToAnyVec(record, *iterator, projections, dataTypes);
                }
            }),
        "testableBTreeReadAll"));

    readRangeFn.emplace(compilationContext.registerFunction(
        std::function(
            [layout, projections, dataTypes = dataTypes](
                nautilus::val<TupleBuffer*> tree,
                nautilus::val<uint64_t> begin,
                nautilus::val<uint64_t> end,
                nautilus::val<std::vector<AnyVec>*> output)
            {
                const BTreeRef ref{BorrowedNautilusBuffer::from(tree), layout};
                for (auto iterator = ref.iteratorAt(begin); iterator != ref.end(end); ++iterator)
                {
                    auto record = anyVecPushBack(output, nautilus::val<size_t>{dataTypes.size()});
                    storeRecordToAnyVec(record, *iterator, projections, dataTypes);
                }
            }),
        "testableBTreeReadRange"));

    sizeFn.emplace(compilationContext.registerFunction(
        std::function(
            [layout](nautilus::val<TupleBuffer*> tree)
            {
                const BTreeRef ref{BorrowedNautilusBuffer::from(tree), layout};
                return ref.size();
            }),
        "testableBTreeSize"));
    compilationContext.compile();
}

/// NOLINTEND(bugprone-unchecked-optional-access, performance-unnecessary-value-param)

void TestableBTree::append(const AnyVec& record)
{
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast, bugprone-unchecked-optional-access)
    (*appendFn)(&tree, &bufferManager, const_cast<AnyVec*>(&record));
}

AnyVec TestableBTree::at(const uint64_t index)
{
    AnyVec result(dataTypes.size());
    /// NOLINTNEXTLINE(bugprone-unchecked-optional-access)
    (*atFn)(&tree, index, &result);
    return result;
}

uint64_t TestableBTree::lowerBound(const AnyVec& record)
{
    INVARIANT(lowerBoundFn.has_value(), "Variable-sized BTree search keys are not supported");
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast, bugprone-unchecked-optional-access)
    return (*lowerBoundFn)(&tree, const_cast<AnyVec*>(&record));
}

uint64_t TestableBTree::upperBound(const AnyVec& record)
{
    INVARIANT(upperBoundFn.has_value(), "Variable-sized BTree search keys are not supported");
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast, bugprone-unchecked-optional-access)
    return (*upperBoundFn)(&tree, const_cast<AnyVec*>(&record));
}

std::vector<AnyVec> TestableBTree::toVector()
{
    std::vector<AnyVec> result;
    result.reserve(size());
    /// NOLINTNEXTLINE(bugprone-unchecked-optional-access)
    (*readAllFn)(&tree, &result);
    return result;
}

std::vector<AnyVec> TestableBTree::range(const uint64_t begin, const uint64_t end)
{
    std::vector<AnyVec> result;
    result.reserve(end - begin);
    /// NOLINTNEXTLINE(bugprone-unchecked-optional-access)
    (*readRangeFn)(&tree, begin, end, &result);
    return result;
}

uint64_t TestableBTree::size()
{
    /// NOLINTNEXTLINE(bugprone-unchecked-optional-access)
    return (*sizeFn)(&tree);
}

}
