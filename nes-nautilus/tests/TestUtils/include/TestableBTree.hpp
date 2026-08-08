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
#include <Interface/BTree/BTreeRef.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <nautilus/Engine.hpp>
#include <CompilationContext.hpp>
#include <DataStructureTestUtils.hpp>

namespace NES::TestUtils
{

/// Mirrors a sorted std::vector<AnyVec> but stores records in a real BTree.
/// All record-facing operations go through BTreeRef in the selected Nautilus engine.
class TestableBTree
{
public:
    TestableBTree(const std::vector<DataType>& fieldTypes, AbstractBufferProvider& bufferManager, EngineMode mode);

    ~TestableBTree() = default;
    TestableBTree(const TestableBTree&) = delete;
    TestableBTree& operator=(const TestableBTree&) = delete;
    TestableBTree(TestableBTree&&) = default;
    TestableBTree& operator=(TestableBTree&&) = delete;

    void append(const AnyVec& record);

    AnyVec at(uint64_t index);

    std::vector<AnyVec> toVector();

    uint64_t size();

private:
    std::vector<DataType> dataTypes;
    TupleBuffer tree;
    /// NOLINTNEXTLINE(cppcoreguidelines-avoid-const-or-ref-data-members)
    AbstractBufferProvider& bufferManager;
    std::unique_ptr<nautilus::engine::NautilusEngine> engine;
    std::shared_ptr<BTreeComparator> comparator;
    std::optional<PipelineFunction<void(TupleBuffer*, AbstractBufferProvider*, AnyVec*)>> appendFn;
    std::optional<PipelineFunction<void(TupleBuffer*, uint64_t, AnyVec*)>> atFn;
    std::optional<PipelineFunction<void(TupleBuffer*, std::vector<AnyVec>*)>> readAllFn;
    std::optional<PipelineFunction<uint64_t(TupleBuffer*)>> sizeFn;
};

}
