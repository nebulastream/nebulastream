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

#include <Execution/Operators/Streaming/HashMapSlice.hpp>
#include <Util/Execution.hpp>

namespace NES::Runtime::Execution
{

/// As a hash join has left and right side, we need to handle the left and right side of the join with one slice
/// Thus, we use a HashMapSlice but duplicate the numberOfHashmaps. The first half belongs to the left and the second half to the right
class HJSlice final : public HashMapSlice
{
public:
    HJSlice(SliceStart sliceStart, SliceEnd sliceEnd, uint64_t numberOfHashMaps);
    ~HJSlice() override;

    Nautilus::Interface::HashMap* getHashMapPtr(const WorkerThreadId workerThreadId, const QueryCompilation::JoinBuildSideType& buildSide) const;
    [[nodiscard]] Nautilus::Interface::HashMap* getHashMapPtrOrCreate(
        WorkerThreadId workerThreadId,
        const QueryCompilation::JoinBuildSideType& buildSide,
        const Operators::CreateNewHashMapSliceArgs& hashMapArgs);
    [[nodiscard]] uint64_t getNumberOfHashMapsPerSide(const QueryCompilation::JoinBuildSideType& buildSide) const;
    void setCleanupFunction(
        const std::function<void(const std::vector<std::unique_ptr<Nautilus::Interface::HashMap>>&)>& cleanupFunction,
        QueryCompilation::JoinBuildSideType buildSide);

private:
    void
    setCleanupFunctionLeft(const std::function<void(const std::vector<std::unique_ptr<Nautilus::Interface::HashMap>>&)>& cleanupFunction);
    void
    setCleanupFunctionRight(const std::function<void(const std::vector<std::unique_ptr<Nautilus::Interface::HashMap>>&)>& cleanupFunction);

    /// We change the access of the inherited method to private as users should use the custom methods
    [[nodiscard]] Nautilus::Interface::HashMap* getHashMapPtr(WorkerThreadId workerThreadId) const override;
    [[nodiscard]] Nautilus::Interface::HashMap*
    getHashMapPtrOrCreate(WorkerThreadId workerThreadId, const Operators::CreateNewHashMapSliceArgs& hashMapArgs) override;
    void setCleanupFunction(
        const std::function<void(const std::vector<std::unique_ptr<Nautilus::Interface::HashMap>>&)>& cleanupFunction) override;

    uint64_t numberOfHashMapsPerSide;
    std::vector<std::unique_ptr<Nautilus::Interface::HashMap>> leftHashMaps;
    std::vector<std::unique_ptr<Nautilus::Interface::HashMap>> rightHashMaps;
    std::function<void(const std::vector<std::unique_ptr<Nautilus::Interface::HashMap>>&)> cleanupFunctionLeft;
    std::function<void(const std::vector<std::unique_ptr<Nautilus::Interface::HashMap>>&)> cleanupFunctionRight;
};

}
