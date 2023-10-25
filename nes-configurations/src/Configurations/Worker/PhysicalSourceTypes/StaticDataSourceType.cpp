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

#include <Configurations/Worker/PhysicalSourceTypes/StaticDataSourceType.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <sstream>
#include <utility>

namespace NES::Experimental {

using namespace Configurations;

namespace detail {

//struct MemoryAreaDeleter {
//    void operator()(uint8_t* ptr) const { free(ptr); }
//};

}// namespace detail

/*
static StaticDataSourceTypePtr create(const std::string& sourceType,
                                      const std::string& pathTableFile,
                                      uint64_t numBuffersToProcess);

GatheringMode getGatheringMode() const;

SourceMode getSourceMode() const;

uint64_t getTaskQueueId() const;

std::string toString() override;

bool equal(const PhysicalSourceTypePtr& other) override;

*/

StaticDataSourceType::StaticDataSourceType(const std::string& logicalSourceName,
                                           const std::string& physicalSourceName,
                                           const std::string& pathTableFile,
                                           uint64_t numBuffersToProcess,
                                           SourceMode sourceMode,
                                           uint64_t taskQueueId,
                                           bool lateStart)
    : PhysicalSourceType(logicalSourceName, physicalSourceName, SourceType::STATIC_DATA_SOURCE),
      pathTableFile(std::move(pathTableFile)), numBuffersToProcess(numBuffersToProcess), sourceMode(sourceMode),
      taskQueueId(taskQueueId), lateStart(lateStart) {}

StaticDataSourceTypePtr StaticDataSourceType::create(const std::string& logicalSourceName,
                                                     const std::string& physicalSourceName,
                                                     const std::string& pathTableFile,
                                                     uint64_t numBuffersToProcess,
                                                     const std::string& sourceMode,
                                                     uint64_t taskQueueId,
                                                     bool lateStart) {
    // todo check validity of path
    SourceMode sourceModeEnum = magic_enum::enum_cast<SourceMode>(sourceMode).value();
    return std::make_shared<StaticDataSourceType>(logicalSourceName,
                                                  physicalSourceName,
                                                  pathTableFile,
                                                  numBuffersToProcess,
                                                  sourceModeEnum,
                                                  taskQueueId,
                                                  lateStart);
}

SourceMode StaticDataSourceType::getSourceMode() const { return sourceMode; }

void StaticDataSourceType::reset() {
    //nothing
}

std::string StaticDataSourceType::getPathTableFile() { return pathTableFile; };

bool StaticDataSourceType::getLateStart() { return lateStart; };

std::string StaticDataSourceType::toString() {
    std::stringstream ss;
    ss << "StaticDataSourceType => {\n";
    ss << "pathTableFile :" << pathTableFile;
    ss << "numBuffersToProcess :" << numBuffersToProcess;
    ss << "SourceMode :" << std::string(magic_enum::enum_name(sourceMode));
    ss << "taskQueueId :" << taskQueueId;
    ss << "lateStart :" << lateStart;
    ss << "\n}";
    return ss.str();
}

bool StaticDataSourceType::equal(const PhysicalSourceTypePtr& other) {
    if (!other->instanceOf<StaticDataSourceType>()) {
        return false;
    }
    auto otherSourceConfig = other->as<StaticDataSourceType>();
    return pathTableFile == otherSourceConfig->pathTableFile && numBuffersToProcess == otherSourceConfig->numBuffersToProcess
        && sourceMode == otherSourceConfig->sourceMode && taskQueueId == otherSourceConfig->taskQueueId
        && lateStart == otherSourceConfig->lateStart;
}

}// namespace NES::Experimental
