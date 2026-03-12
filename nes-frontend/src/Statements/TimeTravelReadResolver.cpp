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

#include <Replay/TimeTravelReadResolver.hpp>

#include <fstream>
#include <filesystem>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <DataTypes/Schema.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Sources/InlineSourceLogicalOperator.hpp>
#include <Operators/Sources/SourceNameLogicalOperator.hpp>
#include <RecordingCatalog.hpp>
#include <Sources/BinaryStoreSource.hpp>
#include <Sources/SourceCatalog.hpp>

namespace NES
{

namespace
{
Schema resolveTimeTravelReadSchema(const SharedPtr<const SourceCatalog>& sourceCatalog, const std::string& recordingFilePath)
{
    Schema schema;
    std::ifstream probe(recordingFilePath, std::ios::binary);
    if (probe.good())
    {
        return BinaryStoreSource::readSchemaFromFile(recordingFilePath);
    }

    for (const auto& logicalSource : sourceCatalog->getAllLogicalSources())
    {
        for (const auto& field : *logicalSource.getSchema())
        {
            schema.addField(field.getUnqualifiedName(), field.dataType);
        }
        break;
    }
    return schema;
}

LogicalOperator replaceTimeTravelReadSource(
    const LogicalOperator& current,
    const SharedPtr<const SourceCatalog>& sourceCatalog,
    const RecordingCatalog* recordingCatalog)
{
    std::vector<LogicalOperator> newChildren;
    for (const auto& child : current.getChildren())
    {
        newChildren.emplace_back(replaceTimeTravelReadSource(child, sourceCatalog, recordingCatalog));
    }

    if (const auto sourceOp = current.tryGetAs<SourceNameLogicalOperator>())
    {
        if (sourceOp.value()->getLogicalSourceName() == "TIME_TRAVEL_READ")
        {
            if (recordingCatalog == nullptr)
            {
                throw UnsupportedQuery("TIME_TRAVEL_READ requires a coordinator-managed active replay recording");
            }

            const auto activeRecording = recordingCatalog->getTimeTravelReadRecording();
            if (!activeRecording.has_value())
            {
                throw UnsupportedQuery("TIME_TRAVEL_READ requires an active replay recording, but none is ready");
            }
            if (activeRecording->filePath.empty())
            {
                throw UnsupportedQuery(
                    "TIME_TRAVEL_READ requires a concrete active replay recording file for recording {}",
                    activeRecording->id);
            }

            const auto schema = resolveTimeTravelReadSchema(sourceCatalog, activeRecording->filePath);
            std::unordered_map<std::string, std::string> sourceConfig{
                {"file_path", activeRecording->filePath},
                {"recording_id", activeRecording->id.getRawValue()}};
            std::unordered_map<std::string, std::string> parserConfig{{"type", "NATIVE"}};
            const InlineSourceLogicalOperator inlineOp{"BinaryStore", schema, std::move(sourceConfig), std::move(parserConfig)};
            return inlineOp.withChildren(newChildren);
        }
    }

    return current.withChildren(std::move(newChildren));
}
}

void resolveTimeTravelReadSources(
    LogicalPlan& plan, const SharedPtr<const SourceCatalog>& sourceCatalog, const RecordingCatalog* recordingCatalog)
{
    std::vector<LogicalOperator> newRoots;
    for (const auto& root : plan.getRootOperators())
    {
        newRoots.emplace_back(replaceTimeTravelReadSource(root, sourceCatalog, recordingCatalog));
    }
    plan = plan.withRootOperators(newRoots);
}

}
