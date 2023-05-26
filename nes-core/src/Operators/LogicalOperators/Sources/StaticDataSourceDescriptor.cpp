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

#include <Operators/LogicalOperators/Sources/StaticDataSourceDescriptor.hpp>
#include <Sources/DataSource.hpp>
#include <Util/Logger/Logger.hpp>
#include <utility>

namespace NES::Experimental {

StaticDataSourceDescriptor::StaticDataSourceDescriptor(SchemaPtr schema, std::string pathTableFile, bool lateStart)
    : SourceDescriptor(std::move(schema)), pathTableFile(std::move(pathTableFile)), lateStart(lateStart) {}

std::shared_ptr<StaticDataSourceDescriptor>
StaticDataSourceDescriptor::create(const SchemaPtr& schema, std::string pathTableFile, bool lateStart) {
    NES_ASSERT(schema, "StaticDataSourceDescriptor: Invalid schema passed.");
    return std::make_shared<StaticDataSourceDescriptor>(schema, pathTableFile, lateStart);
}
std::string StaticDataSourceDescriptor::toString() const {
    return "StaticDataSourceDescriptor. pathTableFile: " + pathTableFile + " lateStart: " + (lateStart ? "On" : "Off");
}

bool StaticDataSourceDescriptor::equal(SourceDescriptorPtr const& other) const {
    if (!other->instanceOf<StaticDataSourceDescriptor>()) {
        return false;
    }
    auto otherTableDescr = other->as<StaticDataSourceDescriptor>();
    return schema == otherTableDescr->schema && pathTableFile == otherTableDescr->pathTableFile
        && lateStart == otherTableDescr->lateStart;
}

SchemaPtr StaticDataSourceDescriptor::getSchema() const { return schema; }

SourceDescriptorPtr StaticDataSourceDescriptor::copy() {
    return StaticDataSourceDescriptor::create(schema->copy(), pathTableFile, lateStart);
}

std::string StaticDataSourceDescriptor::getPathTableFile() const { return pathTableFile; }

bool StaticDataSourceDescriptor::getLateStart() { return lateStart; };

}// namespace NES::Experimental