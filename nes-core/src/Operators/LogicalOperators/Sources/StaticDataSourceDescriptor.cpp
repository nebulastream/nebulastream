/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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
#include <utility>

namespace NES::Experimental {

StaticDataSourceDescriptor::StaticDataSourceDescriptor(SchemaPtr schema, std::string pathTableFile)
: SourceDescriptor(std::move(schema)), pathTableFile(std::move(pathTableFile)) {}

std::shared_ptr<StaticDataSourceDescriptor> StaticDataSourceDescriptor::create(const SchemaPtr& schema, std::string pathTableFile) {
    NES_ASSERT(schema, "StaticDataSourceDescriptor: Invalid schema passed.");
    return std::make_shared<StaticDataSourceDescriptor>(schema, pathTableFile);
}
std::string StaticDataSourceDescriptor::toString() {
    return "StaticDataSourceDescriptor. pathTableFile: " + pathTableFile;
}

bool StaticDataSourceDescriptor::equal(SourceDescriptorPtr const& other) {
    if (!other->instanceOf<StaticDataSourceDescriptor>()) {
        return false;
    }
    auto otherTableDescr = other->as<StaticDataSourceDescriptor>();
    return schema == otherTableDescr->schema && pathTableFile == otherTableDescr->pathTableFile;
}

SchemaPtr StaticDataSourceDescriptor::getSchema() const { return schema; }

std::string StaticDataSourceDescriptor::getPathTableFile() const { return pathTableFile; }
}// namespace NES