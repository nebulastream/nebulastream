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

#include <Operators/LogicalOperators/Sources/TableSourceDescriptor.hpp>
#include <Sources/DataSource.hpp>
#include <utility>

namespace NES {

TableSourceDescriptor::TableSourceDescriptor(SchemaPtr schema, std::string pathTableFile)
: SourceDescriptor(std::move(schema)), pathTableFile(std::move(pathTableFile)) {}

std::shared_ptr<TableSourceDescriptor> TableSourceDescriptor::create(const SchemaPtr& schema, std::string pathTableFile) {
    NES_ASSERT(schema, "TableSourceDescriptor: Invalid schema passed.");
    return std::make_shared<TableSourceDescriptor>(schema, pathTableFile);
}
std::string TableSourceDescriptor::toString() {
    return "TableSourceDescriptor. pathTableFile: " + pathTableFile;
}

bool TableSourceDescriptor::equal(SourceDescriptorPtr const& other) {
    if (!other->instanceOf<TableSourceDescriptor>()) {
        return false;
    }
    auto otherTableDescr = other->as<TableSourceDescriptor>();
    return schema == otherTableDescr->schema && pathTableFile == otherTableDescr->pathTableFile;
}

SchemaPtr TableSourceDescriptor::getSchema() const { return schema; }

std::string TableSourceDescriptor::getPathTableFile() const { return pathTableFile; }
}// namespace NES