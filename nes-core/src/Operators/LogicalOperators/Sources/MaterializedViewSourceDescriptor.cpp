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

#include <Operators/LogicalOperators/Sources/MaterializedViewSourceDescriptor.hpp>
#include <Sources/DataSource.hpp>
#include <utility>

namespace NES::Experimental::MaterializedView {

MaterializedViewSourceDescriptor::MaterializedViewSourceDescriptor(SchemaPtr schema, uint64_t viewId)
    : SourceDescriptor(std::move(schema)), viewId(viewId) {}

SourceDescriptorPtr MaterializedViewSourceDescriptor::create(const SchemaPtr& schema, uint64_t viewId) {
    NES_ASSERT(schema, "invalid schema");
    return std::make_shared<MaterializedViewSourceDescriptor>(
        MaterializedViewSourceDescriptor(std::move(schema), std::move(viewId)));
}
std::string MaterializedViewSourceDescriptor::toString() { return "MaterializedViewSourceDescriptor"; }

bool MaterializedViewSourceDescriptor::equal(SourceDescriptorPtr const& other) {
    if (!other->instanceOf<MaterializedViewSourceDescriptor>()) {
        return false;
    }
    auto otherMemDescr = other->as<MaterializedViewSourceDescriptor>();
    return schema == otherMemDescr->schema;
}

uint64_t MaterializedViewSourceDescriptor::getViewId() const { return viewId; }

SourceDescriptorPtr MaterializedViewSourceDescriptor::copy() {
    auto copy = MaterializedViewSourceDescriptor::create(schema->copy(), viewId);
    copy->setPhysicalSourceName(physicalSourceName);
    return copy;
}
}// namespace NES::Experimental::MaterializedView