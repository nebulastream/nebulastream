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

#include <API/Schema.hpp>
#include <Operators/LogicalOperators/Sinks/MaterializedViewSinkDescriptor.hpp>
#include <Sources/DataSource.hpp>
#include <utility>

namespace NES::Experimental {

MaterializedViewSinkDescriptor::MaterializedViewSinkDescriptor(uint64_t mViewId, Schema schema)
    : schema(schema), mViewId(mViewId) {
    NES_ASSERT(this->mViewId > 0, "invalid materialized view id");
}

MaterializedViewSinkDescriptorPtr create(uint64_t mViewId, Schema schema) {
    return std::make_shared<MaterializedViewSinkDescriptor>(MaterializedViewSinkDescriptor(mViewId, schema));
}

std::string MaterializedViewSinkDescriptor::toString() { return "MaterializedViewSinkDescriptor"; }

bool MaterializedViewSinkDescriptor::equal(SinkDescriptorPtr const& other) {
    if (!other->instanceOf<MaterializedViewSinkDescriptor>()) {
        return false;
    }
    auto otherMemDescr = other->as<MaterializedViewSinkDescriptor>();
    return true;//TODO: schema == otherMemDescr->schema;
}

uint64_t MaterializedViewSinkDescriptor::getMViewId() { return mViewId; }
Schema MaterializedViewSinkDescriptor::getSchema() { return schema; }
}// namespace NES::Experimental