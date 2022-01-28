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

#include <Operators/LogicalOperators/Sinks/MaterializedViewSinkDescriptor.hpp>
#include <Sources/DataSource.hpp>
#include <utility>

namespace NES::Experimental::MaterializedView {

MaterializedViewSinkDescriptor::MaterializedViewSinkDescriptor(uint64_t viewId)
    : viewId(viewId) {
    NES_ASSERT(this->viewId > 0, "invalid materialized view id");
}

SinkDescriptorPtr MaterializedViewSinkDescriptor::create(uint64_t viewId) {
    return std::make_shared<MaterializedViewSinkDescriptor>(MaterializedViewSinkDescriptor(viewId));
}

std::string MaterializedViewSinkDescriptor::toString() { return "MaterializedViewSinkDescriptor"; }

bool MaterializedViewSinkDescriptor::equal(SinkDescriptorPtr const& other) {
    if (!other->instanceOf<MaterializedViewSinkDescriptor>()) {
        return false;
    }
    auto otherSinkDescriptor = other->as<MaterializedViewSinkDescriptor>();
    return otherSinkDescriptor->getViewId() == viewId;
}

uint64_t MaterializedViewSinkDescriptor::getViewId() { return viewId; }
}// namespace NES::Experimental::MaterializedView