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

#include <string>

#include <UDFs/JavaUdfCatalog.hpp>
#include <UDFs/UdfException.hpp>
#include <Util/Logger.hpp>

namespace NES {

// TODO comment about Clang-tidy
void JavaUdfCatalog::registerJavaUdf(const std::string& name, JavaUdfDescriptorPtr descriptor) {
    NES_DEBUG("Registering Java UDF '" << name << "'");
    if (descriptor == nullptr) {
        throw UdfException("Java UDF descriptor must not be null");
    }
    if (auto success = udfStore.insert({name, descriptor}).second; !success) {
        std::stringstream ss;
        ss << "Java UDF '" << name << "' already exists";
        throw UdfException(ss.str());
    }
}

JavaUdfDescriptorPtr JavaUdfCatalog::getUdfDescriptor(const std::string& name) {
    NES_DEBUG("Looking up descriptor for Java UDF '" << name << "'");
    auto entry = udfStore.find(name);
    if (entry == udfStore.end()) {
        NES_DEBUG("Java UDF '" << name << "' does not exist");
        return nullptr;
    }
    return entry->second;
}

bool JavaUdfCatalog::removeUdf(const std::string& name) {
    NES_DEBUG("Removing Java UDF '" << name << "'");
    auto entry = udfStore.find(name);
    if (entry == udfStore.end()) {
        NES_DEBUG("Did not find Java UDF '" << name << "'");
        return false;
    }
    udfStore.erase(entry);
    return true;
}

const std::vector<std::string> JavaUdfCatalog::listUdfs() const {
    NES_DEBUG("Listing names of Java UDFs");
    auto list = std::vector<std::string> {};
    list.reserve(udfStore.size());
    for (const auto& [key, _] : udfStore) {
        list.push_back(key);
    }
    return list;
}

} // namespace NES