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

#include <string>

#include <Catalogs/UDF/UdfCatalog.hpp>
#include <Exceptions/UdfException.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Catalogs::UDF {

std::unique_ptr<UdfCatalog> UdfCatalog::create() { return std::make_unique<UdfCatalog>(); }

void UdfCatalog::registerUdf(const std::string& name, UdfDescriptorPtr descriptor) {
    NES_DEBUG("Registering UDF '" << name << "'");
    if (descriptor == nullptr) {
        throw UdfException("UDF descriptor must not be null");
    }
    if (auto success = udfStore.insert({name, descriptor}).second; !success) {
        std::stringstream ss;
        ss << "UDF '" << name << "' already exists";
        throw UdfException(ss.str());
    }
}

UdfDescriptorPtr UdfCatalog::getUdfDescriptor(const std::string& name) {
    NES_DEBUG("Looking up descriptor for UDF '" << name << "'");
    auto entry = udfStore.find(name);
    if (entry == udfStore.end()) {
        NES_DEBUG("UDF '" << name << "' does not exist");
        return nullptr;
    }
    return entry->second;
}

bool UdfCatalog::removeUdf(const std::string& name) {
    NES_DEBUG("Removing UDF '" << name << "'");
    auto entry = udfStore.find(name);
    if (entry == udfStore.end()) {
        NES_DEBUG("Did not find UDF '" << name << "'");
        // Removing an unregistered UDF is not an error condition
        // because it could have been removed by another user.
        // We just notify the user by returning false.
        return false;
    }
    udfStore.erase(entry);
    return true;
}

std::vector<std::string> UdfCatalog::listUdfs() const {
    NES_DEBUG("Listing names of registered UDFs");
    auto list = std::vector<std::string>{};
    list.reserve(udfStore.size());
    for (const auto& [key, _] : udfStore) {
        list.push_back(key);
    }
    return list;
}

}// namespace NES::Catalogs::UDF