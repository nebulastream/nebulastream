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

#ifndef NES_COMMON_INCLUDE_RECONFIGURATION_METADATA_RECONFIGURATIONMETADATA_HPP_
#define NES_COMMON_INCLUDE_RECONFIGURATION_METADATA_RECONFIGURATIONMETADATA_HPP_

#include <iostream>
#include <memory>

namespace NES {

enum class ReconfigurationMetadataType : uint8_t { DrainQuery = 1, UpdateAndDrainQuery = 2, UpdateQuery = 3 };

class ReconfigurationMetadata : public std::enable_shared_from_this<ReconfigurationMetadata> {

  public:
    explicit ReconfigurationMetadata(ReconfigurationMetadataType reconfigurationMetaDataType)
        : reconfigurationMetadataType(reconfigurationMetaDataType) {}

    virtual ~ReconfigurationMetadata() = default;

    template<class T>
    bool instanceOf() const {
        if (dynamic_cast<const T*>(this)) {
            return true;
        }
        return false;
    };

    template<class T>
    std::shared_ptr<const T> as() const {
        if (instanceOf<T>()) {
            return std::dynamic_pointer_cast<const T>(this->shared_from_this());
        }
        throw std::logic_error(std::string("We performed an invalid cast of reconfiguration metadata to type ")
                               + typeid(T).name());
    }

    const ReconfigurationMetadataType reconfigurationMetadataType;
};
using ReconfigurationMetadatatPtr = std::shared_ptr<ReconfigurationMetadata>;
}// namespace NES
#endif// NES_COMMON_INCLUDE_RECONFIGURATION_METADATA_RECONFIGURATIONMETADATA_HPP_
