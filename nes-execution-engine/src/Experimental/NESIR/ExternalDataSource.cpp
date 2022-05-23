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

#include <Experimental/NESIR/ExternalDataSource.hpp>

namespace NES {
    ExternalDataSource::ExternalDataSource(ExternalDataSourceType externalDataSourceType, std::string identfifier,
                           std::vector<Operation::BasicType> types) : externalDataSourceType(externalDataSourceType), identifier(identfifier), types(types) {}

    ExternalDataSource::ExternalDataSourceType ExternalDataSource::getExternalDataSourceType() const {
        return externalDataSourceType;
    }

    const std::string& ExternalDataSource::getIdentifier() const {
        return identifier;
    }

    const std::vector<Operation::BasicType>& ExternalDataSource::getTypes() const {
        return types;
    }

}// namespace NES