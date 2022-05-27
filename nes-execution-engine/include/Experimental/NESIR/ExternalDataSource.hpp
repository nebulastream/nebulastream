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

#ifndef NES_EXTERNALDATASOURCE_HPP
#define NES_EXTERNALDATASOURCE_HPP

#include <Experimental/NESIR/Operations/Operation.hpp>
#include <vector>

namespace NES::ExecutionEngine::Experimental::IR {

class ExternalDataSource {
  public:
    enum ExternalDataSourceType{TupleBuffer};
    ExternalDataSource(ExternalDataSourceType externalDataSourceType, std::string identfifier, std::vector<Operations::Operation::BasicType> types);
    ~ExternalDataSource() = default;

    [[nodiscard]] ExternalDataSourceType getExternalDataSourceType() const;

    [[nodiscard]] const std::string &getIdentifier() const;

    [[nodiscard]] const std::vector<Operations::Operation::BasicType> &getTypes() const;

private:
    ExternalDataSourceType externalDataSourceType;
    std::string identifier;
    std::vector<Operations::Operation::BasicType> types;
};
using ExternalDataSourcePtr = std::shared_ptr<ExternalDataSource>;

}// namespace NES

#endif //NES_DATASOURCE_HPP
