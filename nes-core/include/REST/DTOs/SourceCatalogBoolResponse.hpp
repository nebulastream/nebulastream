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

#ifndef NES_NES_CORE_INCLUDE_REST_DTOS_SOURCECATALOGBOOLRESPONSE_HPP_
#define NES_NES_CORE_INCLUDE_REST_DTOS_SOURCECATALOGBOOLRESPONSE_HPP_

#include <oatpp/core/data/mapping/type/Object.hpp>
#include <oatpp/core/macro/codegen.hpp>

/* Begin DTO code-generation */
#include OATPP_CODEGEN_BEGIN(DTO)

namespace NES {

/**
 * Message Data-Transfer-Object containing information as string
 */
class SourceCatalogBoolResponse: public oatpp::DTO {
    DTO_INIT(SourceCatalogBoolResponse, DTO /* Extends */)
    DTO_FIELD_INFO(success) { info->description = "true if logical source was added successfully"; }
    DTO_FIELD(Boolean, success);
};
} //namespace NES
#include OATPP_CODEGEN_END(DTO)

#endif//NES_NES_CORE_INCLUDE_REST_DTOS_SOURCECATALOGBOOLRESPONSE_HPP_
