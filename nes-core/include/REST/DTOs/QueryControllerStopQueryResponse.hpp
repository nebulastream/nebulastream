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

#ifndef NES_NES_CORE_INCLUDE_REST_DTOS_NES_NES_CORE_INCLUDE_REST_DTOS_QUERYCONTROLLERSTOPQUERYRESPONSE_HPP_
#define NES_NES_CORE_INCLUDE_REST_DTOS_NES_NES_CORE_INCLUDE_REST_DTOS_QUERYCONTROLLERSTOPQUERYRESPONSE_HPP_

#include <oatpp/core/data/mapping/type/Object.hpp>
#include <oatpp/core/macro/codegen.hpp>

/* Begin DTO code-generation */
#include OATPP_CODEGEN_BEGIN(DTO)

namespace NES {
namespace REST {
namespace DTO {

/**
 * Message Data-Transfer-Object
 */
class QueryControllerStopQueryResponse : public oatpp::DTO {
    DTO_INIT(QueryControllerStopQueryResponse, DTO /* Extends */)
    DTO_FIELD(Boolean, success);
};

}// namespace DTO
}// namespace REST
}// namespace NES

/* End DTO code-generation */
#include OATPP_CODEGEN_END(DTO)

#endif//NES_NES_CORE_INCLUDE_REST_DTOS_NES_NES_CORE_INCLUDE_REST_DTOS_QUERYCONTROLLERSTOPQUERYRESPONSE_HPP_