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

#ifndef NES_COORDINATOR_INCLUDE_REST_DTOS_ERRORRESPONSE_HPP_
#define NES_COORDINATOR_INCLUDE_REST_DTOS_ERRORRESPONSE_HPP_

#include <oatpp/core/Types.hpp>
#include <oatpp/core/macro/codegen.hpp>

#include OATPP_CODEGEN_BEGIN(DTO)

namespace NES {
namespace REST {
namespace DTO {

class ErrorResponse : public oatpp::DTO {

    DTO_INIT(ErrorResponse, DTO)

    DTO_FIELD_INFO(status) { info->description = "Short status text"; }
    DTO_FIELD(String, status);

    DTO_FIELD_INFO(code) { info->description = "Status code"; }
    DTO_FIELD(Int32, code);

    DTO_FIELD_INFO(message) { info->description = "Verbose message"; }
    DTO_FIELD(String, message);
};

}// namespace DTO
}// namespace REST
}// namespace NES

/* End DTO code-generation */
#include OATPP_CODEGEN_END(DTO)
#endif// NES_COORDINATOR_INCLUDE_REST_DTOS_ERRORRESPONSE_HPP_
