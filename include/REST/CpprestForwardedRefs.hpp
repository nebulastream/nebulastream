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

#ifndef NES_INCLUDE_REST_CPPRESTFORWARDEDREFS_H_
#define NES_INCLUDE_REST_CPPRESTFORWARDEDREFS_H_

#include <string>
namespace web {

namespace http {
class http_request;
class http_response;
// same as basic_types.h from cpprest
using method = std::string;

namespace experimental {
namespace listener {
class http_listener;
}// namespace listener
}// namespace experimental

}// namespace http

namespace json {
class value;
}// namespace json

}// namespace web

namespace utility {
// same as basic_types.h from cpprest
using string_t = std::string;
}// namespace utility

#endif//NES_INCLUDE_REST_CPPRESTFORWARDEDREFS_H_
