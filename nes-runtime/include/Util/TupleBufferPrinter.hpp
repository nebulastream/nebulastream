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

#ifndef NES_NES_RUNTIME_INCLUDE_UTIL_TUPLEBUFFERPRINTER_HPP_
#define NES_NES_RUNTIME_INCLUDE_UTIL_TUPLEBUFFERPRINTER_HPP_
namespace NES {
namespace Util {
/**
* @brief Outputs a tuple buffer in text format
* @param buffer the tuple buffer
* @return string of tuple buffer
*/
std::string printTupleBufferAsText(Runtime::TupleBuffer& buffer);

/**
* @brief this method creates a string from the content of a tuple buffer
* @return string of the buffer content
*/
std::string printTupleBufferAsCSV(Runtime::TupleBuffer tbuffer, const SchemaPtr& schema);

}
}
#endif//NES_NES_RUNTIME_INCLUDE_UTIL_TUPLEBUFFERPRINTER_HPP_
