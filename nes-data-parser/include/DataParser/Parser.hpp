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

#ifndef NES_DATA_PARSER_INCLUDE_DATAPARSER_PARSER_HPP_
#define NES_DATA_PARSER_INCLUDE_DATAPARSER_PARSER_HPP_
#include <string_view>
#include <vector>
namespace NES::Runtime {
class TupleBuffer;
}
namespace NES::DataParser {

class Parser {
  public:
    virtual ~Parser() = default;
    /**
     * Parses *all* tuples of the std::string_view and writes them into the TupleBuffer.
     * The function only terminates, if the string_view is empty, no addtional progress can be made, or the TupleBuffer was filled
     * @param data input data
     * @return the rest of the string_view which was not processed
     */
    [[nodiscard]] virtual std::string_view parseIntoBuffer(std::string_view data, NES::Runtime::TupleBuffer&) const = 0;

    /**
    * `getFieldOffset` and `parse` are intended for Nautilus based operators.
    * getFieldOffset returns a vector of type-erased pointers to an internal tuple. Assuming the user knows the schema,
    * the user is able to static_cast the type-erased pointers back to the actual values.
    * @return vector of pointers to the fields of the internal tuple
    */
    [[nodiscard]] virtual std::vector<void*> getFieldOffsets() const = 0;

    /**
     * Parses a single tuple of the std::string_view according to the internal schema and format.
     * The individual fields can be accessed by the pointers returned by the `getFieldOffsets`.
     * @return the rest of the string_view. This means either the string_view is reduced by a single tuple or not changed at all if
     * the parser could not make progress.
     */
    [[nodiscard]] virtual std::string_view parse(std::string_view) const = 0;
};
}// namespace NES::DataParser
#endif// NES_DATA_PARSER_INCLUDE_DATAPARSER_PARSER_HPP_
