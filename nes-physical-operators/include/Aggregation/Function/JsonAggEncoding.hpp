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

#pragma once

#include <string>
#include <string_view>

namespace NES
{

/// The text JSON_AGG writes, kept apart from the Nautilus code so it can be tested on its own. The array is compact,
/// `["a","b"]`; a string is RFC 8259 escaped: `"` and `\` get a backslash, the control characters below 0x20 become
/// `\b \f \n \r \t` or `\u00XX`, and every other byte, UTF-8 included, is copied as it is.
class JsonArrayBuilder
{
public:
    JsonArrayBuilder() : text("[") { }

    void append(std::string_view value);

    /// Closes the array, once; the builder must not be appended to afterwards.
    [[nodiscard]] const std::string& finish();

private:
    std::string text;
    bool empty = true;
    bool finished = false;
};

}
