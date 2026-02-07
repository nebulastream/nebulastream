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

#include <memory>
#include <DataTypes/Schema.hpp>

namespace NES
{
/// Remaining functionality of the former "Output-Formatting" classes of the sink. The insertion of a formatted schema string is the only
/// functionality that cannot be moved into the emit phase, because we cannot guarantee that the formatted schema string arrives first at the
/// sink.
/// Class provides an out-of-the-box formatting function but may be customized via subclasses that override the function (add registry then).
class SchemaFormatter
{
public:
    explicit SchemaFormatter(const std::shared_ptr<const Schema>& schema) : schema(schema) { }

    virtual ~SchemaFormatter() = default;

    virtual std::string getFormattedSchema();

protected:
    std::shared_ptr<const Schema> schema;
};
}
