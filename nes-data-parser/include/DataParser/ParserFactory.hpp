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

#ifndef NES_DATA_PARSER_INCLUDE_DATAPARSER_PARSERFACTORY_HPP_
#define NES_DATA_PARSER_INCLUDE_DATAPARSER_PARSERFACTORY_HPP_

#include <Compiler/JITCompiler.hpp>
#include <DataParser/Parser.hpp>
#include <Runtime/FormatType.hpp>
#include <future>
#include <memory>

namespace NES {
class Schema;
using SchemaPtr = std::shared_ptr<Schema>;
}// namespace NES
namespace NES::Runtime {
class TupleBuffer;
}

namespace NES::DataParser {

class ParserFactory {
  public:
    std::future<std::unique_ptr<Parser>> createParser(const Schema& schema, FormatTypes format) const;
    explicit ParserFactory(std::shared_ptr<Compiler::JITCompiler> jit_compiler) : jitCompiler(std::move(jit_compiler)) {}

  private:
    std::shared_ptr<Compiler::JITCompiler> jitCompiler;
};
}// namespace NES::DataParser

#endif// NES_DATA_PARSER_INCLUDE_DATAPARSER_PARSERFACTORY_HPP_
