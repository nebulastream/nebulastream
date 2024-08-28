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
#include <SerializableOperator.pb.h>
#include <Operators/LogicalOperators/LogicalOperator.hpp>


#include <unordered_map>

namespace NES::Operators
{

class OperatorRegistry
{
    using creatorFunction = std::function<std::unique_ptr<LogicalOperator>(const SerializableOperator& serializableOperator)>;
public:
    ~OperatorRegistry() = default;

    template<bool update = false>
    void registerPlugin(const std::string& name, const creatorFunction& creator)
    {
        if (!update && registry.contains(name))
        {
            std::cerr << "Plugin '" << name << "', but it already exists.\n";
            return;
        }
        registry[name] = creator;
    }
private:
    OperatorRegistry();
    std::unordered_map<std::string, creatorFunction> registry;
};

}