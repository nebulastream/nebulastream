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
#include <API/Expressions/Expressions.hpp>
#include <Expressions/ExpressionNode.hpp>

namespace NES
{

class ExpressionItem;
using ExpressionNodePtr = std::shared_ptr<NES::ExpressionNode>;

/**
 * @brief Defines a UDF call operation.
**/
ExpressionNodePtr CALL(const NES::ExpressionItem& udfName, std::vector<ExpressionNodePtr> functionArgs);

} /// namespace NES
