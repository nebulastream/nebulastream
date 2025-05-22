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
#include <string>
#include <vector>
#include <Functions/NodeFunction.hpp>
#include <Functions/NodeFunctionConstantValue.hpp>
#include <Functions/NodeFunctionFieldAccess.hpp>
#include <Optimizer/QueryRewrite/BaseRewriteRule.hpp>

namespace NES::Optimizer
{


/**
 * @brief This rule is only used for evaluating efficiency of query merging using string based signature computation. This rule
 * will alphabetically sort the attributes provided in the Filter, Map, and Project operators. It will however, won't change the
 * LHS and RHS of a relational or assignment operator.
 *
 * Example:
 *
 * For map:
 * 1. map("b" = "c"+"a") => map("b" = "a"+"c")
 * 2. map("b" = "c"+"b"+"a") => map("b" = "a"+"b"+"c")
 * 3. map("b" = (((("c"+"b")+"a")+"d")+"e")) => map("b" = "a"+"a"+"b"+"c")
 * 4. map("b" = "d"+"c"+"b"*"a") => map("b" = "a"*"b"+"c"+"d")
 * 5. map("b" = "d"+"b"+"c"*"a") => map("b" = "a"*"c"+"b"+"d")
 * 6. map("b" = "c"+10+"a"+100) => map("b" = 10+100+"a"+"c")
 *
 *
 * For Filter:
 * 1. filter("c" * "b" > "d" + "a") => filter("a" + "d" < "b" * "c")
 * 2. filter("c" * "b" > "d" + "a" and "a" < "b") => filter("a" < "b" and "b" * "c" > "a" + "d")
 */
class AttributeSortRule : public BaseRewriteRule
{
public:
    static std::shared_ptr<AttributeSortRule> create();
    AttributeSortRule() = default;
    virtual ~AttributeSortRule() = default;

    /**
     * @brief Apply Attribute Sort rule on input query plan
     * @param queryPlan: the original query plan
     * @return updated logical query plan
     */
    std::shared_ptr<QueryPlan> apply(std::shared_ptr<QueryPlan> queryPlan) override;

private:
    /**
     * @brief Alphabetically sort the attributes in the operator. This method only expects operators of type filter and map.
     * @param logicalOperator: the operator to be sorted
     * @return pointer to the updated function
     */
    std::shared_ptr<NodeFunction> sortAttributesInFunction(std::shared_ptr<NodeFunction> function);

    /**
     * @brief Alphabetically sort the attributes within the arithmetic function
     * @param function: the input arithmetic function
     * @return pointer to the updated function
     */
    std::shared_ptr<NodeFunction> sortAttributesInArithmeticalFunctions(std::shared_ptr<NodeFunction> function);

    /**
     * @brief Alphabetically sort the attributes within the logical function
     * @param function: the input logical function
     * @return pointer to the updated function
     */
    std::shared_ptr<NodeFunction> sortAttributesInLogicalFunctions(const std::shared_ptr<NodeFunction>& function);

    /**
     * @brief fetch all commutative fields of type field access or constant from the relational or arithmetic function of type
     * FunctionType. The fetched fields are then sorted alphabetically.
     * @param function: the function to be used
     * @return: vector of function containing commutative field access or constant function type
     */
    template <class FunctionType>
    std::vector<std::shared_ptr<NodeFunction>> fetchCommutativeFields(const std::shared_ptr<NodeFunction>& function)
    {
        std::vector<std::shared_ptr<NodeFunction>> commutativeFields;
        if (Util::instanceOf<NodeFunctionFieldAccess>(function) || Util::instanceOf<NodeFunctionConstantValue>(function))
        {
            commutativeFields.push_back(function);
        }
        else if (Util::instanceOf<FunctionType>(function))
        {
            for (const auto& child : function->getChildren())
            {
                auto childCommutativeFields = fetchCommutativeFields<FunctionType>(Util::as<NodeFunction>(child));
                commutativeFields.insert(commutativeFields.end(), childCommutativeFields.begin(), childCommutativeFields.end());
            }
        }
        return commutativeFields;
    }

    /**
     * @brief Replace the original function within parent function with updated function
     * @param parentFunction: the parent function containing original function
     * @param originalFunction: the original function
     * @param updatedFunction: the updated function
     */
    bool replaceCommutativeFunctions(
        const std::shared_ptr<NodeFunction>& parentFunction,
        const std::shared_ptr<NodeFunction>& originalFunction,
        const std::shared_ptr<NodeFunction>& updatedFunction);

    /**
     * @brief Fetch the value of the left most constant function or the name of the left most field access function within
     * the input function. This information is then used for performing global sorting in case of a binary function.
     * @param function: the input function
     * @return the name or value of field or constant function
     */
    static std::string fetchLeftMostConstantValueOrFieldName(std::shared_ptr<NodeFunction> function);
};
}
