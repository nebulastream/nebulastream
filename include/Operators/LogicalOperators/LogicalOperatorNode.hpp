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

#ifndef LOGICAL_OPERATOR_NODE_HPP
#define LOGICAL_OPERATOR_NODE_HPP

#include <API/ParameterTypes.hpp>
#include <Operators/LogicalOperators/LogicalOperatorFactory.hpp>
#include <Operators/LogicalOperators/LogicalOperatorForwardRefs.hpp>
#include <Operators/OperatorNode.hpp>

namespace z3 {
class expr;
typedef std::shared_ptr<expr> ExprPtr;
class context;
typedef std::shared_ptr<context> ContextPtr;
}// namespace z3

namespace NES::Optimizer {
class QuerySignature;
typedef std::shared_ptr<QuerySignature> QuerySignaturePtr;
}// namespace NES::Optimizer

namespace NES {

/**
 * @brief Logical operator, enables schema inference and signature computation.
 */
class LogicalOperatorNode : public virtual OperatorNode {

  public:
    LogicalOperatorNode(OperatorId id);

    /**
     * @brief Get the First Order Logic formula representation by the Z3 expression
     * @param context: the shared pointer to the z3::context
     */
    void inferZ3Signature(z3::ContextPtr context);

    /**
     * @brief Set the Z3 signature for the logical operator
     * @param signature : the signature
     */
    void setZ3Signature(Optimizer::QuerySignaturePtr signature);

    /**
     * @brief Get the String based signature for the operator
     */
    virtual void inferStringSignature() = 0;

    /**
     * @brief Set the string signature for the logical operator
     * @param signature : the signature
     */
    void setStringSignature(std::string signature);

    /**
     * @brief Get the Z3 expression for the logical operator
     * @return reference to the Z3 expression
     */
    Optimizer::QuerySignaturePtr getZ3Signature();

    /**
     * @brief Get the string signature computed based on upstream operator chain
     * @return string representing the query signature
     */
    std::string getStringSignature();

    /**
     * @brief Add a new property string to the stored properties map
     * @param key key of the new property
     * @param value value of the new property
     */
    void addProperty(std::string key, std::string value);

    /**
     * @brief Get a the value of a property
     * @param key key of the value to retrieve
     * @return value of the property with the given key
     */
    std::string getProperty(std::string key);

    /**
     * @brief Remove a property string from the stored properties map
     * @param key key of the property to remove
     */
    void removeProperty(std::string key);

    virtual bool inferSchema() = 0;

  protected:
    Optimizer::QuerySignaturePtr z3Signature;
    std::string stringSignature;
    std::map<std::string, std::string> properties;
};

}// namespace NES

#endif// LOGICAL_OPERATOR_NODE_HPP
