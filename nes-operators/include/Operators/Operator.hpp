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

#include <any>
#include <unordered_map>
#include <Identifiers/Identifiers.hpp>
#include <Nodes/Node.hpp>

namespace NES
{

class Schema;
using SchemaPtr = std::shared_ptr<Schema>;

class Operator;
using OperatorProperties = std::unordered_map<std::string, std::any>;


/// Returns the next free operator id
OperatorId getNextOperatorId();

class Operator : public Node
{
public:
    explicit Operator(OperatorId id);

    ~Operator() noexcept override = default;


    OperatorId getId() const;

    /// this method is only called from Logical Plan Expansion Rule
    void setId(OperatorId id);

    /// Create duplicate of this operator by copying its context information and also its parent and child operator set.
    std::shared_ptr<Operator> duplicate();

    virtual std::shared_ptr<Operator> copy() = 0;

    bool hasMultipleChildrenOrParents() const;

    bool hasMultipleChildren() const;

    bool hasMultipleParents() const;

    bool addChild(NodePtr newNode) override;

    bool addParent(NodePtr newNode) override;

    NodePtr getChildWithOperatorId(OperatorId operatorId) const;

    bool containAsGrandChild(NodePtr operatorNode) override;

    bool containAsGrandParent(NodePtr operatorNode) override;

    virtual SchemaPtr getOutputSchema() const = 0;

    virtual void setOutputSchema(SchemaPtr outputSchema) = 0;

    void addProperty(const std::string& key, const std::any value);

    void addAllProperties(const OperatorProperties& properties);

    std::any getProperty(const std::string& key);

    void removeProperty(const std::string& key);

    bool hasProperty(const std::string& key) const;

    virtual std::vector<OriginId> getOutputOriginIds() const = 0;

protected:
    std::shared_ptr<Operator> getDuplicateOfParent(const std::shared_ptr<Operator>& operatorNode);

    std::shared_ptr<Operator> getDuplicateOfChild(const std::shared_ptr<Operator>& operatorNode);

    std::string toString() const override;

    OperatorId id;

    OperatorProperties properties;
};

}
