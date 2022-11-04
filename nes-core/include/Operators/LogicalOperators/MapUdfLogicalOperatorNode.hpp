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

#ifndef NES_MAPUDFLOGICALOPERATORNODE_H
#define NES_MAPUDFLOGICALOPERATORNODE_H

#include <Operators/LogicalOperators/LogicalUnaryOperatorNode.hpp>


namespace NES {

namespace Catalogs::UDF {
class JavaUdfDescriptor;
using JavaUdfDescriptorPtr = std::shared_ptr<JavaUdfDescriptor>;
}

// TODO Documentation
class MapUdfLogicalOperatorNode : public LogicalUnaryOperatorNode {
  public:
    MapUdfLogicalOperatorNode(OperatorId id, const Catalogs::UDF::JavaUdfDescriptorPtr javaUdfDescriptor);

    // TODO Documentation
    Catalogs::UDF::JavaUdfDescriptorPtr getJavaUdfDescriptor() const;

    // TODO Documentation
    bool inferSchema(Optimizer::TypeInferencePhaseContext& typeInferencePhaseContext) override;

    // TODO Documentation
    void inferStringSignature() override;

    // TODO Documentation
    std::string toString() const override;

    // TODO Documentation
    OperatorNodePtr copy() override;

    // TODO Documentation
    [[nodiscard]] bool equal(const NodePtr& other) const override;

    // TODO Documentation
    [[nodiscard]] bool isIdentical(const NodePtr& other) const override;

  private:
    const Catalogs::UDF::JavaUdfDescriptorPtr javaUdfDescriptor;
};

}

#endif//NES_MAPUDFLOGICALOPERATORNODE_H
