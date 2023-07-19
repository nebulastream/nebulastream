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

#ifdef NAUTILUS_PYTHON_UDF_ENABLED
#ifndef NES_PYTHONUDFLOGICALOPERATOR_HPP
#define NES_PYTHONUDFLOGICALOPERATOR_HPP

#include <Operators/LogicalOperators/UDFLogicalOperator.hpp>
#include <Operators/OperatorForwardDeclaration.hpp>

namespace NES {
namespace Catalogs::UDF {
class PythonUDFDescriptor;
using PythonUdfDescriptorPtr = std::shared_ptr<PythonUDFDescriptor>;
}// namespace Catalogs::UDF

/**
 * Logical operator node for a Python udf. This class acts as a parent class for any Python udf logical operator node
 */
class PythonUDFLogicalOperator : public UDFLogicalOperator {

  public:
    /**
     * Construct a PythonUDFLogicalOperator.
     * @param descriptor The descriptor of the Python UDF used in the map operation.
     * @param id The ID of the operator.
     */
    PythonUDFLogicalOperator(const Catalogs::UDF::PythonUdfDescriptorPtr& pythonUDFDescriptor,
                             OperatorId id);

    /**
     * @see LogicalOperatorNode#inferStringSignature
     */
    void inferStringSignature() override;

    /**
     * Getter for the Python UDF descriptor.
     * @return The descriptor of the Python UDF used in the map operation.
     */
    Catalogs::UDF::PythonUDFDescriptorPtr getPythonUDFDescriptor() const;

    /**
     * @see OperatorNode#copy
     */
    virtual OperatorNodePtr copy() override = 0;

    /**
* Two MapUdfLogicalOperatorNode are equal when the wrapped PythonUDFDescriptor are equal.
*/
    [[nodiscard]] virtual bool equal(const NodePtr& other) const override;

    /**
     * @see Node#isIdentical
     */
    [[nodiscard]] bool isIdentical(const NodePtr& other) const override;
  private:
    const Catalogs::UDF::PythonUdfDescriptorPtr pythonUDFDescriptor;
};

}// namespace NES

#endif//NES_PYTHONUDFLOGICALOPERATOR_HPP
#endif// NAUTILUS_PYTHON_UDF_ENABLED