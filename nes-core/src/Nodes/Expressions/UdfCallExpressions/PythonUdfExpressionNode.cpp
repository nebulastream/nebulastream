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
#ifdef PYTHON_UDF_ENABLED
#include <Nodes/Expressions/UdfCallExpressions/PythonUdfExpressionNode.hpp>
#include <Catalogs/UDF/UdfCatalog.hpp>

namespace NES::Experimental {

PythonUdfExpressionNode::PythonUdfExpressionNode() {}

std::string PythonUdfExpressionNode::toString() const {
    std::stringstream ss;
    ss << "PYTHON(" << children[0]->toString() << ")";
    return ss.str();
}

ExpressionNodePtr PythonUdfExpressionNode::copy() {
    return std::make_shared<PythonUdfExpressionNode>(PythonUdfExpressionNode(this));
}

}// namespace NES::Experimental
#endif