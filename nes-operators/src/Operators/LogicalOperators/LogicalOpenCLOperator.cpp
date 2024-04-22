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

#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Operators/LogicalOperators/UDFs/JavaUDFDescriptor.hpp>
#include <Operators/LogicalOperators/LogicalOpenCLOperator.hpp>

namespace NES {

LogicalOpenCLOperator::LogicalOpenCLOperator(Catalogs::UDF::JavaUdfDescriptorPtr javaUDFDescriptor, OperatorId id)
    : Operator(id), UDFLogicalOperator(javaUDFDescriptor, id) {}

std::string LogicalOpenCLOperator::toString() const {
    auto javaUDFDescriptor = Catalogs::UDF::UDFDescriptor::as<Catalogs::UDF::JavaUDFDescriptor>(getUDFDescriptor());
    return "OPENCL_LOGICAL_OPERATOR(" + javaUDFDescriptor->getClassName() + "." + javaUDFDescriptor->getMethodName()
        + "; openCLCode : " + openCLCode + " )";
}

OperatorPtr LogicalOpenCLOperator::copy() {
    auto javaUDFDescriptor = Catalogs::UDF::UDFDescriptor::as<Catalogs::UDF::JavaUDFDescriptor>(getUDFDescriptor());
    auto copy = std::make_shared<LogicalOpenCLOperator>(javaUDFDescriptor, id);
    copy->setInputOriginIds(inputOriginIds);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    copy->setHashBasedSignature(hashBasedSignature);
    copy->setZ3Signature(z3Signature);
    copy->setOperatorState(operatorState);
    copy->setStatisticId(statisticId);
    for (const auto& [key, value] : properties) {
        copy->addProperty(key, value);
    }
    return copy;
}

bool LogicalOpenCLOperator::equal(const NodePtr& other) const {
    return other->instanceOf<LogicalOpenCLOperator>()
        && *getUDFDescriptor() == *other->as<LogicalOpenCLOperator>()->getUDFDescriptor();
}

bool LogicalOpenCLOperator::isIdentical(const NodePtr& other) const {
    return equal(other) && id == other->as<LogicalOpenCLOperator>()->id;
}

const std::string& LogicalOpenCLOperator::getOpenClCode() const { return openCLCode; }

void LogicalOpenCLOperator::setOpenClCode(const std::string& openClCode) { openCLCode = openClCode; }

size_t LogicalOpenCLOperator::getDeviceId() const { return deviceId; }

void LogicalOpenCLOperator::setDeviceId(const size_t deviceId) { LogicalOpenCLOperator::deviceId = deviceId; }

Catalogs::UDF::JavaUDFDescriptorPtr LogicalOpenCLOperator::getJavaUDFDescriptor() const {
    return Catalogs::UDF::UDFDescriptor::as<Catalogs::UDF::JavaUDFDescriptor>(udfDescriptor);
}

}// namespace NES
