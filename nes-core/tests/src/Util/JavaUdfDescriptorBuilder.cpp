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

#include <Util/JavaUdfDescriptorBuilder.hpp>

namespace NES::Catalogs::UDF {

    JavaUdfDescriptorPtr JavaUdfDescriptorBuilder::build() {
        return JavaUdfDescriptor::create(className,
                                         methodName,
                                         instance,
                                         byteCodeList,
                                         outputSchema,
                                         inputClassName,
                                         outputClassName);
    }

    JavaUdfDescriptorBuilder& JavaUdfDescriptorBuilder::setClassName(const std::string& newClassName) {
        this->className = newClassName;
        return *this;
    }

    JavaUdfDescriptorBuilder& JavaUdfDescriptorBuilder::setMethodName(const std::string& newMethodName) {
        this->methodName = newMethodName;
        return *this;
    }

    JavaUdfDescriptorBuilder& JavaUdfDescriptorBuilder::setInstance(const JavaSerializedInstance& newInstance) {
        this->instance = newInstance;
        return *this;
    }

    JavaUdfDescriptorBuilder& JavaUdfDescriptorBuilder::setByteCodeList(const JavaUdfByteCodeList& newByteCodeList) {
        this->byteCodeList = newByteCodeList;
        return *this;
    }

    JavaUdfDescriptorBuilder& JavaUdfDescriptorBuilder::setOutputSchema(const SchemaPtr& newOutputSchema) {
        this->outputSchema = newOutputSchema;
        return *this;
    }

    JavaUdfDescriptorBuilder& JavaUdfDescriptorBuilder::setInputClassName(const std::string newInputClassName) {
        this->inputClassName = newInputClassName;
        return *this;
    }

    JavaUdfDescriptorBuilder& JavaUdfDescriptorBuilder::setOutputClassName(const std::string newOutputClassName) {
        this->outputClassName = newOutputClassName;
        return *this;
    }

    JavaUdfDescriptorPtr JavaUdfDescriptorBuilder::createDefaultJavaUdfDescriptor() { return JavaUdfDescriptorBuilder().build(); }

} // namespace NES::Catalogs::UDF