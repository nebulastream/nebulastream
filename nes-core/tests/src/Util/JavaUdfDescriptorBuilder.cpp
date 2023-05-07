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

#include <Util/JavaUDFDescriptorBuilder.hpp>

namespace NES::Catalogs::UDF {

JavaUDFDescriptorPtr JavaUDFDescriptorBuilder::build() {
    return JavaUDFDescriptor::create(className,
                                     methodName,
                                     instance,
                                     byteCodeList,
                                     inputSchema,
                                     outputSchema,
                                     inputClassName,
                                     outputClassName);
}

JavaUDFDescriptorBuilder& JavaUDFDescriptorBuilder::setClassName(const std::string& newClassName) {
    this->className = newClassName;
    return *this;
}

JavaUDFDescriptorBuilder& JavaUDFDescriptorBuilder::setMethodName(const std::string& newMethodName) {
    this->methodName = newMethodName;
    return *this;
}

JavaUDFDescriptorBuilder& JavaUDFDescriptorBuilder::setInstance(const jni::JavaSerializedInstance& newInstance) {
    this->instance = newInstance;
    return *this;
}

JavaUDFDescriptorBuilder& JavaUDFDescriptorBuilder::setByteCodeList(const jni::JavaUDFByteCodeList& newByteCodeList) {
    this->byteCodeList = newByteCodeList;
    return *this;
}

JavaUDFDescriptorBuilder& JavaUDFDescriptorBuilder::setOutputSchema(const SchemaPtr& newOutputSchema) {
    this->outputSchema = newOutputSchema;
    return *this;
}

JavaUDFDescriptorBuilder& JavaUDFDescriptorBuilder::setInputClassName(const std::string newInputClassName) {
    this->inputClassName = newInputClassName;
    return *this;
}

JavaUDFDescriptorBuilder& JavaUDFDescriptorBuilder::setOutputClassName(const std::string newOutputClassName) {
    this->outputClassName = newOutputClassName;
    return *this;
}

JavaUDFDescriptorPtr JavaUDFDescriptorBuilder::createDefaultJavaUDFDescriptor() { return JavaUDFDescriptorBuilder().build(); }

}// namespace NES::Catalogs::UDF