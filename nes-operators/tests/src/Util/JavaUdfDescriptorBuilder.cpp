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
#include <Util/Logger/Logger.hpp>
#include <algorithm>
#include <filesystem>
#include <fstream>

namespace NES::Catalogs::UDF {

JavaUDFDescriptorPtr JavaUDFDescriptorBuilder::build() const {
    return JavaUDFDescriptor::create(className,
                                     methodName,
                                     instance,
                                     byteCodeList,
                                     inputSchema,
                                     outputSchema,
                                     inputClassName,
                                     outputClassName);
}

JavaUDFDescriptorBuilder& JavaUDFDescriptorBuilder::loadByteCodeFrom(std::string_view classFilePath) {
    for (auto& [className, byteCode] : byteCodeList) {
        std::string copy = className;
        std::replace(copy.begin(), copy.end(), '.', '/');
        const auto fileName = std::filesystem::path(classFilePath) / fmt::format("{}.class", copy);
        NES_DEBUG("Loading byte code: class={}, file={}", className, fileName.string());
        std::ifstream classFile(fileName, std::fstream::binary);
        NES_ASSERT(classFile, "Could not find class file: " << fileName);
        classFile.seekg(0, std::ios_base::end);
        auto fileSize = classFile.tellg();
        classFile.seekg(0, std::ios_base::beg);
        byteCode.resize(fileSize);
        classFile.read(byteCode.data(), byteCode.size());
    }
    return *this;
}

JavaUDFDescriptorBuilder& JavaUDFDescriptorBuilder::setClassName(std::string_view newClassName) {
    this->className = newClassName;
    return *this;
}

JavaUDFDescriptorBuilder& JavaUDFDescriptorBuilder::setMethodName(std::string_view newMethodName) {
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

JavaUDFDescriptorBuilder& JavaUDFDescriptorBuilder::setInputClassName(std::string_view newInputClassName) {
    this->inputClassName = newInputClassName;
    return *this;
}

JavaUDFDescriptorBuilder& JavaUDFDescriptorBuilder::setOutputClassName(std::string_view newOutputClassName) {
    this->outputClassName = newOutputClassName;
    return *this;
}

JavaUDFDescriptorPtr JavaUDFDescriptorBuilder::createDefaultJavaUDFDescriptor() { return JavaUDFDescriptorBuilder().build(); }

}// namespace NES::Catalogs::UDF