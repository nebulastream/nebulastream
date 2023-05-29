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

#include <Nautilus/Backends/Babelfish/BabelfishCode.hpp>
#include <Nautilus/Backends/Babelfish/BabelfishExecutable.hpp>
#include <Nautilus/IR/Types/FloatStamp.hpp>
#include <Nautilus/IR/Types/IntegerStamp.hpp>
#include <Util/JNI/JNI.hpp>
#include <Util/JNI/JNIUtils.hpp>
#include <Util/Logger/Logger.hpp>
#include <complex>
#include <jni.h>
#include <utility>
namespace NES::Nautilus::Backends::Babelfish {

BabelfishExecutable::BabelfishExecutable(BabelfishCode code, jclass entrypointClass, jobject pipelineObject)
    : code(std::move(code)), entrypointClass(entrypointClass), pipelineObject(pipelineObject) {}

class BabelfishInvocable : public Executable::GenericInvocable {
  public:
    explicit BabelfishInvocable(BabelfishCode& code,
                                jclass entripointClazz,
                                jobject pipelineObject,
                                jmethodID callPipeline,
                                jobjectArray argumentArray)
        : code(code), entripointClazz(entripointClazz), pipelineObject(pipelineObject), callPipeline(callPipeline),
          argumentArray(argumentArray) {}
    std::any invokeGeneric(const std::vector<std::any>& vector) override;

  private:
    BabelfishCode& code;
    jclass entripointClazz;
    jobject pipelineObject;
    jmethodID callPipeline;
    jobjectArray argumentArray;
};

void* BabelfishExecutable::getInvocableFunctionPtr(const std::string&) { return (void*) nullptr; }

bool BabelfishExecutable::hasInvocableFunctionPtr() { return false; }

std::unique_ptr<Executable::GenericInvocable> BabelfishExecutable::getGenericInvocable(const std::string&) {

    auto env = jni::getEnv();
    jmethodID pipelineInvocationMethod;
    if (code.resultType->isVoid()) {
        pipelineInvocationMethod =
            env->GetStaticMethodID(entrypointClass, "callPipelineVoid", "(Lnebulastream/Pipeline;[Ljava/lang/Object;)V");
    } else {
        pipelineInvocationMethod = env->GetStaticMethodID(entrypointClass,
                                                          "callPipeline",
                                                          "(Lnebulastream/Pipeline;[Ljava/lang/Object;)Ljava/lang/Object;");
    }

    auto objectClass = jni::findClass("java/lang/Object");
    auto argArray = env->NewObjectArray(code.argTypes.size(), objectClass, nullptr);
    argArray = (jobjectArray) env->NewGlobalRef((jobject) argArray);

    return std::make_unique<BabelfishInvocable>(code, entrypointClass, pipelineObject, pipelineInvocationMethod, argArray);
}
BabelfishExecutable::~BabelfishExecutable() {
    NES_DEBUG2("~BabelfishExecutable()");
    jni::getEnv()->DeleteGlobalRef(pipelineObject);
}

std::any BabelfishInvocable::invokeGeneric(const std::vector<std::any>& args) {

    NES_ASSERT(args.size() == code.argTypes.size(), "Arguments are not of the correct size");
    auto env = jni::getEnv();
    for (size_t i = 0; i < args.size(); i++) {
        if (auto* value = std::any_cast<int8_t>(&args[i])) {
            env->SetObjectArrayElement(argumentArray, i, jni::createByte(*value));
        } else if (auto* value = std::any_cast<int16_t>(&args[i])) {
            env->SetObjectArrayElement(argumentArray, i, jni::createShort(*value));
        } else if (auto* value = std::any_cast<int32_t>(&args[i])) {
            env->SetObjectArrayElement(argumentArray, i, jni::createInteger(*value));
        } else if (auto* value = std::any_cast<int64_t>(&args[i])) {
            env->SetObjectArrayElement(argumentArray, i, jni::createLong(*value));
        } else if (auto* value = std::any_cast<float>(&args[i])) {
            env->SetObjectArrayElement(argumentArray, i, jni::createFloat(*value));
        } else if (auto* value = std::any_cast<double>(&args[i])) {
            env->SetObjectArrayElement(argumentArray, i, jni::createDouble(*value));
        } else if (auto* value = std::any_cast<void*>(&args[i])) {
            auto val = *(int64_t*) value;
            env->SetObjectArrayElement(argumentArray, i, jni::createLong(val));
        } else {
            NES_NOT_IMPLEMENTED();
        }
    }

    auto result = env->CallStaticObjectMethod(entripointClazz, callPipeline, pipelineObject, argumentArray);
    jni::jniErrorCheck();
    if (code.resultType->isInteger()) {
        auto bitWidth = static_pointer_cast<IR::Types::IntegerStamp>(code.resultType)->getBitWidth();
        switch (bitWidth) {
            case IR::Types::IntegerStamp::BitWidth::I8: return jni::getByteValue(result);
            case IR::Types::IntegerStamp::BitWidth::I16: return jni::getShortValue(result);
            case IR::Types::IntegerStamp::BitWidth::I32: return jni::getIntegerValue(result);
            case IR::Types::IntegerStamp::BitWidth::I64: return jni::getLongValue(result);
        }
    }
    if (code.resultType->isFloat()) {
        auto bitWidth = static_pointer_cast<IR::Types::FloatStamp>(code.resultType)->getBitWidth();
        switch (bitWidth) {
            case IR::Types::FloatStamp::BitWidth::F32: return jni::getFloatValue(result);
            case IR::Types::FloatStamp::BitWidth::F64: return jni::getDoubleValue(result);
        }
    }
    return nullptr;
}

}// namespace NES::Nautilus::Backends::Babelfish