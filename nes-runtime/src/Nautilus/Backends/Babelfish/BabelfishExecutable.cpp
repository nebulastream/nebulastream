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

#include "Nautilus/Backends/Babelfish/BabelfishCode.hpp"
#include "Nautilus/IR/Types/FloatStamp.hpp"
#include <Execution/Operators/Relational/JavaUDF/JavaUDFUtils.hpp>
#include <Nautilus/Backends/Babelfish/BabelfishExecutable.hpp>
#include <Nautilus/IR/Types/IntegerStamp.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <cstdint>
#include <utility>
namespace NES::Nautilus::Backends::Babelfish {

BabelfishExecutable::BabelfishExecutable(BabelfishCode code, JNIEnv* env, jclass entripointClazz, jobject pipelineObject)
    : code(code), env(env), entripointClazz(entripointClazz), pipelineObject(pipelineObject) {}

class BabelfishInvocable : public Executable::GenericInvocable {
  public:
    explicit BabelfishInvocable(BabelfishCode& code,
                                JNIEnv* env,
                                jclass entripointClazz,
                                jobject pipelineObject,
                                jmethodID callPipeline,
                                jobjectArray argumentArray)
        : code(code), env(env), entripointClazz(entripointClazz), pipelineObject(pipelineObject), callPipeline(callPipeline),
          argumentArray(argumentArray) {}
    std::any invokeGeneric(const std::vector<std::any>& vector) override;

  private:
    BabelfishCode& code;
    JNIEnv* env;
    jclass entripointClazz;
    jobject pipelineObject;
    jmethodID callPipeline;
    jobjectArray argumentArray;
};

void* BabelfishExecutable::getInvocableFunctionPtr(const std::string&) { return (void*) nullptr; }

bool BabelfishExecutable::hasInvocableFunctionPtr() { return false; }

std::unique_ptr<Executable::GenericInvocable> BabelfishExecutable::getGenericInvocable(const std::string&) {

    jmethodID callPipeline;
    if (code.resultType->isVoid()) {
        callPipeline =
            env->GetStaticMethodID(entripointClazz, "callPipelineVoid", "(Lnebulastream/Pipeline;[Ljava/lang/Object;)V");
    } else {
        callPipeline = env->GetStaticMethodID(entripointClazz,
                                              "callPipeline",
                                              "(Lnebulastream/Pipeline;[Ljava/lang/Object;)Ljava/lang/Object;");
    }

    jclass objectClass = env->FindClass("java/lang/Object");
    if (objectClass == nullptr) {
        throw std::runtime_error("ERROR: java/lang/Object class not found !");
    }

    jclass cls = env->FindClass("java/lang/Integer");
    jmethodID midInit = env->GetMethodID(cls, "<init>", "(I)V");
    if (NULL == midInit)
        return NULL;

    auto argzn = env->NewObjectArray(code.argTypes.size(), objectClass, nullptr);

    return std::make_unique<BabelfishInvocable>(code, env, entripointClazz, pipelineObject, callPipeline, argzn);
}
BabelfishExecutable::~BabelfishExecutable() {
    NES_DEBUG("~BabelfishExecutable()");
    env->DeleteLocalRef(pipelineObject);
}

std::any BabelfishInvocable::invokeGeneric(const std::vector<std::any>& args) {

    NES_ASSERT(args.size() == code.argTypes.size(), "Arguments are not of the correct size");

    for (size_t i = 0; i < args.size(); i++) {
        if (auto* value = std::any_cast<int8_t>(&args[i])) {
            env->SetObjectArrayElement(argumentArray, i, Runtime::Execution::Operators::createByte(env, *value));
        } else if (auto* value = std::any_cast<int16_t>(&args[i])) {
            env->SetObjectArrayElement(argumentArray, i, Runtime::Execution::Operators::createShort(env, *value));
        } else if (auto* value = std::any_cast<int32_t>(&args[i])) {
            env->SetObjectArrayElement(argumentArray, i, Runtime::Execution::Operators::createInteger(env, *value));
        } else if (auto* value = std::any_cast<int64_t>(&args[i])) {
            env->SetObjectArrayElement(argumentArray, i, Runtime::Execution::Operators::createLong(env, *value));
        } else if (auto* value = std::any_cast<float>(&args[i])) {
            env->SetObjectArrayElement(argumentArray, i, Runtime::Execution::Operators::createFloat(env, *value));
        } else if (auto* value = std::any_cast<double>(&args[i])) {
            env->SetObjectArrayElement(argumentArray, i, Runtime::Execution::Operators::createDouble(env, *value));
        } else if (auto* value = std::any_cast<void*>(&args[i])) {
            auto val = *(int64_t*) value;
            env->SetObjectArrayElement(argumentArray, i, Runtime::Execution::Operators::createLong(env, val));
        } else {
            NES_NOT_IMPLEMENTED();
        }
    }

    NES_DEBUG("Execute Pipeline");
    auto result = env->CallStaticObjectMethod(entripointClazz, callPipeline, pipelineObject, argumentArray);
    Runtime::Execution::Operators::jniErrorCheck(env, __func__, __LINE__);
    if (code.resultType->isInteger()) {
        auto bitWidth = static_pointer_cast<IR::Types::IntegerStamp>(code.resultType)->getBitWidth();
        switch (bitWidth) {
            case IR::Types::IntegerStamp::BitWidth::I8: return Runtime::Execution::Operators::getByteValue(env, result);
            case IR::Types::IntegerStamp::BitWidth::I16: return Runtime::Execution::Operators::getShortValue(env, result);
            case IR::Types::IntegerStamp::BitWidth::I32: return Runtime::Execution::Operators::getIntegerValue(env, result);
            case IR::Types::IntegerStamp::BitWidth::I64: return Runtime::Execution::Operators::getLongValue(env, result);
        }
    }
    if (code.resultType->isFloat()) {
        auto bitWidth = static_pointer_cast<IR::Types::FloatStamp>(code.resultType)->getBitWidth();
        switch (bitWidth) {
            case IR::Types::FloatStamp::BitWidth::F32: return Runtime::Execution::Operators::getFloatValue(env, result);
            case IR::Types::FloatStamp::BitWidth::F64: return Runtime::Execution::Operators::getDoubleValue(env, result);
        }
    }
    return nullptr;
}

}// namespace NES::Nautilus::Backends::Babelfish