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

#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Relational/JavaUDF/AbstractJavaUDFOperator.hpp>
#include <Execution/Operators/Relational/JavaUDF/FlatMapJavaUDF.hpp>
#include <Execution/Operators/Relational/JavaUDF/JavaUDFOperatorHandler.hpp>
#include <Execution/Operators/Relational/JavaUDF/JavaUDFUtils.hpp>
#include <Nautilus/Interface/DataTypes/Text/Text.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Util/JNI/JNI.hpp>
#include <Util/JNI/JNIUtils.hpp>
#include <filesystem>
#include <fstream>
#include <jni.h>
#include <utility>
#if not(defined(__APPLE__))
#include <experimental/source_location>
#endif

namespace NES::Runtime::Execution::Operators {

void* executeFlatMapUDF(void* state, void* instance, void* pojoObjectPtr) {
    NES_ASSERT2_FMT(state != nullptr, "op handler context should not be null");
    NES_ASSERT2_FMT(pojoObjectPtr != nullptr, "pojoObjectPtr should not be null");
    auto handler = static_cast<JavaUDFOperatorHandler*>(state);
    // Call udf function
    jobject udfResult = jni::getEnv()->CallObjectMethod((jobject) instance, handler->getUDFMethodId(), pojoObjectPtr);
    jni::jniErrorCheck();
    return udfResult;
}

bool hasNext(void*, void* instance) {
    auto env = jni::getEnv();
    jclass iteratorInterface = env->FindClass("java/util/Iterator");
    jmethodID hasNextMethod = env->GetMethodID(iteratorInterface, "hasNext", "()Z");
    // TODO check for exception
    return env->CallBooleanMethod((jobject) instance, hasNextMethod);
}

void* next(void*, void* instance) {
    auto env = jni::getEnv();
    jclass iteratorInterface = env->FindClass("java/util/Iterator");
    jmethodID nextMethod = env->GetMethodID(iteratorInterface, "next", "()Ljava/lang/Object;");
    jobject element = env->CallObjectMethod((jobject) instance, nextMethod);
    return element;
}

void* getIteratorFromCollection(void*, void* collection) {
    auto env = jni::getEnv();
    jclass collectionInterface = env->FindClass("java/util/Collection");
    jmethodID iteratorMethod = env->GetMethodID(collectionInterface, "iterator", "()Ljava/util/Iterator;");
    jobject iterator = env->CallObjectMethod((jobject) collection, iteratorMethod);
    // TODO check for exception
    return iterator;
}

FlatMapJavaUDF::FlatMapJavaUDF(uint64_t operatorHandlerIndex,
                               NES::SchemaPtr operatorInputSchema,
                               NES::SchemaPtr operatorOutputSchema)
    : AbstractJavaUDFOperator(operatorHandlerIndex, std::move(operatorInputSchema), std::move(operatorOutputSchema)){};

void FlatMapJavaUDF::execute(ExecutionContext& ctx, Record& record) const {
    auto state = (LocalUDFState*) ctx.getLocalState(this);
    auto handler = state->handler;

    // create variables for input pojo ptr java input class
    auto inputPojoPtr = createInputPojo(record, handler);

    // Call udf and get the result value.
    // For flatmap we assume that they return a collection of values.
    auto outputPojoPtr = FunctionCall<>("executeFlatMapUDF", executeFlatMapUDF, handler, state->instance, inputPojoPtr);
    FunctionCall<>("freeObject", freeObject, inputPojoPtr);

    // get the iterator of the collection

    auto iterator = FunctionCall("getIteratorFromCollection", getIteratorFromCollection, handler, outputPojoPtr);

    // iterate over all elements in iterator.
    while (FunctionCall("hasNext", hasNext, handler, iterator)) {
        auto element = FunctionCall("next", next, handler, iterator);
        auto resultRecord = extractRecordFromPojo(handler, element);
        FunctionCall<>("freeObject", freeObject, element);
        // Trigger execution of next operator
        child->execute(ctx, resultRecord);
    }

    FunctionCall<>("freeObject", freeObject, outputPojoPtr);
}

}// namespace NES::Runtime::Execution::Operators