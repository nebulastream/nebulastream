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
#include <Execution/Operators/Relational/JavaUDF/JavaUDFOperatorHandler.hpp>
#include <Execution/Operators/Relational/JavaUDF/JavaUDFUtils.hpp>
#include <Execution/Operators/Relational/JavaUDF/MapJavaUDF.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Util/JNI/JNI.hpp>
#include <Util/JNI/JNIUtils.hpp>
#include <filesystem>
#include <fstream>
#include <jni.h>
#include <utility>

namespace NES::Runtime::Execution::Operators {

/**
 * Execute the java udf
 * @param state operator handler state
 * @param pojoObjectPtr pojo object
 * @return result of the udf
 */
void* executeMapUdf(void* state, void* instance, void* pojoObjectPtr) {
    NES_ASSERT2_FMT(state != nullptr, "op handler context should not be null");
    NES_ASSERT2_FMT(pojoObjectPtr != nullptr, "pojoObjectPtr should not be null");
    auto handler = static_cast<JavaUDFOperatorHandler*>(state);
    // Call udf function
    jobject udfResult = jni::getEnv()->CallObjectMethod((jobject) instance, handler->getUDFMethodId(), pojoObjectPtr);
    jni::jniErrorCheck();
    return udfResult;
}

void* startTimer(void *state) {
    NES_ASSERT2_FMT(state != nullptr, "op handler context should not be null");
    auto handler = static_cast<JavaUDFOperatorHandler*>(state);
    handler->getJniTimer().start();
    return nullptr;
}

void* pauseTimer(void *state) {
    NES_ASSERT2_FMT(state != nullptr, "op handler context should not be null");
    auto handler = static_cast<JavaUDFOperatorHandler*>(state);
    handler->getJniTimer().pause();
    return nullptr;
}

MapJavaUDF::MapJavaUDF(uint64_t operatorHandlerIndex, SchemaPtr operatorInputSchema, SchemaPtr operatorOutputSchema)
    : AbstractJavaUDFOperator(operatorHandlerIndex, std::move(operatorInputSchema), std::move(operatorOutputSchema)) {}

/**
 * Operator execution function
 * @param ctx operator context
 * @param record input record
 */
void MapJavaUDF::execute(ExecutionContext& ctx, Record& record) const {
    auto state = (LocalUDFState*) ctx.getLocalState(this);
    auto handler = state->handler;

    FunctionCall<>("startTimer", startTimer, handler);

    // Convert the input record to the input field of UDF input object
    auto inputPojoPtr = createInputPojo(record, handler);

    // Get output class and call udf
    auto outputPojoPtr = FunctionCall<>("executeMapUdf", executeMapUdf, handler, state->instance, inputPojoPtr);
    FunctionCall<>("freeObject", freeObject, inputPojoPtr);

    auto resultRecord = extractRecordFromPojo(handler, outputPojoPtr);
    FunctionCall<>("freeObject", freeObject, outputPojoPtr);

    FunctionCall<>("pauseTimer", pauseTimer, handler);

    // Trigger execution of next operator
    child->execute(ctx, resultRecord);
}

}// namespace NES::Runtime::Execution::Operators