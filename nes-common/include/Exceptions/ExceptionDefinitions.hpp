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

#ifndef NES_COMMON_INCLUDE_EXCEPTIONS_EXCEPTIONDEFINITIONS_HPP_
#define NES_COMMON_INCLUDE_EXCEPTIONS_EXCEPTIONDEFINITIONS_HPP_

/***
 * NOTE: Do not include this header directly but use:
 * #include <Exceptions/Exception.hpp>
 */

/// 1XXX Configuration Errors
EXCEPTION(InvalidConfigParameter, 1000, "invalid config parameter")
EXCEPTION(MissingConfigParameter, 1001, "missing config parameter")
EXCEPTION(CannotLoadConfig, 1002, "cannot load config")

/// 2XXX Errors during query registration and compilation
EXCEPTION(InvalidQuerySyntax, 2000, "invalid query syntax")
EXCEPTION(UnknownSource, 2001, "unknown source")
EXCEPTION(CannotSerialize, 2002, "cannot serialize")
EXCEPTION(CannotDeserialize, 2003, "cannot deserialize")
EXCEPTION(CannotCompileQuery, 2004, "cannot compile query")
EXCEPTION(CannotInferSchema, 2005, "cannot infer schema")
EXCEPTION(InvalidPhysicalOperator, 2006, "invalid physical operator")

/// 3XXX Errors during query runtime
EXCEPTION(BufferAllocationFailure, 3000, "buffer allocation failure")
EXCEPTION(JavaUDFExcecutionFailure, 3001, "java UDF execution failure")
EXCEPTION(PythonUDFExcecutionFailure, 3002, "python UDF execution failure")
EXCEPTION(CannotStartNodeEngine, 3003, "cannot start node engine")
EXCEPTION(CannotStopNodeEngine, 3004, "cannot stop node engine")

/// 4XXX Errors interpreting data stream, sources and sinks
EXCEPTION(CannotOpenSourceFile, 4000, "cannot open source file")
EXCEPTION(CannotFormatSourceData, 4001, "cannot format source data")
EXCEPTION(MalformatedTuple, 4002, "malformed tuple")

/// 5XXX Network errors
EXCEPTION(CannotConnectToCoordinator, 5000, "cannot connect to coordinator")
EXCEPTION(LostConnectionToCooridnator, 5001, "lost connection to coordinator")

/// 6XXX API error
EXCEPTION(BadApiRequest, 6000, "bad api request")

/// 9XXX Internal errors (e.g. bugs)
EXCEPTION(AssertViolated, 9000, "assert violated")
EXCEPTION(FunctionNotImplemented, 9001, "function not implemented")
EXCEPTION(UnknownWindowingStrategy, 9002, "unknown windowing strategy")
EXCEPTION(UnknownWindowType, 9003, "unknown window type")
EXCEPTION(UnknownPhysicalType, 9004, "unknown physical type")
EXCEPTION(UnknownJoinStrategy, 9005, "unknown join strategy")
EXCEPTION(UnknownPluginType, 9006, "unknown plugin type")
EXCEPTION(DeprecatedFeatureUsed, 9007, "deprecated feature used")

/// Special errors
EXCEPTION(UnknownException, 9999, "unknown exception")

#endif // NES_COMMON_INCLUDE_EXCEPTIONS_EXCEPTIONDEFINITIONS_HPP_
