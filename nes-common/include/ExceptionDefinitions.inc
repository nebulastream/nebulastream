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

#pragma once

/// NOLINT(clang-diagnostic-error)
#ifndef EXCEPTION
#    error "This file should not be included directly! Include it via <ErrorHandling.hpp>"
#endif

/// 1XXX Configuration Errors
EXCEPTION(InvalidConfigParameter, 1000, "invalid config parameter")
EXCEPTION(MissingConfigParameter, 1001, "missing config parameter")
EXCEPTION(CannotLoadConfig, 1002, "cannot load config")
EXCEPTION(SLTUnexpectedToken, 1003, "unexpected token in sql logic test file")
EXCEPTION(SLTWrongSchema, 1004, "schema is not equal to query result schema")

/// 2XXX Errors during query registration and compilation
EXCEPTION(InvalidQuerySyntax, 2000, "invalid query syntax")
EXCEPTION(CannotSerialize, 2001, "cannot serialize")
EXCEPTION(CannotDeserialize, 2002, "cannot deserialize")
EXCEPTION(CannotInferSchema, 2003, "cannot infer schema")
EXCEPTION(FieldNotFound, 2004, "field not found")
EXCEPTION(FieldAlreadyExists, 2005, "field already exists")
EXCEPTION(DifferentFieldTypeExpected, 2006, "different field type expected")
EXCEPTION(CannotInferStamp, 2007, "cannot infer stamp")

/// 21XX Errors during query compilation
EXCEPTION(UnknownWindowingStrategy, 2100, "unknown windowing strategy")
EXCEPTION(UnknownWindowType, 2101, "unknown window type")
EXCEPTION(UnknownPhysicalType, 2102, "unknown physical type")
EXCEPTION(UnknownJoinStrategy, 2103, "unknown join strategy")
EXCEPTION(UnknownPluginType, 2104, "unknown plugin type")
EXCEPTION(UnknownFunctionType, 2105, "unknown function type")
EXCEPTION(UnknownSinkType, 2106, "unknown sink type")
EXCEPTION(UnknownSourceType, 2107, "unknown source type")
EXCEPTION(UnknownSinkFormat, 2108, "unknown sink format")
EXCEPTION(UnknownSourceFormat, 2109, "unknown source format")
EXCEPTION(UnknownPhysicalOperator, 2110, "unknown physical operator")
EXCEPTION(UnknownLogicalOperator, 2111, "unknown logical operator")
EXCEPTION(UnknownUserDefinedFunctionType, 2112, "unknown user defined function type")
EXCEPTION(UnknownTimeFunctionType, 2113, "unknown time function type")
EXCEPTION(UnknownAggregationType, 2114, "unknown aggregation type")
EXCEPTION(UnknownStatisticsType, 2115, "unknown statistics type")
EXCEPTION(UnknownExpressionArgumentType, 2116, "unknown expression argument type")
EXCEPTION(UnknownWatermarkStrategy, 2117, "unknown watermark strategy")
EXCEPTION(UnknownInvocationType, 2118, "unknown invocation type")
EXCEPTION(UnknownExecutablePipelineProviderType, 2119, "unknown executable pipeline provider type")
EXCEPTION(UnknownCompilationBackendType, 2120, "unknown compilation backend type")
EXCEPTION(InvalidUseOfFunction, 2121, "function is not valid")
EXCEPTION(UnsupportedOperation, 2122, "tried to execute an unsupported operation")
EXCEPTION(UnknownParserType, 2123, "unknown parser type")

/// 22XX NebuLI
EXCEPTION(QueryDescriptionNotReadable, 2200, "could not read query description")
EXCEPTION(QueryDescriptionNotParsable, 2201, "could not parse query description")
EXCEPTION(QueryInvalid, 2202, "query is invalid")
EXCEPTION(LogicalSourceNotFoundInQueryDescription, 2203, "logical source was not found in the query description")
EXCEPTION(PhysicalSourceNotFoundInQueryDescription, 2204, "physical source was not found in the query description")
EXCEPTION(OperatorNotFound, 2205, "operator not found")
EXCEPTION(TypeInferenceException, 2206, "failed to apply type inference")

/// 3XXX Errors during query runtime
EXCEPTION(BufferAllocationFailure, 3000, "buffer allocation failure")
EXCEPTION(CannotStartNodeEngine, 3001, "cannot start node engine")
EXCEPTION(CannotStopNodeEngine, 3002, "cannot stop node engine")
EXCEPTION(CannotStartQueryManager, 3003, "cannot start query manager")
EXCEPTION(CannotStopQueryManager, 3004, "cannot stop query manager")
EXCEPTION(QueryNotRegistered, 3005, "query not registered")
EXCEPTION(CannotStartQuery, 3006, "cannot start query")
/// Todo #500: place holder until we can cleanly handle malformed string values (NULL/Undefined or similar)
/// MUST be used as the central exception to report malformed string values.
EXCEPTION(CannotFormatMalformedStringValue, 3007, "cannot format malformed string value")
EXCEPTION(BufferAccessException, 3008, "buffer allocation failure")
EXCEPTION(QueryRuntimeError, 3009, "general query runtime error")

/// 4XXX Errors interpreting data stream, sources and sinks
EXCEPTION(CannotFormatSourceData, 4000, "cannot format source data")
EXCEPTION(MalformatedTuple, 4001, "malformed tuple")
EXCEPTION(DataIngestionFailure, 4002, "error during data ingestion of source")
EXCEPTION(StopBeforeStartFailure, 4003, "source was stopped before it was started")
EXCEPTION(CannotOpenSource, 4004, "failed to open a source")
EXCEPTION(CSVParsingError, 4005, "error during csv parsing")
EXCEPTION(CannotOpenSink, 4006, "failed to open a sink")

/// 5XXX Network errors
EXCEPTION(CannotConnectToCoordinator, 5000, "cannot connect to coordinator")
EXCEPTION(LostConnectionToCooridnator, 5001, "lost connection to coordinator")

/// 6XXX API error
EXCEPTION(BadApiRequest, 6000, "bad api request")

/// 9XXX Internal errors (e.g. bugs)
EXCEPTION(PreconditionViolated, 9000, "precondition violated")
EXCEPTION(InvariantViolated, 9001, "invariant violated")
EXCEPTION(FunctionNotImplemented, 9002, "function not implemented")
EXCEPTION(DeprecatedFeatureUsed, 9003, "deprecated feature used")
EXCEPTION(CannotAllocateBuffer, 9004, "cannot allocate buffer")
EXCEPTION(InvalidRefCountForBuffer, 9005, "invalid reference counter for buffer")
EXCEPTION(InvalidDynamicCast, 9006, "Invalid dynamic cast")
EXCEPTION(UnknownOperator, 9007, "unknown operator")
EXCEPTION(NotImplemented, 9008, "not implemented")

/// Special errors
EXCEPTION(UnknownException, 9999, "unknown exception")
