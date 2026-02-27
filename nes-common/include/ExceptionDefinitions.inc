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

/// NOTE: we have to omitt the pragma once to enable macro triggering mutliple times

/// NOLINT(clang-diagnostic-error)
#ifndef EXCEPTION
#    error "This file should not be included directly! Include it via <ErrorHandling.hpp>"
#endif

/// 1XXX Configuration Errors
EXCEPTION(InvalidConfigParameter, 1000, "invalid config parameter")
EXCEPTION(CannotLoadConfig, 1001, "cannot load config")
EXCEPTION(SLTUnexpectedToken, 1002, "unexpected token in sql logic test file")
EXCEPTION(SLTWrongSchema, 1003, "schema is not equal to query result schema")
EXCEPTION(InvalidUUID, 1004, "invalid UUID")

/// 2XXX Errors during query registration, optimization, and compilation
EXCEPTION(InvalidQuerySyntax, 2000, "invalid query syntax")
EXCEPTION(CannotSerialize, 2001, "cannot serialize")
EXCEPTION(CannotDeserialize, 2002, "cannot deserialize")
EXCEPTION(CannotInferSchema, 2003, "cannot infer schema")
EXCEPTION(FieldNotFound, 2004, "field not found")
EXCEPTION(FieldAlreadyExists, 2005, "field already exists")
EXCEPTION(DifferentFieldTypeExpected, 2006, "different field type expected")
EXCEPTION(CannotInferStamp, 2007, "cannot infer stamp")
EXCEPTION(UnsupportedQuery, 2008, "specified query is (currently) not supported")
EXCEPTION(InvalidIdentifier, 2009, "invalid identifier")
EXCEPTION(InvalidLiteral, 2010, "invalid literal")
EXCEPTION(UnknownWindowType, 2011, "unknown window type")
EXCEPTION(UnknownPhysicalType, 2012, "unknown physical type")
EXCEPTION(UnknownJoinStrategy, 2013, "unknown join strategy")
EXCEPTION(UnknownPluginType, 2014, "unknown plugin type")
EXCEPTION(UnknownFunctionType, 2015, "unknown function type")
EXCEPTION(UnknownSinkType, 2016, "unknown sink type")
EXCEPTION(UnknownSourceType, 2017, "unknown source type")
EXCEPTION(UnknownSinkFormat, 2018, "unknown sink format")
EXCEPTION(UnknownAggregationType, 2019, "unknown aggregation type")
EXCEPTION(UnknownOperation, 2020, "tried to execute an unsupported operation")
EXCEPTION(UnknownParserType, 2021, "unknown parser type")
EXCEPTION(UnknownOptimizerRule, 2022, "unknown optimizer rule")
EXCEPTION(UnknownDataType, 2023, "unknown data type")
EXCEPTION(QueryDescriptionNotReadable, 2024, "could not read query description")
EXCEPTION(QueryDescriptionNotParsable, 2025, "could not parse query description")
EXCEPTION(QueryInvalid, 2026, "query is invalid")
EXCEPTION(LogicalSourceNotFoundInQueryDescription, 2027, "logical source was not found in the query description")
EXCEPTION(TypeInferenceException, 2028, "failed to apply type inference")
EXCEPTION(InvalidStatement, 2029, "invalid statement")
EXCEPTION(UnknownSourceName, 2030, "unknown source name")
EXCEPTION(SourceAlreadyExists, 2031, "source already exists")
EXCEPTION(QueryCompilerError, 2032, "query compiler / backend throwed an error")
EXCEPTION(UnknownSinkName, 2033, "unknown sink name")
EXCEPTION(SinkAlreadyExists, 2034, "sink already exists")

/// 3XXX Errors during query runtime
EXCEPTION(BufferAllocationFailure, 3000, "buffer allocation failure")
EXCEPTION(QueryNotRegistered, 3001, "query not registered")
/// Todo #500: place holder until we can cleanly handle malformed string values (NULL/Undefined or similar)
/// MUST be used as the central exception to report malformed string values.
EXCEPTION(CannotFormatMalformedStringValue, 3002, "cannot format malformed string value")
EXCEPTION(CannotAllocateBuffer, 3003, "cannot allocate buffer")
EXCEPTION(SkippingDelayedTaskDuringShutdown, 3004, "skipping delayed task during shutdown")
EXCEPTION(TuplesTooLargeForPipelineBufferSize, 3005, "tuples too large for pipeline buffer size")

/// 4XXX Errors interpreting data stream, sources and sinks
EXCEPTION(CannotFormatSourceData, 4000, "cannot format source data")
EXCEPTION(RunningRoutineFailure, 4001, "error in running routine of SourceThread") /// TODO #237: Improve error handling in sources
EXCEPTION(CannotOpenSource, 4002, "failed to open a source")
EXCEPTION(FormattingError, 4003, "error during formatting")
EXCEPTION(CannotOpenSink, 4004, "failed to open a sink")

/// 5XXX API errors
EXCEPTION(QueryNotFound, 5000, "query is not registered")
EXCEPTION(QueryRegistrationFailed, 5001, "query registration call failed")
EXCEPTION(QueryStartFailed, 5002, "query start call failed")
EXCEPTION(QueryStopFailed, 5003, "query stop call failed")
EXCEPTION(QueryUnregistrationFailed, 5004, "query unregistration call failed")
EXCEPTION(QueryStatusFailed, 5005, "query status call failed")

/// 9XXX Internal errors (e.g. bugs)
EXCEPTION(FunctionNotImplemented, 9000, "function not implemented")
EXCEPTION(InvalidRefCountForBuffer, 9001, "invalid reference counter for buffer")
EXCEPTION(InvalidDynamicCast, 9002, "Invalid dynamic cast")
EXCEPTION(NotImplemented, 9003, "not implemented")

/// Special errors
EXCEPTION(UnknownException, 9999, "unknown exception")

/// Test Exception is thrown in testing related code, to differentiate between system errors and errors during test setup/cleanup
EXCEPTION(TestException, 10000, "exception in test code")
