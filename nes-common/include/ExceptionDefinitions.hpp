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

#ifndef EXCEPTION
#    error "This file should not be included directly! Include it via <ErrorHandling.hpp>"
#endif

/// 1XXX Configuration Errors
EXCEPTION(InvalidConfigParameter, 1000, "invalid config parameter")
EXCEPTION(MissingConfigParameter, 1001, "missing config parameter")
EXCEPTION(CannotLoadConfig, 1002, "cannot load config")
EXCEPTION(SLTUnexpectedToken, 1003, "unexpected token in sql logic test file")

/// 2XXX Errors during query registration and compilation
EXCEPTION(InvalidQuerySyntax, 2000, "invalid query syntax")
EXCEPTION(CannotSerialize, 2001, "cannot serialize")
EXCEPTION(CannotDeserialize, 2002, "cannot deserialize")
EXCEPTION(CannotInferSchema, 2003, "cannot infer schema")
EXCEPTION(InvalidUseOfOperatorFunction, 2004, "invalid use of an operator function")

/// 21XX Errors during query compilation
EXCEPTION(UnknownWindowingStrategy, 2100, "unknown windowing strategy")
EXCEPTION(UnknownWindowType, 2101, "unknown window type")
EXCEPTION(UnknownPhysicalType, 2102, "unknown physical type")
EXCEPTION(UnknownJoinStrategy, 2103, "unknown join strategy")
EXCEPTION(UnknownPluginType, 2104, "unknown plugin type")
EXCEPTION(UnknownExpressionType, 2105, "unknown expression type")
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
EXCEPTION(UnknownWatermarkStrategy, 2116, "unknown watermark strategy")

/// 22XX NebuLI
EXCEPTION(QueryDescriptionNotReadable, 2200, "could not read query description")
EXCEPTION(QueryDescriptionNotParsable, 2201, "could not parse query description")
EXCEPTION(QueryInvalid, 2202, "query is invalid")
EXCEPTION(LogicalSourceNotFoundInQueryDescription, 2203, "logical source was not found in the query description")
EXCEPTION(PhysicalSourceNotFoundInQueryDescription, 2204, "physical source was not found in the query description")
EXCEPTION(OperatorNotFound, 2205, "operator not found")

/// 3XXX Errors during query runtime
EXCEPTION(BufferAllocationFailure, 3000, "buffer allocation failure")
EXCEPTION(CannotStartNodeEngine, 3003, "cannot start node engine")
EXCEPTION(CannotStopNodeEngine, 3004, "cannot stop node engine")
EXCEPTION(CannotStartQueryManager, 3005, "cannot start query manager")
EXCEPTION(CannotStopQueryManager, 3006, "cannot stop query manager")

/// 4XXX Errors interpreting data stream, sources and sinks
EXCEPTION(CannotOpenSourceFile, 4000, "cannot open source file")
EXCEPTION(CannotFormatSourceData, 4001, "cannot format source data")
EXCEPTION(MalformatedTuple, 4002, "malformed tuple")
EXCEPTION(RunningRoutineFailure, 4003, "error in running routine of SourceThread") ///Todo #237: Improve error handling in sources
EXCEPTION(StopBeforeStartFailure, 4003, "source was stopped before it was started") ///Todo #237: Improve error handling in sources

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
EXCEPTION(DynamicCast, 9006, "Invalid dynamic cast")
EXCEPTION(UnknownOperator, 9007, "unknown operator")

/// Special errors
EXCEPTION(UnknownException, 9999, "unknown exception")
