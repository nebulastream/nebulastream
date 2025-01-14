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


namespace NES::Runtime::Execution::Aggregation
{

/// Base class for any aggregation state. This class is used to store the intermediate state of an aggregation.
/// For example, the aggregation state for a sum aggregation would be the sum of all values seen so far.
/// For a median aggregation, the aggregation value would be a data structure that stores all seen values so far.
struct AggregationState
{
};

}
