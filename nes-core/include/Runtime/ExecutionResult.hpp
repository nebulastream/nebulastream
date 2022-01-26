/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#ifndef NES_INCLUDE_RUNTIME_EXECUTIONRESULT_HPP_
#define NES_INCLUDE_RUNTIME_EXECUTIONRESULT_HPP_

namespace NES {

enum class ExecutionResult : unsigned char {
    /// Execution completed successfully
    Ok = 0,
    /// Execution completed unsuccessfully -> Must handle error
    Error,
    /// Query Execution completed successfully and no further data are to be expected
    Finished,
    /// All Queries Execution completed successfully and no further data are to be expected
    AllFinished
};

}

#endif  // NES_INCLUDE_RUNTIME_EXECUTIONRESULT_HPP_
