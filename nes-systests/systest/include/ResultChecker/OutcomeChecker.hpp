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

#include <span>

#include <coordinator/lib.h>

#include <Model/RunnableTest.hpp>
#include <Model/Verdict.hpp>

namespace NES
{

/// Checks the coordinator's answers to one case against what the test expects.
/// A query answers with one outcome and each kind of expectation is a different check.
/// A differential block answers with one outcome per half that ran, and its check is that the two results agree.
[[nodiscard]] Verdict checkCase(std::span<const StatementOutcome> outcomes, const RunnableCase& testCase);

}
