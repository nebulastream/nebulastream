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

/**
 * @brief The following pragma disables particular compiler warnings for a particular code fragment.
 * For example:
 *
 *  DISABLE_WARNING_PUSH
 *  DISABLE_WARNING_UNREFERENCED_FUNCTION
        ---
        code where you want
        to disable the warnings
        ---
 *  DISABLE_WARNING_POP
 *
 *  Disables unreferenced function warning.
 *
 *  The macro works for GCC, CLANG, and Visual Studio.
 *  Details here: https://www.fluentcpp.com/2019/08/30/how-to-disable-a-warning-in-cpp
 *
 */
/// clang-format off
#if defined(_MSC_VER)
    #define DISABLE_WARNING_PUSH __pragma(warning(push))
    #define DISABLE_WARNING_POP __pragma(warning(pop))
    #define DISABLE_WARNING(warningNumber) __pragma(warning(disable : warningNumber))

    #define DISABLE_WARNING_UNREFERENCED_FORMAL_PARAMETER DISABLE_WARNING(4100)
    #define DISABLE_WARNING_UNREFERENCED_FUNCTION DISABLE_WARNING(4505)
/// other warnings you want to deactivate...

#elif defined(__GNUC__) || defined(__clang__)
    #define DO_PRAGMA(X) _Pragma(#X)
    #define DISABLE_WARNING_PUSH DO_PRAGMA(GCC diagnostic push)
    #define DISABLE_WARNING_POP DO_PRAGMA(GCC diagnostic pop)
    #define DISABLE_WARNING(warningName) DO_PRAGMA(GCC diagnostic ignored #warningName)

    #define DISABLE_WARNING_UNREFERENCED_FORMAL_PARAMETER DISABLE_WARNING(-Wunused - parameter)
    #define DISABLE_WARNING_UNREFERENCED_FUNCTION DISABLE_WARNING(-Wunused - function)
/// other warnings you want to deactivate...

#else
    #define DISABLE_WARNING_PUSH
    #define DISABLE_WARNING_POP
    #define DISABLE_WARNING_UNREFERENCED_FORMAL_PARAMETER
    #define DISABLE_WARNING_UNREFERENCED_FUNCTION
/// other warnings you want to deactivate...

#endif
/// clang-format on
