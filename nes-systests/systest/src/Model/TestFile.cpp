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

/// The model is types rather than code, so nothing else compiles its headers on their own. This translation unit does,
/// and the header it includes reaches every one of them, so a header that stops being self contained fails here rather
/// than at the first place that includes it.
/// It also gives the directory a compiled source, which clang-tidy needs to reconstruct the include paths of a header.

#include <Model/TestFile.hpp>
