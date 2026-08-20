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

#include <string>

#include <Rewriter/ClassifiedStatement.hpp>
#include <Rewriter/NameQualifier.hpp>
#include <Rewriter/SinkRewriting.hpp>

namespace NES
{

/// What is catalog-visible for one test file: each name's qualified spelling, and each declared sink's definition.
/// The declaring phase produces this, and the emitting phase rewrites every statement against it.
struct Declarations
{
    QualifiedNames names;
    SinkByName sinkByName;
};

/// Registers every catalog-visible name of the file before the emitting phase rewrites any statement against it.
/// Rewriting substitutes only registered names, so declaring everything first lets a statement refer to a name declared further down.
Declarations declareAll(ClassifiedTest& classified, const std::string& testFileKey);

}
