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

#ifndef NES_INCLUDE_QUERYCOMPILER_GENERATABLEOPERATORS_GENERATABLEOPERATORFORWARDREF_HPP_
#define NES_INCLUDE_QUERYCOMPILER_GENERATABLEOPERATORS_GENERATABLEOPERATORFORWARDREF_HPP_

#include <memory>
namespace NES {

class GeneratableFilterOperator;
typedef std::shared_ptr<GeneratableFilterOperator> GeneratableFilterOperatorPtr;

class GeneratableMapOperator;
typedef std::shared_ptr<GeneratableMapOperator> GeneratableMapOperatorPtr;

class GeneratableMergeOperator;
typedef std::shared_ptr<GeneratableMergeOperator> GeneratableMergeOperatorPtr;

class GeneratableJoinOperator;
typedef std::shared_ptr<GeneratableJoinOperator> GeneratableJoinOperatorPtr;


class GeneratableCompleteWindowOperator;
typedef std::shared_ptr<GeneratableCompleteWindowOperator> GeneratableWindowOperatorPtr;

class GeneratableSlicingWindowOperator;
typedef std::shared_ptr<GeneratableSlicingWindowOperator> GeneratableDistributedlWindowSliceCreationOperatorPtr;

class GeneratableCombiningWindowOperator;
typedef std::shared_ptr<GeneratableCombiningWindowOperator> GeneratableDistributedlWindowCombinerOperatorPtr;

class GeneratableSinkOperator;
typedef std::shared_ptr<GeneratableSinkOperator> GeneratableSinkOperatorPtr;

class GeneratableSourceOperator;
typedef std::shared_ptr<GeneratableSourceOperator> GeneratableSourceOperatorPtr;

class GeneratableScanOperator;
typedef std::shared_ptr<GeneratableScanOperator> GeneratableScanOperatorPtr;

class GeneratableWatermarkAssignerOperator;
typedef std::shared_ptr<GeneratableWatermarkAssignerOperator> GeneratableWatermarkAssignerOperatorPtr;

}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_GENERATABLEOPERATORS_GENERATABLEOPERATORFORWARDREF_HPP_
