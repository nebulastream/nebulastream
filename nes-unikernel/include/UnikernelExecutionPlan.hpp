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
#ifndef NES_UNIKERNELEXECUTIONPLAN_H
#define NES_UNIKERNELEXECUTIONPLAN_H
#include <Runtime/RuntimeForwardRefs.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <UnikernelSink.h>
#include <UnikernelSource.h>
#include <UnikernelStage.h>
#include <functional>
#include <iostream>
#include <string>
#include <tuple>

extern NES::Runtime::WorkerContextPtr TheWorkerContext;

namespace NES::Unikernel {
template<typename T>
concept UnikernelPipelineImpl = requires(T a) { T::setup(); };
template<typename T>
concept UnikernelBackwardsPipelineImpl = requires(T a) { /*T::execute(std::declval<NES::Runtime::TupleBuffer&>());*/
                                                         { a };
};

template<typename T, typename Prev>
concept UnikernelPipeline = requires(T a) {
    { a };
};
template<typename Prev, typename Source, typename... Stages>
class PipelineImpl;

template<UnikernelBackwardsPipelineImpl Prev,
         typename Source,
         NES::Unikernel::UnikernelStage Stage,
         NES::Unikernel::UnikernelStage... Stages>
class PipelineImpl<Prev, Source, Stage, Stages...> {
    using Next = PipelineImpl<PipelineImpl, Source, Stages...>;

  public:
    constexpr static size_t StageId = Stage::StageId;
    static void setup() {
        NES_INFO("Calling Setup for stage {}", StageId);
        auto ctx = UnikernelPipelineExecutionContext::create<Prev, Stage>();
        Stage::setup(ctx, TheWorkerContext);
        Next::setup();
    }

    static void stop() {
        auto ctx = UnikernelPipelineExecutionContext::create<Prev, Stage>();
        Stage::terminate(ctx, TheWorkerContext);
        Next::stop();
    }

    static void execute(NES::Runtime::TupleBuffer& tb) {
        NES_INFO("Execute: {}", StageId);
        auto ctx = UnikernelPipelineExecutionContext::create<Prev, Stage>();
        Stage::execute(ctx, TheWorkerContext, tb);
    }
};

template<UnikernelBackwardsPipelineImpl Prev, typename Source>
class PipelineImpl<Prev, Source> {
  public:
    static void setup() {
        auto ctx = UnikernelPipelineExecutionContext::create<Prev>();
        NES_INFO("Calling Setup for Source {}", Source::Id);
        Source::setup(ctx);
        Source::start();
    }

    static void stop() { Source::stop(); }
};

template<typename Source, NES::Unikernel::UnikernelStage... Stages>
class Pipeline {
  public:
    template<typename Prev>
    using Impl = PipelineImpl<Prev, Source, Stages...>;
};

template<typename Sink, UnikernelPipeline<Sink> Pipeline>
class SubQuery {
    using PipelineImpl = Pipeline::template Impl<Sink>;

  public:
    static void setup() {
        PipelineImpl::setup();
        Sink::setup();
    }
    static void stop() {
        PipelineImpl::stop();
        Sink::stop();
    }
};

template<UnikernelBackwardsPipelineImpl Prev,
         NES::Unikernel::UnikernelStage Stage,
         UnikernelPipeline<Prev> Left,
         UnikernelPipeline<Prev> Right>
class PipelineJoinImpl {
    using LeftImpl = Left::template Impl<PipelineJoinImpl>;
    using RightImpl = Right::template Impl<PipelineJoinImpl>;

  public:
    constexpr static size_t StageId = Stage::StageId;
    static void setup() {
        auto ctx = UnikernelPipelineExecutionContext::create<Prev, Stage>();
        NES_INFO("Calling Setup for Stage {}", Stage::StageId);
        Stage::setup(ctx, TheWorkerContext);
        LeftImpl::setup();
        RightImpl::setup();
    }

    static void execute(NES::Runtime::TupleBuffer& tb) {
        NES_INFO("Calling Execute for Stage {}", Stage::StageId);
        auto ctx = UnikernelPipelineExecutionContext::create<Prev, Stage>();
        Stage::execute(ctx, TheWorkerContext, tb);
    }

    static void stop() {
        auto ctx = UnikernelPipelineExecutionContext::create<Prev, Stage>();
        Stage::terminate(ctx, TheWorkerContext);
        LeftImpl::stop();
        RightImpl::stop();
    }
};

template<NES::Unikernel::UnikernelStage Stage, typename Left, typename Right>
class PipelineJoin {
  public:
    template<typename Prev>
    using Impl = PipelineJoinImpl<Prev, Stage, Left, Right>;
};

template<typename... SubQuery>
class UnikernelExecutionPlan {
  public:
    static void setup() { (SubQuery::setup(), ...); }

    static void stop() { (SubQuery::stop(), ...); }
};

}// namespace NES::Unikernel

#endif//NES_UNIKERNELEXECUTIONPLAN_H
