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
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/WorkerContext.hpp>
#include <UnikernelSink.hpp>
#include <UnikernelSource.hpp>
#include <UnikernelStage.hpp>
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

template<UnikernelBackwardsPipelineImpl Prev, NES::Unikernel::UnikernelStage Stage, typename... Stages>
class PipelineImpl<Prev, Stage, Stages...> {
    using Next = PipelineImpl<PipelineImpl, Stages...>;

  public:
    constexpr static size_t StageId = Stage::StageId;
    static void setup() {
        NES_INFO("Calling Setup for stage {}", StageId);
        auto ctx = UnikernelPipelineExecutionContext::create<Prev, Stage>();
        Stage::setup(ctx, TheWorkerContext);
        Next::setup();
    }

    static void stop(size_t /*stage_id*/) {
        NES_INFO("Calling Stop for stage {}", StageId);
        auto ctx = UnikernelPipelineExecutionContext::create<Prev, Stage>();
        Stage::terminate(ctx, TheWorkerContext);
        Prev::stop(Stage::StageId);
    }

    static void request_stop() {
        Next::request_stop();
        auto ctx = UnikernelPipelineExecutionContext::create<Prev, Stage>();
        Stage::terminate(ctx, TheWorkerContext);
    }

    static void execute(NES::Runtime::TupleBuffer& tb) {
        NES_INFO("Execute: {}", StageId);
        auto ctx = UnikernelPipelineExecutionContext::create<Prev, Stage>();
        Stage::execute(ctx, TheWorkerContext, tb);
    }
};

template<UnikernelBackwardsPipelineImpl Prev, typename Source>
class PipelineImpl<Prev, Source> {
    using SourceImpl = Source::template Impl<Prev>;

  public:
    static void setup() {
        NES_INFO("Calling Setup for Source {}", SourceImpl::Id);
        SourceImpl::setup();
    }

    static void request_stop() { SourceImpl::request_stop(); }
};

template<typename... Stages>
class Pipeline {
  public:
    template<typename Prev>
    using Impl = PipelineImpl<Prev, Stages...>;
};

template<typename Query, typename Sink, UnikernelPipeline<Sink> Pipeline>
class SubQueryImpl {
    using SinkImpl = typename Sink::template Impl<SubQueryImpl>;
    using PipelineImpl = typename Pipeline::template Impl<SinkImpl>;

  public:
    static void setup() {
        SinkImpl::setup();
        PipelineImpl::setup();
    }
    static void stop() {
        NES_INFO("SubQuery Stopped");
        Query::stop();
    }

    static void request_stop() {
        PipelineImpl::request_stop();
        SinkImpl::request_stop();
        Query::stop();
    }
};

template<typename Sink, UnikernelPipeline<Sink> Pipeline>
class SubQuery {
  public:
    template<typename Query>
    using Impl = SubQueryImpl<Query, Sink, Pipeline>;
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

    static void stop(size_t stageId) {
        std::cerr << "Calling stop" << std::endl;

        if (stageId == LeftImpl::StageId) {
            RightImpl::request_stop();
        } else {
            LeftImpl::request_stop();
        }

        auto ctx = UnikernelPipelineExecutionContext::create<Prev, Stage>();
        Stage::terminate(ctx, TheWorkerContext);

        Prev::stop(Stage::StageId);
    }

    static void request_stop() {
        std::cerr << "Requesting stop" << std::endl;
        LeftImpl::request_stop();
        RightImpl::request_stop();
        auto ctx = UnikernelPipelineExecutionContext::create<Prev, Stage>();
        Stage::terminate(ctx, TheWorkerContext);
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
    static std::condition_variable stop_condition;
    static std::mutex m;
    static bool is_running;

  public:
    static void setup() {
        (SubQuery::template Impl<UnikernelExecutionPlan>::setup(), ...);
        std::unique_lock lock(m);
        is_running = true;
    }

    static void stop() {
        {
            std::unique_lock lock(m);
            is_running = false;
        }
        stop_condition.notify_all();
        NES_INFO("Query has stopped");
    }

    static void request_stop() {
        NES_INFO("Query Stop Requested");
        (SubQuery::template Impl<UnikernelExecutionPlan>::request_stop(), ...);
    }

    static void wait() {
        std::unique_lock lock(m);
        stop_condition.wait(lock, []() {
            return !is_running;
        });
    }
};
template<typename... SubQuery>
std::condition_variable UnikernelExecutionPlan<SubQuery...>::stop_condition;

template<typename... SubQuery>
std::mutex UnikernelExecutionPlan<SubQuery...>::m;

template<typename... SubQuery>
bool UnikernelExecutionPlan<SubQuery...>::is_running = false;
}// namespace NES::Unikernel

#endif//NES_UNIKERNELEXECUTIONPLAN_H
