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

#define UNIKERNEL_TRACE_PIPELINE_EXECUTION
#ifdef UNIKERNEL_TRACE_PIPELINE_EXECUTION
#define TRACE_PIPELINE_SETUP(...) NES_INFO("Pipeline: Setup {} {}", StageId, fmt::format(__VA_ARGS__))
#define TRACE_PIPELINE_EXECUTE(...) NES_INFO("Pipeline: Execute {} {}", StageId, fmt::format(__VA_ARGS__))
#define TRACE_PIPELINE_STOP(...)                                                                                                 \
    NES_INFO("Pipeline: Stopping {} {} {}", magic_enum::enum_name(type), StageId, fmt::format(__VA_ARGS__))
#define TRACE_PIPELINE_STOP_REQUEST(...)                                                                                         \
    NES_INFO("Pipeline: Stop Requested {} {} {}", magic_enum::enum_name(type), StageId, fmt::format(__VA_ARGS__))
#else
#define TRACE_PIPELINE_SETUP(...)
#define TRACE_PIPELINE_EXECUTE(...)
#define TRACE_PIPELINE_STOP(...)
#define TRACE_PIPELINE_STOP_REQUEST(...)
#endif

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
        TRACE_PIPELINE_SETUP("Pipeline");
        auto ctx = UnikernelPipelineExecutionContext::create<Prev, Stage>();
        Stage::setup(ctx, TheWorkerContext);
        Next::setup();
    }

    static void stop(size_t /*stage_id*/, Runtime::QueryTerminationType type) {
        TRACE_PIPELINE_STOP("Pipeline");
        auto ctx = UnikernelPipelineExecutionContext::create<Prev, Stage>();
        Stage::terminate(ctx, TheWorkerContext);
        Prev::stop(Stage::StageId, type);
    }

    static void request_stop(Runtime::QueryTerminationType type) {
        TRACE_PIPELINE_STOP_REQUEST("Pipeline");
        Next::request_stop(type);
        auto ctx = UnikernelPipelineExecutionContext::create<Prev, Stage>();
        Stage::terminate(ctx, TheWorkerContext);
    }

    static void execute(NES::Runtime::TupleBuffer& tb) {
        TRACE_PIPELINE_EXECUTE("Pipeline");
        auto ctx = UnikernelPipelineExecutionContext::create<Prev, Stage>();
        Stage::execute(ctx, TheWorkerContext, tb);
    }
};

template<UnikernelBackwardsPipelineImpl Prev, typename Source>
class PipelineImpl<Prev, Source> {
    using SourceImpl = Source::template Impl<Prev>;
    constexpr static size_t StageId = SourceImpl::Id;

  public:
    static void setup() {
        TRACE_PIPELINE_SETUP("Pipeline Source");
        SourceImpl::setup();
    }

    static void request_stop(Runtime::QueryTerminationType type) {
        TRACE_PIPELINE_STOP_REQUEST("Pipeline Source");
        SourceImpl::request_stop(type);
    }
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
    constexpr static size_t StageId = 0;

  public:
    static void setup() {
        TRACE_PIPELINE_SETUP("SubQuery");
        SinkImpl::setup();
        PipelineImpl::setup();
    }
    static void stop(Runtime::QueryTerminationType type) {
        TRACE_PIPELINE_STOP("SubQuery");
        Query::stop(type);
    }

    static void request_stop(Runtime::QueryTerminationType type) {
        TRACE_PIPELINE_STOP_REQUEST("SubQuery");
        PipelineImpl::request_stop(type);
        SinkImpl::request_stop(type);
        Query::stop(type);
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
    static std::atomic_bool eos_left;
    static std::atomic_bool eos_right;

  public:
    constexpr static size_t StageId = Stage::StageId;
    static void setup() {
        TRACE_PIPELINE_SETUP("PipelineJoin")
        auto ctx = UnikernelPipelineExecutionContext::create<Prev, Stage>();
        Stage::setup(ctx, TheWorkerContext);
        LeftImpl::setup();
        RightImpl::setup();
    }

    static void execute(NES::Runtime::TupleBuffer& tb) {
        NES_ASSERT(tb.getBuffer() != nullptr, "Execute called with empty tuple buffer");
        NES_ASSERT(tb.getOriginId() != 0, "Execute called with tuple buffer with a bad origin id");

        TRACE_PIPELINE_EXECUTE("PipelineJoin");
        auto ctx = UnikernelPipelineExecutionContext::create<Prev, Stage>();
        Stage::execute(ctx, TheWorkerContext, tb);
    }

    static void stop(size_t stageId, Runtime::QueryTerminationType type) {
        NES_ASSERT(stageId == LeftImpl::StageId || stageId == RightImpl::StageId, "Stop called with bad arguments");

        TRACE_PIPELINE_STOP("PipelineJoin");
        if (type == Runtime::QueryTerminationType::Graceful) {
            if (stageId == LeftImpl::StageId) {
                NES_INFO("PipelineJoin EoS for Left: {}", stageId);
                if (eos_left.exchange(true)) {
                    NES_FATAL_ERROR("Left called EoS twice", stageId);
                }
            } else if (stageId == RightImpl::StageId) {
                NES_INFO("PipelineJoin EoS for Right: {}", stageId);
                if (eos_right.exchange(true)) {
                    NES_FATAL_ERROR("Right called EoS twice", stageId);
                }
            }

            if (eos_right.load() && eos_left.load()) {
                NES_INFO("PipelineJoin left and right are EoS propagating: {}", stageId);
                auto ctx = UnikernelPipelineExecutionContext::create<Prev, Stage>();
                Stage::terminate(ctx, TheWorkerContext);
                Prev::stop(Stage::StageId, type);
            }
        } else {
            if (stageId == LeftImpl::StageId && !eos_left.exchange(true)) {
                RightImpl::request_stop(type);
            } else if (stageId == RightImpl::StageId && !eos_right.exchange(true)) {
                LeftImpl::request_stop(type);
            } else {
                return;
            }

            auto ctx = UnikernelPipelineExecutionContext::create<Prev, Stage>();
            Stage::terminate(ctx, TheWorkerContext);
            Prev::stop(Stage::StageId, type);
        }
    }

    static void request_stop(Runtime::QueryTerminationType type) {
        TRACE_PIPELINE_STOP_REQUEST("PipelineJoin")
        if (!eos_left.exchange(true)) {
            LeftImpl::request_stop(type);
        }

        if (!eos_right.exchange(true)) {
            RightImpl::request_stop(type);
        }

        auto ctx = UnikernelPipelineExecutionContext::create<Prev, Stage>();
        Stage::terminate(ctx, TheWorkerContext);
    }
};

template<UnikernelBackwardsPipelineImpl Prev,
         NES::Unikernel::UnikernelStage Stage,
         UnikernelPipeline<Prev> Left,
         UnikernelPipeline<Prev> Right>
std::atomic_bool PipelineJoinImpl<Prev, Stage, Left, Right>::eos_left = false;
template<UnikernelBackwardsPipelineImpl Prev,
         NES::Unikernel::UnikernelStage Stage,
         UnikernelPipeline<Prev> Left,
         UnikernelPipeline<Prev> Right>
std::atomic_bool PipelineJoinImpl<Prev, Stage, Left, Right>::eos_right = false;

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

    static void stop(Runtime::QueryTerminationType type) {
        {
            std::unique_lock lock(m);
            is_running = false;
        }
        stop_condition.notify_all();
        NES_INFO("Query has stopped: {}", magic_enum::enum_name(type));
    }

    static void request_stop(Runtime::QueryTerminationType type) {
        NES_INFO("Query Stop Requested");
        (SubQuery::template Impl<UnikernelExecutionPlan>::request_stop(type), ...);
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
