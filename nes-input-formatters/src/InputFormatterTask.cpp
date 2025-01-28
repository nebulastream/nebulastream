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

#include <memory>
#include <Identifiers/Identifiers.hpp>
#include <InputFormatters/InputFormatterTask.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <SequenceShredder.hpp>
#include "InputFormatters/InputFormatter.hpp"


// Nomenclature:
// - rTB: raw tuple buffer
// - fTB: formatted tuple buffer
// - full tuple: entire tuple is in exactly 1 rTB
// - partial tuple: tuple spans across rTBs
// - trTB: tokenized rTB
// - TupDel: tuple delimiter
//
// Main Exploit: We can parse full tuples in parallel across rTBs as long as the resulting fTBs link the tuples correctly to the original TB (sequence numbers)
//
// Cases:
// 1. [x] [..] .. (rTB is first in sequence)
// 2. .. [..] [x] [..] .. (rTB is sandwiched)
// 3. .. [..] [x] (rTB is last in sequence)
//
// Two threads can write to fTB in parallel, if offsets are correct and output schemas
//
// Generalize case 2 to also handle 1/3:
//     1. Parse until first tupleDelimiter
//     2. Check if pre-slot is locked
//     If it is not locked:
//         3.1. If is head, continue
//         3.2 If not, check if slot contains fTB
//         3.2.1 If contains sTB, take, write from offset
//
//         Todo: is offset calculation necessary? Other thread can just write, is sTB even necessary?
//
//         3.2.2 If not, getBuffer -> fTB, create sTB(fTB, offset(tuple size - offset of first TupDel)), start writing from offset
//         4. Continue until last (partial) tuple
//         If it is locked:
//             3. Wait until lock is free, then lock
//             5. take sTB from slot, then free lock
//             6. Start writing first tuple from offset
//             7. Continue until last (partial) tuple
//             -----
//             (Before parsing last (partial) tuple)
//             5./8. Emit current fTB
//             9. Check if post-slot is locked
//             9.1 If not, getBuffer -> fTB, create sTB(fTB, offset(tuple size - size of partial tuple)), start writing (to offset), finish
//             9.2.1 If is locked, wait until lock is free, get lock, get sTB from lock
//             9.2.2 write to sTB, finish

namespace NES::InputFormatters {

// Todo: think about setup/stop
// Todo: implement interface with
// - processBeginningPartialTupleSynchronized()
// - processEndingPartialTupleSynchronized()
// - processMiddleTuples()
// - execute, which calls in order:
//   - processBeginningPartialTupleSynchronized
//      - processes first tuple; synchronizes with prior task if necessary (first byte(s) of buffer is not TupleDelimiter)
//   - processMiddleTuples
//      - asynchronously process all tuples between the first and the last TupleDelimiter
//   - processEndingPartialTupleSynchronized
//      - process last tuple; synchronize with subsequent task (even if last byte is TupleDelimiter,
//      the next task must know, or potentially the next task already wrote an entire tuple that the current task must process)

// Todo: alternative: Iterators
// - each parser must implement an iterator (virtual function call is fine (only 1))
//      - iterator must be able to handle partial data
// - execute:
//      - get iterator
//      - check prior slot in staging area
//          - if contains staged TB: get state from previous formatter, continue reading first partial tuple
//          - if not: getBufferBlocking; create stagedTB(buffer, rawBuffer) <--- other Task will construct/write exactly one tuple
//      - start parsing with iterator; start writing at offset, which is size of one tuple
//      - emit current buffer
//      - if last position == num bytes in rawTB
//          - check next slot in staging area
//          - if contains staged TB: parse first tuple; finish
//          - if not: write 'empty' staged TB that signalizes that there was no partial tuple (or one with offset == size of buffer); finish
//      - if last position != num bytes in rawTB
//          - check next slot in staging area
//          - if contains 'empty' staged TB: parse last tuple; finish
//          - if contains 'non-empty' staged TB: construct tuple from own rawTB and the one from the slot; parse tuple; finish
//          - if it does not contain staged TB: get buffer; parse current tuple to end of rawTB; write formatter state to staging area; finish


// Todo: argument against iterators
// - iterators will probably be extremely heterogeneous
// - it is difficult to force them to provide an interface that can be used by the InputFormatterTask
//      - example: must read bytes of partial first tuple (difficult to enforce)
//      - example: somehow must provide access to partial first tuple
//      - similar with ending partial tuple

// Todo: loose ideas:
// - execute staging-area tasks with separate thread (in task)
// - allows main parsing work to essentially happen sequentially
//      - does it make sense on start? -> on start, check if buffer available, otherwise get buffer (is blocking, no matter what) and read into buffer
//          - no real benefit
//      - on end? -> emit buffer if last symbol does not clarify tuple delimiter; get buffer (might be blocking and might block task)
//          - could give the pipelinecontext the 'task' to read last bytes of buffer into new buffer, as soon as available, if not directly available
//          - would free up task, should be optimization
//      - could make parser task wait for subsequent task

// Information
// Format Types:
// 1. Can detect tuple delimiter without any knowledge about prior or later data (except for checking for escape symbols around the delimiter)
//      - allows to chunk data and to parse all complete chunks(tuples) in parallel and only the initial and final partial chunk(tuple) coordinated
//      - examples (CSV, (JSON))
// 2. Can detect a delimiter if (potentially) all prior data were seen
//      - requires sequential parsing
//      - examples: delta encoded data, LZ encoded data

// Backwards parsing:
// - at least two different kinds of parsers:
//  1. allows backwards parsing (e.g. CSV)
//      - mistake: we can just parse CSV forwards from a partial tuple! We don't know where we are in the CSV tuple string, we need to parse in reverse
//  2. does not allow backwards parsing (e.g., delta compressed data)
//      - requires sequential parsing for partial tuples (or for all tuples!? <-- how could we detect a tuple separation?)
//      - potentially, synchronized source/parser pipelines make sense here

// Todo:
// - assume type 1 formats
// - interface:
//  - each InputFormatter determines a static (constexpr) symbol that allows to separate tuples (could later extend to control symbols (e.g., {,[,.. for JSON))
//  - InputFormatterTask<InputFormatter> <-- use CRTP to instantiate InputFormatterTask, gives access to InputFormatter-specific symbol
//  Do:
//  - InputFormatterTask takes rawBuffer
//  - InputFormatterTask chunks rawBuffer (determines all tuple separators, does not respect escaped tuple separator symbols for now)
//  - initiates BEGIN_SYNCHRONIZATION for data in front of first tuple separator (synchronous for now)
//  - iterates over all tuples between first and last tuple separator and writes the to tupleBuffer
//  - emits TupleBuffer
//  - initiates END_SYNCHRONIZATION

InputFormatterTask::InputFormatterTask(std::unique_ptr<InputFormatter> inputFormatter)
    : inputFormatter(std::move(inputFormatter)), sequenceShredder(std::make_unique<SequenceShredder>())
{
}
InputFormatterTask::~InputFormatterTask() = default;
void InputFormatterTask::stop(Runtime::Execution::PipelineExecutionContext& pipelineExecutionContext)
{
    inputFormatter->flushBuffers(pipelineExecutionContext, *sequenceShredder);
}
// Todo: think about layouts
// Todo: do we create a new InputFormatterTask everytime we execute the pipeline?
// - if yes: then the InputFormatterTask should have a static SequenceShredder that all instances can use
// - if no:  then the InputFormatterTask should have a non-static SequenceShredder that it passes to the CSVInputFormatter
// - Most problematic: one InputFormatterTask <--- means synchronization is necessary!, probably best to construct the actual parser on each 'execute' call and pass the SequenceShredder to it
void InputFormatterTask::execute(
    const Memory::TupleBuffer& inputTupleBuffer,
    Runtime::Execution::PipelineExecutionContext& pipelineExecutionContext)
{
    /// Todo: We cannot simply call inputFormatter each time, since the ThreadPool of the QueryEngine may execute this Task many times concurrently.
    /// - each Task in the TaskQueue contains a RunningQueryPlanNode that holds a unique_ptr to a ExecutablePipelineStage, which is this InputFormatterTask
    // either, we create a new input formatter each time, or, the CSVInputFormatter is stateless/thread-safe
    // (temporary) Solution
    // -> the CSVInputFormatter is thread-safe, since it does not change its state after construction, each call to 'parseTupleBufferRaw' creates a new ProgressTracker that is stateful for the duration of the call
    inputFormatter->parseTupleBufferRaw(inputTupleBuffer, pipelineExecutionContext, inputTupleBuffer.getNumberOfTuples(), *sequenceShredder);
}
}