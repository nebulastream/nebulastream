//! C ABI between NebulaStream operators and the unmodified reference query manager.
use reference_query_manager::query_plan::{
    QueryAdaptation, QueryPlanPipeline, QueryPlanPipelineId, QueryPlanSource, QueryPlanSourceId,
};
use reference_query_manager::traits::{
    DataBuffer, DataUnit, Pipeline, PipelineError, PipelineExecutionContext, Sequence, Source,
    SourceResult,
};
use reference_query_manager::{
    QueryError, QueryFork, QueryId, QueryListener, QueryManager, QueryPlan, SharingId, StateDonor,
};
use std::collections::VecDeque;
use std::ffi::{CStr, c_char, c_void};
use std::slice;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};
use std::time::Duration;

type Output = unsafe extern "C" fn(*mut c_void, *mut c_void, u64);

unsafe extern "C" {
    fn nes_ref_release_buffer(buffer: *mut c_void);
    fn nes_ref_pipeline_absorb(pipeline: *mut c_void, state: *mut c_void) -> bool;
    fn nes_ref_pipeline_emit(pipeline: *mut c_void, output: Output, context: *mut c_void);
    fn nes_ref_pipeline_execute(
        pipeline: *mut c_void,
        buffer: *mut c_void,
        output: Output,
        context: *mut c_void,
    ) -> bool;
    fn nes_ref_pipeline_release(pipeline: *mut c_void);
    fn nes_ref_source_start(source: *mut c_void, sender: *mut c_void) -> bool;
    fn nes_ref_source_stop(source: *mut c_void);
    fn nes_ref_source_release(source: *mut c_void);
    fn nes_ref_query_stopped(listener: *mut c_void, query: usize);
    fn nes_ref_query_failed(listener: *mut c_void, query: usize);
}

struct CppPipeline {
    handle: *mut c_void,
    initialized: AtomicBool,
}
// SAFETY: The C++ stage wrapper serializes calls to its mutable stage and owns
// thread-safe TupleBuffer handles; its release operation is valid on any thread.
unsafe impl Send for CppPipeline {}
unsafe impl Sync for CppPipeline {}

struct CppOutput<'a>(&'a mut dyn PipelineExecutionContext);

unsafe extern "C" fn output(context: *mut c_void, buffer: *mut c_void, sequence: u64) {
    // SAFETY: C++ calls this synchronously during execute, while CppOutput lives.
    let context = unsafe { &mut *(context.cast::<CppOutput<'_>>()) };
    let data = unsafe { DataBuffer::from_opaque_raw(buffer, nes_ref_release_buffer) };
    if let Some(data) = data {
        context.0.output(DataUnit {
            sequence: Sequence(sequence as usize),
            data,
        });
    }
}

impl Pipeline for CppPipeline {
    fn execute(
        &self,
        data: &DataUnit,
        context: &mut dyn PipelineExecutionContext,
    ) -> Result<(), PipelineError> {
        let Some(buffer) = (unsafe { data.data.as_opaque_raw() }) else {
            return Ok(());
        };
        let mut bridge = CppOutput(context);
        let ok = unsafe {
            nes_ref_pipeline_execute(
                self.handle,
                buffer,
                output,
                (&mut bridge as *mut CppOutput).cast(),
            )
        };
        if ok {
            Ok(())
        } else {
            Err("pipeline execution failed".into())
        }
    }

    // ponytail: donor index ignored; C++ absorb expects exactly one donor per target.
    fn absorb(&self, _donor: usize, data: &DataUnit, _: &mut dyn PipelineExecutionContext) {
        let state = unsafe { data.data.as_opaque_raw() }.unwrap_or(std::ptr::null_mut());
        self.initialized.store(
            unsafe { nes_ref_pipeline_absorb(self.handle, state) },
            Ordering::Release,
        );
    }

    fn emit(&self, context: &mut dyn PipelineExecutionContext) {
        let mut bridge = CppOutput(context);
        unsafe {
            nes_ref_pipeline_emit(self.handle, output, (&mut bridge as *mut CppOutput).cast())
        };
    }
}

impl Drop for CppPipeline {
    fn drop(&mut self) {
        unsafe { nes_ref_pipeline_release(self.handle) }
    }
}

/// A polled buffer and its raw byte count.
type SourceItem = Option<(DataUnit, u64)>;

/// The C++ callback's end of a source channel. `queued` counts buffers sent but not yet polled, so the C++
/// source thread can wait instead of filling the buffer pool faster than the query drains it.
struct SourceSender {
    sender: Sender<SourceItem>,
    queued: Arc<AtomicUsize>,
}

struct CppSource {
    handle: *mut c_void,
    receiver: Receiver<SourceItem>,
    sender: Option<Box<SourceSender>>,
    queued: Arc<AtomicUsize>,
    started: bool,
    /// Bytes consumed and next sequence number at the last poll: the source's
    /// position at a commit's cut, which C++ skips to when it resumes.
    bytes: u64,
    next_sequence: u64,
    answers: VecDeque<SourceResult>,
}
// SAFETY: Source is owned and polled on one Rust source thread. Its C++ source
// is stopped synchronously before its callback sender is released.
unsafe impl Send for CppSource {}

impl Source for CppSource {
    fn start(&mut self) {
        let sender = self.sender.as_ref().expect("source starts once");
        self.started = unsafe {
            nes_ref_source_start(
                self.handle,
                (&**sender as *const SourceSender).cast_mut().cast(),
            )
        };
    }
    fn poll(&mut self) -> SourceResult {
        if let Some(answer) = self.answers.pop_front() {
            return answer;
        }
        if !self.started {
            return SourceResult::EOS;
        }
        match self.receiver.try_recv() {
            Ok(Some((data, bytes))) => {
                self.queued.fetch_sub(1, Ordering::Release);
                self.bytes += bytes;
                self.next_sequence = data.sequence.0 as u64 + 1;
                SourceResult::Data(data)
            }
            Ok(None) | Err(TryRecvError::Disconnected) => SourceResult::EOS,
            Err(TryRecvError::Empty) => SourceResult::Pending,
        }
    }
    fn stop(&mut self) {
        if self.started {
            unsafe { nes_ref_source_stop(self.handle) };
            self.started = false;
        }
    }
    fn request_state(&mut self) {
        let mut position = self.bytes.to_le_bytes().to_vec();
        position.extend(self.next_sequence.to_le_bytes());
        self.answers.push_back(SourceResult::State(DataUnit {
            sequence: Sequence(0),
            data: position.into(),
        }));
        self.answers.push_back(SourceResult::StateEnd);
    }
}
impl Drop for CppSource {
    fn drop(&mut self) {
        self.stop();
        unsafe { nes_ref_source_release(self.handle) };
    }
}

/// Called by C++ with an owned TupleBuffer copy. This callback only takes
/// ownership when the sender is still alive.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rq_source_data(
    sender: *mut c_void,
    buffer: *mut c_void,
    sequence: u64,
    bytes: u64,
) {
    let Some(data) = (unsafe { DataBuffer::from_opaque_raw(buffer, nes_ref_release_buffer) })
    else {
        return;
    };
    let sender = unsafe { &*sender.cast::<SourceSender>() };
    sender.queued.fetch_add(1, Ordering::Release);
    let _ = sender.sender.send(Some((
        DataUnit {
            sequence: Sequence(sequence as usize),
            data,
        },
        bytes,
    )));
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn rq_source_eos(sender: *mut c_void) {
    let sender = unsafe { &*sender.cast::<SourceSender>() };
    let _ = sender.sender.send(None);
}

/// Buffers the source sent that were not polled yet.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rq_source_queued(sender: *mut c_void) -> usize {
    unsafe { &*sender.cast::<SourceSender>() }
        .queued
        .load(Ordering::Acquire)
}

struct Listener(*mut c_void);
unsafe impl Send for Listener {}
impl QueryListener for Listener {
    fn on_query_stopped(&mut self, id: QueryId) {
        unsafe { nes_ref_query_stopped(self.0, id.0) };
    }

    fn on_query_failed(&mut self, id: QueryId, _: QueryError) {
        // C++ keeps the original exception (error code included) and reports it.
        unsafe { nes_ref_query_failed(self.0, id.0) };
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn rq_manager_new(listener: *mut c_void) -> *mut QueryManager {
    Box::into_raw(Box::new(QueryManager::new(Listener(listener))))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn rq_manager_free(manager: *mut QueryManager) {
    if !manager.is_null() {
        drop(unsafe { Box::from_raw(manager) });
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn rq_plan_new(query: usize) -> *mut QueryPlan {
    Box::into_raw(Box::new(QueryPlan {
        id: QueryId(query),
        sources: vec![],
        pipelines: vec![],
    }))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn rq_plan_free(plan: *mut QueryPlan) {
    if !plan.is_null() {
        drop(unsafe { Box::from_raw(plan) });
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn rq_plan_add_pipeline(
    plan: *mut QueryPlan,
    id: usize,
    handle: *mut c_void,
    successors: *const usize,
    count: usize,
    sharing_id: *const c_char,
) -> bool {
    let successors = if count == 0 {
        &[]
    } else {
        unsafe { slice::from_raw_parts(successors, count) }
    }
    .iter()
    .copied()
    .map(QueryPlanPipelineId)
    .collect();
    let pipeline = CppPipeline {
        handle,
        initialized: AtomicBool::new(false),
    };
    // Initial attachment has no donor state. The reference manager only calls
    // absorb itself when migrating state from an existing pipeline.
    pipeline.absorb(
        0,
        &DataUnit {
            sequence: Sequence(0),
            data: DataBuffer::from_bytes(Vec::new()),
        },
        &mut InitialContext,
    );
    let initialized = pipeline.initialized.load(Ordering::Acquire);
    let sharing_id = if sharing_id.is_null() {
        None
    } else {
        Some(SharingId(
            unsafe { CStr::from_ptr(sharing_id) }
                .to_string_lossy()
                .into_owned(),
        ))
    };
    unsafe { &mut *plan }.pipelines.push(QueryPlanPipeline {
        id: QueryPlanPipelineId(id),
        sharing_id,
        pipeline: Box::new(pipeline),
        successors,
    });
    initialized
}

struct InitialContext;
impl PipelineExecutionContext for InitialContext {
    fn output(&mut self, _: DataUnit) {}
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn rq_plan_add_source(
    plan: *mut QueryPlan,
    id: usize,
    handle: *mut c_void,
    successors: *const usize,
    count: usize,
    sharing_id: *const c_char,
    start_bytes: u64,
    next_sequence: u64,
) {
    let successors = if count == 0 {
        &[]
    } else {
        unsafe { slice::from_raw_parts(successors, count) }
    }
    .iter()
    .copied()
    .map(QueryPlanPipelineId)
    .collect();
    let (tx, receiver) = mpsc::channel();
    let queued = Arc::new(AtomicUsize::new(0));
    let sharing_id = if sharing_id.is_null() {
        None
    } else {
        Some(SharingId(
            unsafe { CStr::from_ptr(sharing_id) }
                .to_string_lossy()
                .into_owned(),
        ))
    };
    unsafe { &mut *plan }.sources.push(QueryPlanSource {
        id: QueryPlanSourceId(id),
        sharing_id,
        source: Box::new(CppSource {
            handle,
            receiver,
            sender: Some(Box::new(SourceSender {
                sender: tx,
                queued: Arc::clone(&queued),
            })),
            queued,
            started: false,
            bytes: start_bytes,
            next_sequence,
            answers: VecDeque::new(),
        }),
        successors,
    });
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn rq_manager_start(
    manager: *mut QueryManager,
    plan: *mut QueryPlan,
) -> bool {
    let plan = unsafe { Box::from_raw(plan) };
    unsafe { &*manager }.start_query(*plan).is_ok()
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn rq_manager_stop(manager: *mut QueryManager, query: usize) -> bool {
    unsafe { &*manager }.stop_query(QueryId(query)).is_ok()
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn rq_manager_adapt(
    manager: *mut QueryManager,
    plan: *mut QueryPlan,
    donors: *const usize,
    targets: *const usize,
    count: usize,
) -> bool {
    let plan = unsafe { Box::from_raw(plan) };
    let mut adaptation = QueryAdaptation::new(*plan);
    if count != 0 {
        let donors = unsafe { slice::from_raw_parts(donors, count) };
        let targets = unsafe { slice::from_raw_parts(targets, count) };
        for (&donor, &target) in donors.iter().zip(targets) {
            adaptation =
                adaptation.transfer_state(QueryPlanPipelineId(donor), QueryPlanPipelineId(target));
        }
    }
    unsafe { &*manager }.adapt_query(adaptation).is_ok()
}

/// Units absorbed by a snapshot sink, tagged with their donor's position.
pub struct Snapshot(Vec<(usize, DataUnit)>);

/// Input-less snapshot sink. The engine drops it once it has absorbed every
/// donor, which closes the channel and completes the snapshot.
struct SnapshotSink(Mutex<Sender<(usize, DataUnit)>>);
impl Pipeline for SnapshotSink {
    fn execute(
        &self,
        _: &DataUnit,
        _: &mut dyn PipelineExecutionContext,
    ) -> Result<(), PipelineError> {
        Ok(())
    }
    fn absorb(&self, donor: usize, data: &DataUnit, _: &mut dyn PipelineExecutionContext) {
        let _ = self.0.lock().unwrap().send((donor, data.clone()));
    }
    fn emit(&self, _: &mut dyn PipelineExecutionContext) {}
}

/// Replays one donor's snapshot units as state, then ends.
struct SnapshotSource {
    units: Vec<DataUnit>,
    answers: VecDeque<SourceResult>,
}
impl Source for SnapshotSource {
    fn start(&mut self) {}
    fn poll(&mut self) -> SourceResult {
        self.answers.pop_front().unwrap_or(SourceResult::Pending)
    }
    fn stop(&mut self) {}
    fn request_state(&mut self) {
        self.answers
            .extend(self.units.drain(..).map(SourceResult::State));
        self.answers.push_back(SourceResult::StateEnd);
        self.answers.push_back(SourceResult::EOS);
    }
}

/// Pause a query in one commit: every listed pipeline and the source donate
/// state to a snapshot sink, and the running plan is removed. Donor positions
/// follow `pipelines`; the source is the last donor. Returns null on failure.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rq_manager_pause(
    manager: *mut QueryManager,
    query: usize,
    pipelines: *const usize,
    count: usize,
    source: usize,
) -> *mut Snapshot {
    let (tx, rx) = mpsc::channel();
    let mut plan = QueryPlan::new(QueryId(query));
    let sink = plan.add_state_target(Box::new(SnapshotSink(Mutex::new(tx))));
    let mut adaptation = QueryAdaptation::new(plan);
    let pipelines = if count == 0 {
        &[]
    } else {
        unsafe { slice::from_raw_parts(pipelines, count) }
    };
    for &pipeline in pipelines {
        adaptation = adaptation.transfer_state(QueryPlanPipelineId(pipeline), sink);
    }
    adaptation =
        adaptation.transfer_state(StateDonor::Current(QueryPlanSourceId(source).into()), sink);
    if unsafe { &*manager }.adapt_query(adaptation).is_err() {
        return std::ptr::null_mut();
    }
    let mut units = Vec::new();
    loop {
        match rx.recv_timeout(Duration::from_secs(30)) {
            Ok(unit) => units.push(unit),
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
            Err(mpsc::RecvTimeoutError::Timeout) => return std::ptr::null_mut(),
        }
    }
    Box::into_raw(Box::new(Snapshot(units)))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn rq_snapshot_free(snapshot: *mut Snapshot) {
    if !snapshot.is_null() {
        drop(unsafe { Box::from_raw(snapshot) });
    }
}

/// Reads the source position (bytes consumed, next sequence) donated by `donor`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rq_snapshot_source_position(
    snapshot: *const Snapshot,
    donor: usize,
    bytes: *mut u64,
    next_sequence: *mut u64,
) -> bool {
    let snapshot = unsafe { &*snapshot };
    let Some(position) = snapshot
        .0
        .iter()
        .find(|(position, _)| *position == donor)
        .and_then(|(_, unit)| unit.data.as_bytes())
        .filter(|position| position.len() == 16)
    else {
        return false;
    };
    unsafe {
        *bytes = u64::from_le_bytes(position[..8].try_into().unwrap());
        *next_sequence = u64::from_le_bytes(position[8..].try_into().unwrap());
    }
    true
}

/// Adds a state source that replays `donor`'s snapshot units; returns its plan ID.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rq_plan_add_snapshot_source(
    plan: *mut QueryPlan,
    snapshot: *const Snapshot,
    donor: usize,
) -> usize {
    let units = unsafe { &*snapshot }
        .0
        .iter()
        .filter(|(position, _)| *position == donor)
        .map(|(_, unit)| unit.clone())
        .collect();
    unsafe { &mut *plan }
        .add_state_source(Box::new(SnapshotSource {
            units,
            answers: VecDeque::new(),
        }))
        .0
}

/// Start a query whose pipelines load their state from the plan's own sources.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rq_manager_start_from(
    manager: *mut QueryManager,
    plan: *mut QueryPlan,
    sources: *const usize,
    targets: *const usize,
    count: usize,
) -> bool {
    let plan = unsafe { Box::from_raw(plan) };
    let mut fork = QueryFork::detached(*plan);
    if count != 0 {
        let sources = unsafe { slice::from_raw_parts(sources, count) };
        let targets = unsafe { slice::from_raw_parts(targets, count) };
        for (&source, &target) in sources.iter().zip(targets) {
            fork = fork.transfer_state(QueryPlanSourceId(source), QueryPlanPipelineId(target));
        }
    }
    unsafe { &*manager }.start_query_from(fork).is_ok()
}
