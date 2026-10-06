//! C ABI between NebulaStream operators and the unmodified reference query manager.
use reference_query_manager::query_plan::{
    QueryAdaptation, QueryPlanPipeline, QueryPlanPipelineId, QueryPlanSource, QueryPlanSourceId,
};
use reference_query_manager::traits::{
    DataBuffer, DataUnit, Pipeline, PipelineExecutionContext, Sequence, Source, SourceResult,
};
use reference_query_manager::{QueryId, QueryListener, QueryManager, QueryPlan, SharingId};
use std::ffi::{CStr, c_char, c_void};
use std::slice;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};

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
    );
    fn nes_ref_pipeline_release(pipeline: *mut c_void);
    fn nes_ref_source_start(source: *mut c_void, sender: *mut c_void) -> bool;
    fn nes_ref_source_stop(source: *mut c_void);
    fn nes_ref_source_release(source: *mut c_void);
    fn nes_ref_query_stopped(listener: *mut c_void, query: usize);
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
    fn execute(&self, data: &DataUnit, context: &mut dyn PipelineExecutionContext) {
        let Some(buffer) = (unsafe { data.data.as_opaque_raw() }) else {
            return;
        };
        let mut bridge = CppOutput(context);
        unsafe {
            nes_ref_pipeline_execute(
                self.handle,
                buffer,
                output,
                (&mut bridge as *mut CppOutput).cast(),
            )
        };
    }

    fn absorb(&self, data: &DataUnit, _: &mut dyn PipelineExecutionContext) {
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

struct CppSource {
    handle: *mut c_void,
    receiver: Receiver<Option<DataUnit>>,
    sender: Option<Box<Sender<Option<DataUnit>>>>,
    started: bool,
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
                (&**sender as *const Sender<_>).cast_mut().cast(),
            )
        };
    }
    fn poll(&mut self) -> SourceResult {
        if !self.started {
            return SourceResult::EOS;
        }
        match self.receiver.try_recv() {
            Ok(Some(data)) => SourceResult::Data(data),
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
pub unsafe extern "C" fn rq_source_data(sender: *mut c_void, buffer: *mut c_void, sequence: u64) {
    let Some(data) = (unsafe { DataBuffer::from_opaque_raw(buffer, nes_ref_release_buffer) })
    else {
        return;
    };
    let sender = unsafe { &*sender.cast::<Sender<Option<DataUnit>>>() };
    let _ = sender.send(Some(DataUnit {
        sequence: Sequence(sequence as usize),
        data,
    }));
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn rq_source_eos(sender: *mut c_void) {
    let sender = unsafe { &*sender.cast::<Sender<Option<DataUnit>>>() };
    let _ = sender.send(None);
}

struct Listener(*mut c_void);
unsafe impl Send for Listener {}
impl QueryListener for Listener {
    fn on_query_stopped(&mut self, id: QueryId) {
        unsafe { nes_ref_query_stopped(self.0, id.0) };
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
            sender: Some(Box::new(tx)),
            started: false,
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
