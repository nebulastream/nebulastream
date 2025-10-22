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

use nes_network::protocol::TupleBuffer;
use std::collections::HashMap;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Arc, Mutex};
use threadpool::ThreadPool;
use tracing::{error, info};

pub(crate) type EmitFn = Box<dyn Fn(TupleBuffer) + Sync + Send>;
type Queue = crossbeam_queue::ArrayQueue<Task>;

pub struct Node {
    pipeline: Arc<dyn ExecutablePipeline + Send + Sync>,
    successor: Option<Arc<Node>>,
}

impl Drop for Node {
    fn drop(&mut self) {
        self.pipeline.stop();
    }
}

impl Node {
    pub fn new(
        successor: Option<Arc<Node>>,
        pipeline: Arc<dyn ExecutablePipeline + Send + Sync>,
    ) -> Arc<Self> {
        Arc::new(Self {
            pipeline,
            successor,
        })
    }
}

pub struct SourceNode {
    implementation: Box<dyn SourceImpl + Send + Sync>,
    successor: Arc<Node>,
}

impl SourceNode {
    fn start(&self, queue: Arc<Queue>, query_id: usize) {
        let successor = self.successor.clone();
        let emit_fn = Box::new(move |buffer| {
            let _ = queue.push(Task::Compute(query_id, buffer, successor.clone()));
        }) as EmitFn;
        self.implementation.start(emit_fn);
    }
    fn stop(&self) {
        self.implementation.stop()
    }
}

impl SourceNode {
    pub fn new(
        successor: Arc<Node>,
        implementation: Box<dyn SourceImpl + Send + Sync>,
    ) -> SourceNode {
        SourceNode {
            successor,
            implementation,
        }
    }
}

pub trait SourceImpl {
    fn start(&self, emit: EmitFn);
    fn stop(&self);
}
pub trait PipelineContext {
    #[allow(dead_code)]
    fn emit(&mut self, data: TupleBuffer);
}
pub trait ExecutablePipeline {
    fn execute(&self, data: &TupleBuffer, context: &mut dyn PipelineContext);
    fn stop(&self);
}

enum Task {
    Compute(usize, TupleBuffer, Arc<Node>),
}

pub struct Query {
    sources: Vec<Arc<Mutex<SourceNode>>>,
}

impl Query {
    pub fn new(sources: Vec<SourceNode>) -> Self {
        let sources = sources
            .into_iter()
            .map(|node| Arc::new(Mutex::new(node)))
            .collect();
        Self { sources }
    }
}

pub struct QueryEngine {
    id_counter: AtomicUsize,
    queue: Arc<Queue>,
    queries: Mutex<HashMap<usize, Query>>,
    worker_span: tracing::Span,
}

impl Default for QueryEngine {
    fn default() -> Self {
        QueryEngine {
            queue: Arc::new(crossbeam_queue::ArrayQueue::new(1024)),
            queries: Mutex::default(),
            id_counter: AtomicUsize::default(),
            worker_span: tracing::info_span!("Worker", worker_id = "unknown"),
        }
    }
}

struct PEC<'a> {
    #[allow(dead_code)]
    query_id: usize,
    #[allow(dead_code)]
    queue: &'a Queue,
    #[allow(dead_code)]
    successor: &'a Option<Arc<Node>>,
}

impl PipelineContext for PEC<'_> {
    #[allow(dead_code)]
    fn emit(&mut self, data: TupleBuffer) {
        if let Some(successor) = self.successor {
            let _ = self.queue
                .push(Task::Compute(self.query_id, data, successor.clone()));
        }
    }
}

impl QueryEngine {
    pub(crate) fn start(worker_id: String) -> Arc<QueryEngine> {
        let mut engine = QueryEngine::default();
        engine.worker_span = tracing::info_span!("Worker", worker_id = worker_id.as_str());
        let engine = Arc::new(engine);
        let pool = ThreadPool::with_name("engine".to_string(), 2);

        pool.execute({
            let engine = engine.clone();
            move || loop {
                if let Some(task) = engine.queue.pop() {
                    match task {
                        Task::Compute(query_id, input, node) => {
                            let mut pec = PEC {
                                query_id,
                                queue: &engine.queue,
                                successor: &node.successor,
                            };
                            let span = tracing::info_span!(parent: &engine.worker_span, "Task", query_id = query_id);
                            {
                                let _enter = span.enter();
                                node.pipeline.execute(&input, &mut pec);
                            }
                        }
                    }
                }
            }
        });

        engine
    }

    pub fn stop_query(self: &Arc<Self>, id: usize) {
        let _enter = self.worker_span.enter();
        if let Some(query) = self.queries.lock().unwrap().remove(&id) {
            for source in &query.sources {
                source.lock().unwrap().stop();
            }
            info!("Stopped Query with Id {id}");
        } else {
            error!("Query with id {id} does not exist!");
        }
    }
    pub fn start_query(self: &Arc<Self>, query: Query) -> usize {
        let _enter = self.worker_span.enter();
        let id = self.id_counter.fetch_add(1, Relaxed);
        for source in &query.sources {
            source.lock().unwrap().start(self.queue.clone(), id);
        }
        self.queries.lock().unwrap().insert(id, query);
        info!("Started Query with id {id}");
        id
    }
}
