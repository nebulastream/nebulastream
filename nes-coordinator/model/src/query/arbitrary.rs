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

use super::query_fragment::{CreateQueryFragment, QueryFragmentsWithRefs};
use super::*;
use crate::ConnectorKind;
use crate::source::logical::CreateLogicalSource;
use crate::worker::CreateWorker;
use crate::worker::endpoint::NetworkAddr;
use proptest::prelude::*;
use proptest::strategy::BoxedStrategy;

/// Test fixture bundling a `CreateQuery` with every catalog row it
/// depends on (workers, logical source, physical sources, sink). The
/// `setup` helpers insert them in dependency order.
#[derive(Debug, Clone)]
pub struct CreateQueryWithRefs {
    pub workers: Vec<CreateWorker>,
    pub logical_source: CreateLogicalSource,
    pub physical_sources: Vec<SourceFactory>,
    pub sink: SinkFactory,
    pub query: CreateQuery,
}

impl Arbitrary for CreateQueryWithRefs {
    type Parameters = ();
    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        let source_name = "[a-z][a-z0-9_]{2,29}";
        let sink_name = "[a-z][a-z0-9_]{2,29}";
        let query_name = proptest::option::of("[a-z][a-z0-9_]{2,29}");
        let statement = (source_name, sink_name)
            .prop_map(|(source, sink)| format!("SELECT * FROM {} INTO {};", source, sink));
        let kind = prop_oneof![
            Just(ConnectorKind::Shared),
            Just(ConnectorKind::Anonymous),
            Just(ConnectorKind::Internal),
        ];

        any::<QueryFragmentsWithRefs>()
            .prop_flat_map(move |fragments| {
                let n = fragments.fragments.len();
                (
                    Just(fragments),
                    source_name,
                    "[A-Za-z]{1,10}",
                    sink_name,
                    "[A-Za-z]{1,10}",
                    statement.clone(),
                    0..n,
                    kind.clone(),
                    query_name.clone(),
                )
            })
            .prop_map(
                |(
                    fragments,
                    source_name,
                    source_type,
                    sink_name,
                    sink_type,
                    statement,
                    sink_frag_idx,
                    kind,
                    query_name,
                )| {
                    let logical_source = CreateLogicalSource {
                        name: source_name.clone(),
                        schema: Default::default(),
                        if_not_exists: false,
                    };

                    let sink_host = fragments.fragments[sink_frag_idx].host_addr.clone();
                    let physical_sources = build_physical_sources(
                        &fragments.fragments,
                        kind,
                        &source_name,
                        &source_type,
                    );
                    let sink = build_sink(kind, sink_name, sink_host, sink_type);

                    let mut query = CreateQuery::new(statement).with_fragments(fragments.fragments);
                    if let Some(name) = query_name {
                        query = query.with_name(name);
                    }
                    CreateQueryWithRefs {
                        workers: fragments.workers,
                        logical_source,
                        physical_sources,
                        sink,
                        query,
                    }
                },
            )
            .boxed()
    }

    type Strategy = BoxedStrategy<Self>;
}

/// Builds a physical source factory for every source-bearing fragment,
/// as either a shared or an inline/internal connector per `kind`.
fn build_physical_sources(
    fragments: &[CreateQueryFragment],
    kind: ConnectorKind,
    logical_source: &str,
    source_type: &str,
) -> Vec<SourceFactory> {
    fragments
        .iter()
        .filter(|fragment| fragment.has_source)
        .map(|fragment| match kind {
            ConnectorKind::Shared => SourceFactory::Shared(CreatePhysicalSource {
                logical_source: logical_source.to_string(),
                host_addr: fragment.host_addr.clone(),
                source_type: source_type.to_string(),
                source_config: Default::default(),
                parser_config: Default::default(),
                if_not_exists: false,
            }),
            ConnectorKind::Anonymous | ConnectorKind::Internal => {
                SourceFactory::Anonymous(CreateAnonymousSource {
                    source_type: source_type.to_string(),
                    source_config: Default::default(),
                    parser_config: Default::default(),
                    host_addr: fragment.host_addr.clone(),
                    internal: kind == ConnectorKind::Internal,
                })
            }
        })
        .collect()
}

/// Builds a sink factory as either a shared or an inline/internal
/// connector per `kind`.
fn build_sink(
    kind: ConnectorKind,
    name: String,
    host_addr: NetworkAddr,
    sink_type: String,
) -> SinkFactory {
    match kind {
        ConnectorKind::Shared => SinkFactory::Shared(CreateSink {
            name,
            host_addr,
            sink_type,
            schema: Default::default(),
            config: Default::default(),
            if_not_exists: false,
        }),
        ConnectorKind::Anonymous | ConnectorKind::Internal => {
            SinkFactory::Anonymous(CreateAnonymousSink {
                sink_type,
                schema: Default::default(),
                config: Default::default(),
                host_addr,
                internal: kind == ConnectorKind::Internal,
            })
        }
    }
}
