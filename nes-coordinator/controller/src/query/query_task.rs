use crate::query::context::QueryContext;
use crate::query::running;
use crate::worker::worker_task::{RegisterFragmentRequest, Rpc, StartFragmentRequest};
use anyhow::Result;
use model::query::StopMode;
use model::query::fragment::FragmentState;
use model::query::query_state::QueryState;
use std::time::Duration;
use tokio::select;
use tokio_retry::strategy::{ExponentialBackoff, jitter};
use tracing::{debug, info, info_span};

const TRANSITION_INITIAL_BACKOFF_MS: u64 = 50;
const TRANSITION_BACKOFF_FACTOR: u64 = 10;
const TRANSITION_MAX_DELAY: Duration = Duration::from_secs(30);

fn transition_retry_strategy() -> impl Iterator<Item = Duration> {
    ExponentialBackoff::from_millis(TRANSITION_INITIAL_BACKOFF_MS)
        .factor(TRANSITION_BACKOFF_FACTOR)
        .max_delay(TRANSITION_MAX_DELAY)
        .map(jitter)
}

async fn transition(ctx: &mut QueryContext) -> Result<()> {
    match ctx.query.current_state {
        QueryState::Pending => {
            ctx.transition_fragments(
                FragmentState::Pending,
                |id| {
                    let (rx, req) = RegisterFragmentRequest::new(id);
                    (rx, Rpc::RegisterFragment(req))
                },
                FragmentState::Registered,
            )
            .await
        }
        QueryState::Registered => {
            ctx.transition_fragments(
                FragmentState::Registered,
                |id| {
                    let (rx, req) = StartFragmentRequest::new(id);
                    (rx, Rpc::StartFragment(req))
                },
                FragmentState::Started,
            )
            .await
        }
        QueryState::Running => running::poll_until_complete(ctx).await,
        _ => unreachable!(),
    }
}

async fn try_transition(
    ctx: &mut QueryContext,
    stop_rx: &mut flume::Receiver<StopMode>,
    span: &tracing::Span,
) -> bool {
    let mut backoff = transition_retry_strategy();

    loop {
        let from_state = ctx.query.current_state;
        select! {
            transition_result = transition(ctx) => {
                let _guard = span.enter();
                match transition_result {
                    Ok(()) => {
                        debug!(from = %from_state, to = %ctx.query.current_state, "transition succeeded");
                        return !ctx.query.current_state.is_terminal();
                    }
                    Err(e) => {
                        debug!(from = %from_state, to = %ctx.query.current_state, "transition failed: {e:#}");
                        drop(_guard);

                        if ctx.query.current_state.is_terminal() {
                            ctx.rollback_fragments(StopMode::Forceful).await;
                            return false;
                        }

                        match backoff.next() {
                            Some(delay) => {
                                tokio::time::sleep(delay).await;
                            }
                            None => return false,
                        }
                    }
                }
            },
            Ok(mode) = stop_rx.recv_async() => {
                let _guard = span.enter();
                info!(?mode, "stopping query");
                drop(_guard);
                return if from_state == QueryState::Running && mode == StopMode::Graceful {
                    ctx.stop_source_fragments().await;
                    !ctx.query.current_state.is_terminal()
                } else {
                    ctx.rollback_fragments(StopMode::Forceful).await;
                    false
                }
            }
        }
    }
}

pub(crate) async fn run(mut ctx: QueryContext, mut stop_rx: flume::Receiver<StopMode>) {
    let span = info_span!(
        "query",
        id = ctx.query.id,
        name = %ctx.query.name,
    );
    {
        let _guard = span.enter();
        info!("starting reconciliation");
    }
    ctx.fragments = ctx.catalog.query.get_fragments(ctx.query.id).await.unwrap();
    while try_transition(&mut ctx, &mut stop_rx, &span).await {}
    {
        let _guard = span.enter();
        info!("reconciliation finished: {}", ctx.query.current_state);
    }
}
