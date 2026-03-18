use crate::query::context::QueryContext;
use crate::worker::worker_task::{FragmentError as ProtoError, QueryStatusReply};
use anyhow::Result;
use common::error::Retryable;
use model::Set;
use model::query::fragment::{self, FragmentError, FragmentState};
use model::query::query_state::QueryState;
use std::time::Duration;
use tracing::{debug, info, warn};

const QUERY_POLL_INTERVAL: Duration = Duration::from_secs(5);

impl From<&ProtoError> for FragmentError {
    fn from(err: &ProtoError) -> Self {
        FragmentError::WorkerInternal {
            code: err.code,
            msg: err.message.clone(),
            trace: err.stack_trace.clone(),
        }
    }
}

fn unix_ms_to_datetime(ms: u64) -> chrono::DateTime<chrono::Local> {
    chrono::DateTime::from_timestamp_millis(ms as i64)
        .unwrap_or_default()
        .with_timezone(&chrono::Local)
}

fn apply_status_reply(
    fragment: &fragment::Model,
    reply: QueryStatusReply,
) -> Option<fragment::ActiveModel> {
    let state = match FragmentState::try_from(reply.state) {
        Ok(state) => state,
        Err(tag) => {
            warn!("fragment {} returned unknown state tag {tag}", fragment.id);
            return None;
        }
    };
    let start_timestamp = reply
        .metrics
        .as_ref()
        .and_then(|m| m.start_unix_time_in_ms)
        .map(unix_ms_to_datetime);
    let stop_timestamp = reply
        .metrics
        .as_ref()
        .and_then(|m| m.stop_unix_time_in_ms)
        .map(unix_ms_to_datetime);
    let error = if state == FragmentState::Failed {
        let err = reply
            .metrics
            .as_ref()
            .and_then(|m| m.error.as_ref())
            .map(FragmentError::from)
            .unwrap_or_else(|| FragmentError::WorkerCommunication {
                msg: "fragment failed without error details".to_string(),
            });
        warn!(fragment_id = fragment.id, %err, "fragment failed");
        Some(err)
    } else {
        None
    };

    let mut am: fragment::ActiveModel = fragment.clone().into();
    am.current_state = Set(state);
    if let Some(ts) = start_timestamp {
        am.start_timestamp = Set(Some(ts));
    }
    if let Some(ts) = stop_timestamp {
        am.stop_timestamp = Set(Some(ts));
    }
    if let Some(err) = error {
        am.error = Set(Some(err));
    }
    Some(am)
}

pub(crate) async fn poll_until_complete(ctx: &mut QueryContext) -> Result<()> {
    loop {
        if let Err(e) = ctx
            .apply_rpc_results(
                ctx.poll_fragment_status().await,
                |fragment, result| match result {
                    Ok(reply) => apply_status_reply(fragment, reply),
                    Err(e) if e.retryable() => {
                        debug!("failed to poll fragment {} status: {e}", fragment.id);
                        None
                    }
                    Err(e) if e.fragment_not_found() => {
                        debug!(fragment_id = fragment.id, "fragment not found on worker, resetting for re-registration");
                        let mut am: fragment::ActiveModel = fragment.clone().into();
                        am.current_state = Set(FragmentState::Pending);
                        Some(am)
                    }
                    Err(e) => {
                        let error = FragmentError::from(e);
                        warn!(fragment_id = fragment.id, %error, "permanent failure of fragment");
                        let mut am: fragment::ActiveModel = fragment.clone().into();
                        am.current_state = Set(FragmentState::Failed);
                        am.error = Set(Some(error));
                        Some(am)
                    }
                },
            )
            .await
        {
            if ctx.query.current_state == QueryState::Failed {
                return Err(e);
            }
            warn!("poll cycle failed: {e:#}");
        }

        if ctx.query.current_state != QueryState::Running {
            if ctx.query.current_state.is_terminal() {
                info!("query reached terminal state: {}", ctx.query.current_state);
            }
            return Ok(());
        }

        tokio::time::sleep(QUERY_POLL_INTERVAL).await;
    }
}
