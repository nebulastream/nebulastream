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

use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use model::error::{CodedError, ErrorCode};
use model::statement::StatementResult;
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct ErrorBody {
    pub code: u16,
    pub error: String,
    pub message: String,
}

#[derive(Debug)]
pub struct ApiError {
    pub status: StatusCode,
    pub body: ErrorBody,
    retry_after: Option<u32>,
}

impl ApiError {
    #[must_use]
    pub fn new(status: StatusCode, code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            status,
            body: ErrorBody {
                code: code as u16,
                error: format!("{code:?}"),
                message: message.into(),
            },
            retry_after: None,
        }
    }

    #[must_use]
    pub fn bad_request(message: impl Into<String>) -> Self {
        Self::new(
            StatusCode::BAD_REQUEST,
            ErrorCode::InvalidStatement,
            message,
        )
    }

    #[must_use]
    pub fn not_found(code: ErrorCode, message: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, code, message)
    }

    #[must_use]
    pub fn overloaded() -> Self {
        let mut error = Self::unavailable("the request queue is full");
        error.retry_after = Some(1);
        error
    }

    #[must_use]
    pub fn unavailable(message: impl Into<String>) -> Self {
        Self::new(
            StatusCode::SERVICE_UNAVAILABLE,
            ErrorCode::CatalogUnavailable,
            message,
        )
    }

    #[must_use]
    pub fn unexpected(result: &StatementResult) -> Self {
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            ErrorCode::UnknownException,
            format!("unexpected coordinator reply: {result:?}"),
        )
    }

    #[must_use]
    pub const fn retry_after(&self) -> Option<u32> {
        self.retry_after
    }
}

#[must_use]
pub fn status_for(code: ErrorCode) -> StatusCode {
    match code {
        ErrorCode::UnknownWorker
        | ErrorCode::UnknownSourceName
        | ErrorCode::UnknownSinkName
        | ErrorCode::UnknownModelName
        | ErrorCode::QueryNotFound => StatusCode::NOT_FOUND,
        ErrorCode::WorkerAlreadyExists
        | ErrorCode::SourceAlreadyExists
        | ErrorCode::SinkAlreadyExists
        | ErrorCode::QueryAlreadyRegistered
        | ErrorCode::ModelAlreadyExists => StatusCode::CONFLICT,
        ErrorCode::PlacementFailure
        | ErrorCode::InvalidTopology
        | ErrorCode::CatalogWriteRejected => StatusCode::UNPROCESSABLE_ENTITY,
        ErrorCode::WorkerUnreachable => StatusCode::BAD_GATEWAY,
        ErrorCode::CatalogUnavailable => StatusCode::SERVICE_UNAVAILABLE,
        ErrorCode::QueryWaitTimeout => StatusCode::GATEWAY_TIMEOUT,
        code if (2000..2300).contains(&(code as u16)) => StatusCode::BAD_REQUEST,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

impl From<anyhow::Error> for ApiError {
    fn from(error: anyhow::Error) -> Self {
        match error.downcast_ref::<CodedError>() {
            Some(coded) => Self::new(status_for(coded.code), coded.code, format!("{error:#}")),
            None => Self::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                ErrorCode::UnknownException,
                format!("{error:#}"),
            ),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let mut response = (self.status, axum::Json(self.body)).into_response();
        if let Some(seconds) = self.retry_after {
            response
                .headers_mut()
                .insert(header::RETRY_AFTER, HeaderValue::from(seconds));
        }
        response
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codes_map_to_the_statuses_a_client_expects() {
        assert_eq!(status_for(ErrorCode::UnknownWorker), StatusCode::NOT_FOUND);
        assert_eq!(status_for(ErrorCode::QueryNotFound), StatusCode::NOT_FOUND);
        assert_eq!(
            status_for(ErrorCode::SourceAlreadyExists),
            StatusCode::CONFLICT
        );
        assert_eq!(
            status_for(ErrorCode::PlacementFailure),
            StatusCode::UNPROCESSABLE_ENTITY
        );
        assert_eq!(
            status_for(ErrorCode::WorkerUnreachable),
            StatusCode::BAD_GATEWAY
        );
        assert_eq!(
            status_for(ErrorCode::CatalogUnavailable),
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            status_for(ErrorCode::QueryWaitTimeout),
            StatusCode::GATEWAY_TIMEOUT
        );
        assert_eq!(
            status_for(ErrorCode::InvalidQuerySyntax),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            status_for(ErrorCode::UnknownException),
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }

    #[test]
    fn a_coded_cause_keeps_its_code_under_added_context() {
        let error = anyhow::Error::new(CodedError::new(ErrorCode::SinkAlreadyExists, "sink taken"))
            .context("failed to create sink");
        let api = ApiError::from(error);
        assert_eq!(api.status, StatusCode::CONFLICT);
        assert_eq!(api.body.code, 2034);
        assert_eq!(api.body.error, "SinkAlreadyExists");
        assert_eq!(api.body.message, "failed to create sink: sink taken");
    }

    #[test]
    fn an_unclassified_failure_reports_the_unknown_code() {
        let api = ApiError::from(anyhow::anyhow!("boom"));
        assert_eq!(api.status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(api.body.code, 9999);
        assert_eq!(api.body.error, "UnknownException");
    }

    #[test]
    fn an_overloaded_answer_carries_a_retry_hint() {
        let response = ApiError::overloaded().into_response();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            response.headers().get(header::RETRY_AFTER).unwrap(),
            HeaderValue::from(1u32)
        );
        assert!(
            ApiError::unavailable("gone")
                .into_response()
                .headers()
                .get(header::RETRY_AFTER)
                .is_none()
        );
    }
}
