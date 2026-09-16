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

use model::query::query_state::QueryState;
use model::request::Wait;
use serde::Deserialize;
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WaitKind {
    None,
    Started,
    Running,
    Completed,
    Terminated,
}

#[derive(Debug, Clone, Copy, Default, Deserialize)]
pub struct WaitParams {
    pub wait: Option<WaitKind>,
    pub timeout_ms: Option<u64>,
}

impl WaitParams {
    #[must_use]
    pub fn resolve(self, cap: Duration) -> Wait {
        let timeout = Some(
            self.timeout_ms
                .map_or(cap, |ms| Duration::from_millis(ms).min(cap)),
        );
        match self.wait.unwrap_or(WaitKind::None) {
            WaitKind::None => Wait::None,
            WaitKind::Started => Wait::UntilState {
                state: QueryState::Started,
                timeout,
            },
            WaitKind::Running => Wait::UntilState {
                state: QueryState::Running,
                timeout,
            },
            WaitKind::Completed => Wait::UntilState {
                state: QueryState::Completed,
                timeout,
            },
            WaitKind::Terminated => Wait::UntilTerminated { timeout },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const CAP: Duration = Duration::from_secs(30);

    fn params(query: &str) -> WaitParams {
        serde_urlencoded_from(query)
    }

    fn serde_urlencoded_from(query: &str) -> WaitParams {
        axum::extract::Query::<WaitParams>::try_from_uri(&format!("/?{query}").parse().unwrap())
            .unwrap()
            .0
    }

    #[test]
    fn no_wait_ignores_the_timeout() {
        assert!(matches!(params("").resolve(CAP), Wait::None));
        assert!(matches!(
            params("wait=none&timeout_ms=5").resolve(CAP),
            Wait::None
        ));
    }

    #[test]
    fn each_state_maps_to_a_state_wait() {
        for (kind, state) in [
            ("started", QueryState::Started),
            ("running", QueryState::Running),
            ("completed", QueryState::Completed),
        ] {
            assert!(matches!(
                params(&format!("wait={kind}")).resolve(CAP),
                Wait::UntilState { state: s, timeout: Some(t) } if s == state && t == CAP
            ));
        }
        assert!(matches!(
            params("wait=terminated").resolve(CAP),
            Wait::UntilTerminated { timeout: Some(t) } if t == CAP
        ));
    }

    #[test]
    fn the_timeout_is_cut_to_the_cap() {
        assert!(matches!(
            params("wait=running&timeout_ms=100").resolve(CAP),
            Wait::UntilState { timeout: Some(t), .. } if t == Duration::from_millis(100)
        ));
        assert!(matches!(
            params("wait=running&timeout_ms=999999").resolve(CAP),
            Wait::UntilState { timeout: Some(t), .. } if t == CAP
        ));
    }

    #[test]
    fn an_unknown_kind_or_timeout_does_not_parse() {
        let bad = |query: &str| {
            axum::extract::Query::<WaitParams>::try_from_uri(&format!("/?{query}").parse().unwrap())
                .is_err()
        };
        assert!(bad("wait=soon"));
        assert!(bad("timeout_ms=abc"));
    }
}
