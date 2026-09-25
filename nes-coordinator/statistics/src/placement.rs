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
//! Finds where a deployed query placed its statistic store writer.
//!
//! A worker's statistic store is its own, so a statistic can only be read back on the worker its writer ran on.
//! Which worker that is, is up to the planner. The service learns it from the fragments the coordinator deployed.

use crate::service::Fragment;
use model::worker::endpoint::NetworkAddr;

const WRITER_OPERATOR: &str = "StatisticStoreWriter";

/// The part of a serialized fragment plan this module reads: the operators, each reflected as a JSON document.
#[derive(Clone, PartialEq, prost::Message)]
struct SerializedPlan {
    #[prost(string, repeated, tag = "1")]
    reflected_operators: Vec<String>,
}

/// The worker of the fragment that writes `statistic_id` into its statistic store, if any does.
pub fn writer_worker(fragments: &[Fragment], statistic_id: u64) -> Option<NetworkAddr> {
    fragments
        .iter()
        .find(|fragment| writes_statistic(&fragment.plan, statistic_id))
        .map(|fragment| fragment.worker.clone())
}

fn writes_statistic(plan: &[u8], statistic_id: u64) -> bool {
    let Ok(plan) = <SerializedPlan as prost::Message>::decode(plan) else {
        tracing::warn!("a deployed fragment plan could not be decoded");
        return false;
    };
    plan.reflected_operators.iter().any(|reflected| {
        let Ok(operator) = serde_json::from_str::<serde_json::Value>(reflected) else {
            return false;
        };
        operator["type"] == WRITER_OPERATOR
            && operator["config"]["statisticId"].as_u64() == Some(statistic_id)
    })
}

/// A fragment on `worker` whose plan holds the given reflected operators.
#[cfg(test)]
pub(crate) fn fragment(worker: &str, operators: &[&str]) -> Fragment {
    let plan = SerializedPlan {
        reflected_operators: operators.iter().map(ToString::to_string).collect(),
    };
    Fragment {
        worker: worker.parse().unwrap(),
        plan: prost::Message::encode_to_vec(&plan),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Operators as the C++ side reflects them, trimmed to the fields that matter here.
    const WRITER: &str = r#"{"type": "StatisticStoreWriter", "operatorId": 147, "childrenIds": [146], "config": {"operatorId": 147, "statisticId": 7, "typeName": "Avg"}, "traitSet": {"traits": []}}"#;
    const AGGREGATION: &str = r#"{"type": "WindowedAggregation", "operatorId": 146, "childrenIds": [145], "config": {}, "traitSet": {"traits": []}}"#;

    #[test]
    fn the_writer_is_found_in_whichever_fragment_holds_it() {
        let fragments = [
            fragment("source-node:8080", &[AGGREGATION]),
            fragment("middle-node:8080", &[WRITER, AGGREGATION]),
            fragment("sink-node:8080", &[]),
        ];
        assert_eq!(
            writer_worker(&fragments, 7),
            Some("middle-node:8080".parse().unwrap())
        );
    }

    #[test]
    fn a_writer_of_another_statistic_does_not_count() {
        let fragments = [fragment("middle-node:8080", &[WRITER])];
        assert_eq!(writer_worker(&fragments, 8), None);
    }

    #[test]
    fn an_undecodable_plan_holds_no_writer() {
        let fragments = [Fragment {
            worker: "middle-node:8080".parse().unwrap(),
            plan: vec![0xff, 0xff, 0xff],
        }];
        assert_eq!(writer_worker(&fragments, 7), None);
    }
}
