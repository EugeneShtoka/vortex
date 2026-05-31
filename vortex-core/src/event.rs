use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Telemetry events broadcast to all connected WebSocket observers.
///
/// Lifecycle order for a successful run:
/// `TriggerReceived` → `TriggerAccepted` → `WorkflowStarted`
///   → (`TaskStarted` → `TaskFinished` | `TaskSkipped`) × N
///   → `WorkflowFinished`
///
/// On rejection: `TriggerReceived` → `TriggerRejected`
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Event {
    /// A well-formed trigger request arrived (pre-auth).
    TriggerReceived { run_id: String, workflow: String, params: HashMap<String, String> },
    /// Auth passed and the workflow was found; engine is being dispatched.
    TriggerAccepted { run_id: String, workflow: String, params: HashMap<String, String> },
    /// The trigger was rejected. `reason`: `"unauthorized"` | `"unknown_workflow"`.
    TriggerRejected { run_id: String, reason: String },

    /// Engine started executing the workflow's task graph.
    WorkflowStarted  { run_id: String, workflow: String, #[serde(default)] timestamp: u64 },
    TaskStarted      { run_id: String, task: String,     #[serde(default)] timestamp: u64 },
    TaskSkipped      { run_id: String, task: String,     #[serde(default)] timestamp: u64 },
    TaskFinished     { run_id: String, task: String, success: bool, exit_code: i32, stdout: String, stderr: String, #[serde(default)] timestamp: u64 },
    /// All tasks finished (or were skipped). `success` is false if any task failed.
    WorkflowFinished { run_id: String, workflow: String, success: bool, #[serde(default)] timestamp: u64 },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(e: &Event) -> Event {
        serde_json::from_str(&serde_json::to_string(e).unwrap()).unwrap()
    }

    #[test]
    fn trigger_received_has_correct_type_tag() {
        let e = Event::TriggerReceived { run_id: "r1".into(), workflow: "deploy".into(), params: HashMap::new() };
        let json = serde_json::to_string(&e).unwrap();
        assert!(json.contains(r#""type":"trigger_received""#));
        assert!(json.contains(r#""workflow":"deploy""#));
    }

    #[test]
    fn trigger_received_serialises_params() {
        let mut p = HashMap::new();
        p.insert("msg".into(), "hello".into());
        let e = Event::TriggerReceived { run_id: "r1".into(), workflow: "w".into(), params: p };
        let json = serde_json::to_string(&e).unwrap();
        assert!(json.contains(r#""msg":"hello""#));
    }

    #[test]
    fn trigger_rejected_has_reason() {
        let e = Event::TriggerRejected { run_id: "r1".into(), reason: "unauthorized".into() };
        let json = serde_json::to_string(&e).unwrap();
        assert!(json.contains(r#""type":"trigger_rejected""#));
        assert!(json.contains(r#""reason":"unauthorized""#));
    }

    #[test]
    fn task_finished_serialises_all_fields() {
        let e = Event::TaskFinished {
            run_id: "r1".into(),
            task: "build".into(),
            success: false,
            exit_code: 2,
            stdout: "output".into(),
            stderr: "error".into(),
            timestamp: 0,
        };
        let json = serde_json::to_string(&e).unwrap();
        assert!(json.contains(r#""type":"task_finished""#));
        assert!(json.contains(r#""success":false"#));
        assert!(json.contains(r#""exit_code":2"#));
        assert!(json.contains(r#""stdout":"output""#));
        assert!(json.contains(r#""stderr":"error""#));
    }

    #[test]
    fn all_variants_round_trip() {
        let events = vec![
            Event::TriggerReceived  { run_id: "r".into(), workflow: "w".into(), params: HashMap::new() },
            Event::TriggerAccepted  { run_id: "r".into(), workflow: "w".into(), params: HashMap::new() },
            Event::TriggerRejected  { run_id: "r".into(), reason: "unauthorized".into() },
            Event::WorkflowStarted  { run_id: "r".into(), workflow: "w".into(), timestamp: 0 },
            Event::TaskStarted      { run_id: "r".into(), task: "t".into(), timestamp: 0 },
            Event::TaskSkipped      { run_id: "r".into(), task: "t".into(), timestamp: 0 },
            Event::TaskFinished     { run_id: "r".into(), task: "t".into(), success: true, exit_code: 0, stdout: String::new(), stderr: String::new(), timestamp: 0 },
            Event::WorkflowFinished { run_id: "r".into(), workflow: "w".into(), success: true, timestamp: 0 },
        ];
        for e in &events {
            assert_eq!(&round_trip(e), e);
        }
    }

    #[test]
    fn timestamp_serialised_in_workflow_started() {
        let e = Event::WorkflowStarted { run_id: "r".into(), workflow: "w".into(), timestamp: 1700000000000 };
        let json = serde_json::to_string(&e).unwrap();
        assert!(json.contains(r#""timestamp":1700000000000"#));
    }

    #[test]
    fn timestamp_defaults_to_zero_when_missing_from_json() {
        let json = r#"{"type":"workflow_started","run_id":"r","workflow":"w"}"#;
        let e: Event = serde_json::from_str(json).unwrap();
        assert!(matches!(e, Event::WorkflowStarted { timestamp: 0, .. }));
    }

    #[test]
    fn task_finished_includes_timestamp() {
        let e = Event::TaskFinished {
            run_id: "r".into(), task: "t".into(),
            success: true, exit_code: 0,
            stdout: String::new(), stderr: String::new(),
            timestamp: 42,
        };
        let json = serde_json::to_string(&e).unwrap();
        assert!(json.contains(r#""timestamp":42"#));
    }
}
