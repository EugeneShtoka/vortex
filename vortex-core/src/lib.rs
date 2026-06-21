pub mod event;
pub use event::Event;

use std::time::SystemTime;

use serde::{Deserialize, Serialize};

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Running,
    Success,
    Failed,
    Skipped,
}

impl TaskStatus {
    pub fn is_running(&self)  -> bool { matches!(self, Self::Running)  }
    pub fn is_success(&self)  -> bool { matches!(self, Self::Success)  }
    pub fn is_failed(&self)   -> bool { matches!(self, Self::Failed)   }
    pub fn is_skipped(&self)  -> bool { matches!(self, Self::Skipped)  }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Success => "success",
            Self::Failed  => "failed",
            Self::Skipped => "skipped",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TriggerStatus {
    Received,
    Accepted,
    Rejected,
    Running,
    Finished,
}

impl TriggerStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Received => "received",
            Self::Accepted => "accepted",
            Self::Rejected => "rejected",
            Self::Running  => "running",
            Self::Finished => "finished",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn task_status_round_trips_json() {
        for (status, expected) in [
            (TaskStatus::Running, "\"running\""),
            (TaskStatus::Success, "\"success\""),
            (TaskStatus::Failed,  "\"failed\""),
            (TaskStatus::Skipped, "\"skipped\""),
        ] {
            let json = serde_json::to_string(&status).unwrap();
            assert_eq!(json, expected);
            let back: TaskStatus = serde_json::from_str(&json).unwrap();
            assert_eq!(back, status);
        }
    }

    #[test]
    fn trigger_status_round_trips_json() {
        for (status, expected) in [
            (TriggerStatus::Received, "\"received\""),
            (TriggerStatus::Accepted, "\"accepted\""),
            (TriggerStatus::Rejected, "\"rejected\""),
            (TriggerStatus::Running,  "\"running\""),
            (TriggerStatus::Finished, "\"finished\""),
        ] {
            let json = serde_json::to_string(&status).unwrap();
            assert_eq!(json, expected);
            let back: TriggerStatus = serde_json::from_str(&json).unwrap();
            assert_eq!(back, status);
        }
    }
}
