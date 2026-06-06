use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use cron::Schedule;
use tokio::sync::broadcast;
use tracing::error;
use uuid::Uuid;

use crate::config::{Config, WorkflowConfig};
use crate::engine::Engine;
use crate::event::Event;

pub async fn run(config: Arc<Config>, event_tx: broadcast::Sender<Event>) {
    for (name, wf) in &config.workflows {
        let Some(expr) = wf.cron.clone() else { continue };
        let (name, wf, tx, db) = (
            name.clone(),
            wf.clone(),
            event_tx.clone(),
            config.server.db_path.clone(),
        );
        tokio::spawn(async move {
            if let Err(e) = run_schedule(name.clone(), wf, expr, db, tx).await {
                error!(workflow = %name, "Scheduler stopped: {e:#}");
            }
        });
    }
}

async fn run_schedule(
    name: String,
    wf: WorkflowConfig,
    expr: String,
    db_path: String,
    event_tx: broadcast::Sender<Event>,
) -> Result<()> {
    let schedule = parse_schedule(&expr)?;
    loop {
        sleep_until_next(&schedule).await?;
        tokio::spawn(fire_workflow(name.clone(), wf.clone(), db_path.clone(), event_tx.clone()));
    }
}

async fn fire_workflow(
    name: String,
    wf: WorkflowConfig,
    db_path: String,
    event_tx: broadcast::Sender<Event>,
) {
    let run_id = Uuid::new_v4().to_string();
    let _ = event_tx.send(Event::TriggerReceived {
        run_id: run_id.clone(),
        workflow: name.clone(),
        params: HashMap::new(),
    });
    let _ = event_tx.send(Event::TriggerAccepted {
        run_id: run_id.clone(),
        workflow: name.clone(),
        params: HashMap::new(),
    });
    let engine = Engine::new(wf, &db_path)
        .with_events(event_tx)
        .with_run_id(run_id);
    if let Err(e) = engine.run(&name).await {
        error!(workflow = %name, "Cron run failed: {e:#}");
    }
}

async fn sleep_until_next(schedule: &Schedule) -> Result<()> {
    let next = schedule
        .upcoming(Utc)
        .next()
        .context("Schedule has no upcoming runs")?;
    let delay = (next - Utc::now()).to_std().unwrap_or_default();
    tokio::time::sleep(delay).await;
    Ok(())
}

fn parse_schedule(expr: &str) -> Result<Schedule> {
    let normalized = normalize_cron(expr);
    Schedule::from_str(&normalized)
        .map_err(|e| anyhow::anyhow!("Invalid cron expression '{expr}': {e}"))
}

pub fn normalize_cron(expr: &str) -> String {
    if expr.split_whitespace().count() == 5 {
        format!("0 {expr}")
    } else {
        expr.to_string()
    }
}

pub fn next_run_after(expr: &str, after: DateTime<Utc>) -> Result<DateTime<Utc>> {
    let schedule = parse_schedule(expr)?;
    schedule
        .after(&after)
        .next()
        .context("Schedule has no upcoming runs after the given time")
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Timelike};

    #[test]
    fn normalize_5_field_prepends_seconds() {
        assert_eq!(normalize_cron("0 2 * * *"), "0 0 2 * * *");
        assert_eq!(normalize_cron("*/5 * * * *"), "0 */5 * * * *");
    }

    #[test]
    fn normalize_6_field_unchanged() {
        assert_eq!(normalize_cron("0 0 2 * * *"), "0 0 2 * * *");
    }

    #[test]
    fn next_run_after_returns_future_time_at_correct_hour() {
        let midnight = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let next = next_run_after("0 2 * * *", midnight).unwrap();
        assert!(next > midnight);
        assert_eq!(next.hour(), 2);
        assert_eq!(next.minute(), 0);
        assert_eq!(next.second(), 0);
    }

    #[test]
    fn next_run_every_5_minutes_is_within_5_minutes() {
        let now = Utc.with_ymd_and_hms(2024, 1, 1, 12, 0, 0).unwrap();
        let next = next_run_after("*/5 * * * *", now).unwrap();
        let diff_secs = (next - now).num_seconds();
        assert!(diff_secs > 0 && diff_secs <= 300);
    }

    #[test]
    fn invalid_cron_returns_error() {
        assert!(next_run_after("not valid", chrono::Utc::now()).is_err());
    }
}
