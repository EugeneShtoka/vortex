use std::collections::HashMap;

use anyhow::Result;
use rusqlite::{params, Connection};
use vortex_core::{TaskStatus, TriggerStatus};

/// Persistent key-value store backed by SQLite.
/// Pass `":memory:"` as `path` for ephemeral in-process storage (tests, dry-runs).
pub struct Store {
    pub(crate) conn: Connection,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RunRow {
    pub id: String,
    pub workflow: String,
    pub status: String,      // "running"|"success"|"failed"
    pub params: String,      // JSON object
    pub started_at: u64,
    pub finished_at: Option<u64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TriggerRow {
    pub id: String,
    pub workflow: String,
    pub status: String,             // "received"|"accepted"|"rejected"|"running"|"finished"
    pub params: String,             // JSON object
    pub source: String,             // "http"|"uds"|"ntfy"|"cron"|"peer"
    pub rejection_cause: Option<String>,
    pub remote_addr: Option<String>,
    pub received_at: u64,
    pub finished_at: Option<u64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TaskRow {
    pub run_id: String,
    pub task_id: String,
    pub status: String,      // "running"|"success"|"failed"|"skipped"
    pub exit_code: Option<i32>,
    pub stdout: Option<String>,
    pub stderr: Option<String>,
    pub started_at: Option<u64>,
    pub finished_at: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct RunDetail {
    pub run: RunRow,
    pub tasks: Vec<TaskRow>,
}

#[derive(Debug, Clone)]
pub struct LogRow {
    pub run_id:    String,
    pub task_id:   String,
    pub stream:    String,
    pub line:      String,
    pub logged_at: u64,
}

impl Store {
    pub fn open(path: &str) -> Result<Self> {
        let conn = if path == ":memory:" {
            Connection::open_in_memory()?
        } else {
            Connection::open(path)?
        };
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS globals (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS runs (
                id           TEXT PRIMARY KEY,
                workflow     TEXT NOT NULL,
                status       TEXT NOT NULL,
                params       TEXT NOT NULL,
                started_at   INTEGER NOT NULL,
                finished_at  INTEGER
             );
             CREATE TABLE IF NOT EXISTS task_results (
                run_id      TEXT NOT NULL REFERENCES runs(id),
                task_id     TEXT NOT NULL,
                status      TEXT NOT NULL,
                exit_code   INTEGER,
                stdout      TEXT,
                stderr      TEXT,
                started_at  INTEGER,
                finished_at INTEGER,
                PRIMARY KEY (run_id, task_id)
             );
             CREATE TABLE IF NOT EXISTS triggers (
                id               TEXT PRIMARY KEY,
                workflow         TEXT NOT NULL,
                status           TEXT NOT NULL,
                params           TEXT NOT NULL,
                source           TEXT NOT NULL,
                rejection_cause  TEXT,
                remote_addr      TEXT,
                received_at      INTEGER NOT NULL,
                finished_at      INTEGER
             );
             CREATE TABLE IF NOT EXISTS task_logs (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id     TEXT    NOT NULL,
                task_id    TEXT    NOT NULL,
                stream     TEXT    NOT NULL,
                line       TEXT    NOT NULL,
                logged_at  INTEGER NOT NULL,
                expires_at INTEGER
             );
             CREATE INDEX IF NOT EXISTS task_logs_run ON task_logs(run_id, task_id);
             CREATE INDEX IF NOT EXISTS task_logs_expiry ON task_logs(expires_at);",
        )?;
        Ok(Self { conn })
    }

    // ── globals ───────────────────────────────────────────────────────────────

    pub fn get(&self, key: &str) -> Result<Option<String>> {
        let mut stmt = self.conn.prepare_cached("SELECT value FROM globals WHERE key = ?1")?;
        let mut rows = stmt.query(params![key])?;
        Ok(rows.next()?.map(|row| row.get(0)).transpose()?)
    }

    pub fn set(&self, key: &str, value: &str) -> Result<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO globals (key, value) VALUES (?1, ?2)",
            params![key, value],
        )?;
        Ok(())
    }

    pub fn get_all(&self) -> Result<HashMap<String, String>> {
        let mut stmt = self.conn.prepare_cached("SELECT key, value FROM globals")?;
        let rows = stmt.query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)))?;
        rows.map(|r| r.map_err(Into::into)).collect()
    }

    pub fn delete(&self, key: &str) -> Result<()> {
        self.conn.execute("DELETE FROM globals WHERE key = ?1", params![key])?;
        Ok(())
    }

    // ── runs ──────────────────────────────────────────────────────────────────

    pub fn insert_run(&self, id: &str, workflow: &str, params: &str, started_at: u64) -> Result<()> {
        self.conn.execute(
            "INSERT OR IGNORE INTO runs (id, workflow, status, params, started_at)
             VALUES (?1, ?2, 'running', ?3, ?4)",
            params![id, workflow, params, started_at as i64],
        )?;
        Ok(())
    }

    pub fn finish_run(&self, id: &str, success: bool, finished_at: u64) -> Result<()> {
        let status = if success { "success" } else { "failed" };
        self.conn.execute(
            "UPDATE runs SET status = ?1, finished_at = ?2 WHERE id = ?3",
            params![status, finished_at as i64, id],
        )?;
        Ok(())
    }

    pub fn upsert_task(
        &self,
        run_id: &str,
        task_id: &str,
        status: TaskStatus,
        exit_code: Option<i32>,
        stdout: Option<&str>,
        stderr: Option<&str>,
        started_at: Option<u64>,
        finished_at: Option<u64>,
    ) -> Result<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO task_results
             (run_id, task_id, status, exit_code, stdout, stderr, started_at, finished_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                run_id,
                task_id,
                status.as_str(),
                exit_code,
                stdout,
                stderr,
                started_at.map(|t| t as i64),
                finished_at.map(|t| t as i64),
            ],
        )?;
        Ok(())
    }

    pub fn list_runs(&self, limit: usize, offset: usize) -> Result<Vec<RunRow>> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT id, workflow, status, params, started_at, finished_at
             FROM runs ORDER BY started_at DESC LIMIT ?1 OFFSET ?2",
        )?;
        let rows = stmt.query_map(params![limit as i64, offset as i64], |row| {
            Ok(RunRow {
                id:          row.get(0)?,
                workflow:    row.get(1)?,
                status:      row.get(2)?,
                params:      row.get(3)?,
                started_at:  row.get::<_, i64>(4)? as u64,
                finished_at: row.get::<_, Option<i64>>(5)?.map(|t| t as u64),
            })
        })?;
        rows.map(|r| r.map_err(Into::into)).collect()
    }

    pub fn get_run(&self, id: &str) -> Result<Option<RunDetail>> {
        let run = {
            let mut stmt = self.conn.prepare_cached(
                "SELECT id, workflow, status, params, started_at, finished_at
                 FROM runs WHERE id = ?1",
            )?;
            let mut rows = stmt.query(params![id])?;
            match rows.next()? {
                None => return Ok(None),
                Some(row) => RunRow {
                    id:          row.get(0)?,
                    workflow:    row.get(1)?,
                    status:      row.get(2)?,
                    params:      row.get(3)?,
                    started_at:  row.get::<_, i64>(4)? as u64,
                    finished_at: row.get::<_, Option<i64>>(5)?.map(|t| t as u64),
                },
            }
        };

        let mut stmt = self.conn.prepare_cached(
            "SELECT run_id, task_id, status, exit_code, stdout, stderr, started_at, finished_at
             FROM task_results WHERE run_id = ?1",
        )?;
        let tasks: Vec<TaskRow> = stmt
            .query_map(params![id], |row| {
                Ok(TaskRow {
                    run_id:      row.get(0)?,
                    task_id:     row.get(1)?,
                    status:      row.get(2)?,
                    exit_code:   row.get(3)?,
                    stdout:      row.get(4)?,
                    stderr:      row.get(5)?,
                    started_at:  row.get::<_, Option<i64>>(6)?.map(|t| t as u64),
                    finished_at: row.get::<_, Option<i64>>(7)?.map(|t| t as u64),
                })
            })?
            .map(|r| r.map_err(Into::into))
            .collect::<Result<_>>()?;

        Ok(Some(RunDetail { run, tasks }))
    }

    // ── triggers ──────────────────────────────────────────────────────────────

    pub fn insert_trigger(
        &self,
        id: &str,
        workflow: &str,
        params: &str,
        source: &str,
        remote_addr: Option<&str>,
        received_at: u64,
    ) -> Result<()> {
        self.conn.execute(
            "INSERT OR IGNORE INTO triggers (id, workflow, status, params, source, remote_addr, received_at)
             VALUES (?1, ?2, 'received', ?3, ?4, ?5, ?6)",
            params![id, workflow, params, source, remote_addr, received_at as i64],
        )?;
        Ok(())
    }

    pub fn update_trigger_status(
        &self,
        id: &str,
        status: TriggerStatus,
        rejection_cause: Option<&str>,
        finished_at: Option<u64>,
    ) -> Result<()> {
        self.conn.execute(
            "UPDATE triggers SET status = ?1, rejection_cause = ?2, finished_at = ?3 WHERE id = ?4",
            params![status.as_str(), rejection_cause, finished_at.map(|t| t as i64), id],
        )?;
        Ok(())
    }

    pub fn list_triggers(&self, limit: usize, offset: usize) -> Result<Vec<TriggerRow>> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT id, workflow, status, params, source, rejection_cause, remote_addr, received_at, finished_at
             FROM triggers ORDER BY received_at DESC LIMIT ?1 OFFSET ?2",
        )?;
        let rows = stmt.query_map(params![limit as i64, offset as i64], |row| {
            Ok(TriggerRow {
                id:              row.get(0)?,
                workflow:        row.get(1)?,
                status:          row.get(2)?,
                params:          row.get(3)?,
                source:          row.get(4)?,
                rejection_cause: row.get(5)?,
                remote_addr:     row.get(6)?,
                received_at:     row.get::<_, i64>(7)? as u64,
                finished_at:     row.get::<_, Option<i64>>(8)?.map(|t| t as u64),
            })
        })?;
        rows.map(|r| r.map_err(Into::into)).collect()
    }

    pub fn get_trigger(&self, id: &str) -> Result<Option<TriggerRow>> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT id, workflow, status, params, source, rejection_cause, remote_addr, received_at, finished_at
             FROM triggers WHERE id = ?1",
        )?;
        let mut rows = stmt.query(params![id])?;
        match rows.next()? {
            None => Ok(None),
            Some(row) => Ok(Some(TriggerRow {
                id:              row.get(0)?,
                workflow:        row.get(1)?,
                status:          row.get(2)?,
                params:          row.get(3)?,
                source:          row.get(4)?,
                rejection_cause: row.get(5)?,
                remote_addr:     row.get(6)?,
                received_at:     row.get::<_, i64>(7)? as u64,
                finished_at:     row.get::<_, Option<i64>>(8)?.map(|t| t as u64),
            })),
        }
    }
    // ── task_logs ─────────────────────────────────────────────────────────────

    pub fn insert_task_log(
        &self,
        run_id: &str,
        task_id: &str,
        stream: &str,
        line: &str,
        logged_at: u64,
        expires_at: Option<u64>,
    ) -> Result<()> {
        self.conn.execute(
            "INSERT INTO task_logs (run_id, task_id, stream, line, logged_at, expires_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                run_id, task_id, stream, line,
                logged_at as i64,
                expires_at.map(|t| t as i64),
            ],
        )?;
        Ok(())
    }

    pub fn get_task_logs(&self, run_id: &str, task_id: &str) -> Result<Vec<LogRow>> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT run_id, task_id, stream, line, logged_at
             FROM task_logs WHERE run_id = ?1 AND task_id = ?2
             ORDER BY id ASC",
        )?;
        let rows = stmt.query_map(params![run_id, task_id], |row| Ok(LogRow {
            run_id:    row.get(0)?,
            task_id:   row.get(1)?,
            stream:    row.get(2)?,
            line:      row.get(3)?,
            logged_at: row.get::<_, i64>(4)? as u64,
        }))?;
        rows.collect::<rusqlite::Result<Vec<_>>>().map_err(Into::into)
    }

    /// All log lines for all runs of `workflow`, newest runs first.
    pub fn get_workflow_logs(&self, workflow: &str, limit: usize, offset: usize) -> Result<Vec<LogRow>> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT tl.run_id, tl.task_id, tl.stream, tl.line, tl.logged_at
             FROM task_logs tl
             JOIN runs r ON r.id = tl.run_id
             WHERE r.workflow = ?1
             ORDER BY tl.logged_at DESC
             LIMIT ?2 OFFSET ?3",
        )?;
        let rows = stmt.query_map(params![workflow, limit as i64, offset as i64], |row| Ok(LogRow {
            run_id:    row.get(0)?,
            task_id:   row.get(1)?,
            stream:    row.get(2)?,
            line:      row.get(3)?,
            logged_at: row.get::<_, i64>(4)? as u64,
        }))?;
        rows.collect::<rusqlite::Result<Vec<_>>>().map_err(Into::into)
    }

    /// Delete all log rows whose `expires_at` is set and in the past. Returns count deleted.
    pub fn cleanup_expired_logs(&self, now_ms: u64) -> Result<usize> {
        let count = self.conn.execute(
            "DELETE FROM task_logs WHERE expires_at IS NOT NULL AND expires_at < ?1",
            params![now_ms as i64],
        )?;
        Ok(count)
    }
}

// ──────────────────────────────────────────────
// Tests
// ──────────────────────────────────────────────
#[cfg(test)]
mod tests {
    use super::*;

    fn mem() -> Store {
        Store::open(":memory:").unwrap()
    }

    // --- globals ---

    #[test]
    fn get_missing_key_returns_none() {
        assert_eq!(mem().get("absent").unwrap(), None);
    }

    #[test]
    fn set_and_get_round_trips() {
        let s = mem();
        s.set("k", "v").unwrap();
        assert_eq!(s.get("k").unwrap(), Some("v".into()));
    }

    #[test]
    fn set_overwrites_existing_value() {
        let s = mem();
        s.set("k", "first").unwrap();
        s.set("k", "second").unwrap();
        assert_eq!(s.get("k").unwrap(), Some("second".into()));
    }

    #[test]
    fn get_all_returns_every_entry() {
        let s = mem();
        s.set("a", "1").unwrap();
        s.set("b", "2").unwrap();
        s.set("c", "3").unwrap();
        let all = s.get_all().unwrap();
        assert_eq!(all.len(), 3);
        assert_eq!(all["a"], "1");
    }

    #[test]
    fn get_all_empty_store_returns_empty_map() {
        assert!(mem().get_all().unwrap().is_empty());
    }

    #[test]
    fn values_persist_across_reopened_connections() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("vortex.db").to_string_lossy().into_owned();
        { Store::open(&path).unwrap().set("counter", "7").unwrap(); }
        assert_eq!(Store::open(&path).unwrap().get("counter").unwrap(), Some("7".into()));
    }

    // --- runs ---

    #[test]
    fn insert_run_creates_running_record() {
        let s = mem();
        s.insert_run("r1", "deploy", "{}", 1000).unwrap();
        let run = s.get_run("r1").unwrap().unwrap().run;
        assert_eq!(run.id, "r1");
        assert_eq!(run.workflow, "deploy");
        assert_eq!(run.status, "running");
        assert_eq!(run.started_at, 1000);
        assert_eq!(run.finished_at, None);
    }

    #[test]
    fn finish_run_updates_status_and_timestamp() {
        let s = mem();
        s.insert_run("r1", "deploy", "{}", 1000).unwrap();
        s.finish_run("r1", true, 2000).unwrap();
        let run = s.get_run("r1").unwrap().unwrap().run;
        assert_eq!(run.status, "success");
        assert_eq!(run.finished_at, Some(2000));
    }

    #[test]
    fn finish_run_failure_sets_failed_status() {
        let s = mem();
        s.insert_run("r1", "deploy", "{}", 1000).unwrap();
        s.finish_run("r1", false, 2000).unwrap();
        assert_eq!(s.get_run("r1").unwrap().unwrap().run.status, "failed");
    }

    #[test]
    fn list_runs_returns_most_recent_first() {
        let s = mem();
        s.insert_run("r1", "deploy", "{}", 1000).unwrap();
        s.insert_run("r2", "build",  "{}", 2000).unwrap();
        s.insert_run("r3", "test",   "{}", 3000).unwrap();
        let runs = s.list_runs(10, 0).unwrap();
        assert_eq!(runs[0].id, "r3");
        assert_eq!(runs[1].id, "r2");
        assert_eq!(runs[2].id, "r1");
    }

    #[test]
    fn list_runs_respects_limit_and_offset() {
        let s = mem();
        for i in 0..5u64 {
            s.insert_run(&format!("r{i}"), "w", "{}", i * 1000).unwrap();
        }
        let page = s.list_runs(2, 1).unwrap();
        assert_eq!(page.len(), 2);
    }

    #[test]
    fn get_run_returns_none_for_unknown_id() {
        assert!(mem().get_run("nope").unwrap().is_none());
    }

    // --- task_results ---

    #[test]
    fn upsert_task_stores_and_retrieves() {
        let s = mem();
        s.insert_run("r1", "deploy", "{}", 1000).unwrap();
        s.upsert_task("r1", "pull", TaskStatus::Success, Some(0), Some("ok\n"), Some(""), Some(1000), Some(1200)).unwrap();
        let detail = s.get_run("r1").unwrap().unwrap();
        assert_eq!(detail.tasks.len(), 1);
        let t = &detail.tasks[0];
        assert_eq!(t.task_id, "pull");
        assert_eq!(t.status, "success");
        assert_eq!(t.exit_code, Some(0));
        assert_eq!(t.stdout.as_deref(), Some("ok\n"));
        assert_eq!(t.started_at, Some(1000));
        assert_eq!(t.finished_at, Some(1200));
    }

    #[test]
    fn upsert_task_overwrites_on_duplicate() {
        let s = mem();
        s.insert_run("r1", "deploy", "{}", 1000).unwrap();
        s.upsert_task("r1", "pull", TaskStatus::Running, None, None, None, Some(1000), None).unwrap();
        s.upsert_task("r1", "pull", TaskStatus::Success, Some(0), Some("done\n"), Some(""), Some(1000), Some(1500)).unwrap();
        let tasks = s.get_run("r1").unwrap().unwrap().tasks;
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].status, "success");
    }

    #[test]
    fn upsert_skipped_task_stores_without_exit_code() {
        let s = mem();
        s.insert_run("r1", "deploy", "{}", 1000).unwrap();
        s.upsert_task("r1", "notify", TaskStatus::Skipped, None, None, None, None, None).unwrap();
        let t = &s.get_run("r1").unwrap().unwrap().tasks[0];
        assert_eq!(t.status, "skipped");
        assert_eq!(t.exit_code, None);
    }

    // --- triggers ---

    #[test]
    fn insert_trigger_creates_received_record() {
        let s = mem();
        s.insert_trigger("t1", "deploy", "{}", "http", Some("127.0.0.1:1234"), 1000).unwrap();
        let t = s.get_trigger("t1").unwrap().unwrap();
        assert_eq!(t.id, "t1");
        assert_eq!(t.workflow, "deploy");
        assert_eq!(t.status, "received");
        assert_eq!(t.source, "http");
        assert_eq!(t.remote_addr.as_deref(), Some("127.0.0.1:1234"));
        assert_eq!(t.received_at, 1000);
        assert_eq!(t.finished_at, None);
        assert_eq!(t.rejection_cause, None);
    }

    #[test]
    fn insert_trigger_without_remote_addr() {
        let s = mem();
        s.insert_trigger("t1", "greet", "{}", "uds", None, 1000).unwrap();
        let t = s.get_trigger("t1").unwrap().unwrap();
        assert_eq!(t.remote_addr, None);
    }

    #[test]
    fn update_trigger_status_to_accepted() {
        let s = mem();
        s.insert_trigger("t1", "deploy", "{}", "http", None, 1000).unwrap();
        s.update_trigger_status("t1", TriggerStatus::Accepted, None, None).unwrap();
        assert_eq!(s.get_trigger("t1").unwrap().unwrap().status, "accepted");
    }

    #[test]
    fn update_trigger_status_rejected_stores_cause_and_timestamp() {
        let s = mem();
        s.insert_trigger("t1", "deploy", "{}", "http", None, 1000).unwrap();
        s.update_trigger_status("t1", TriggerStatus::Rejected, Some("unauthorized"), Some(1100)).unwrap();
        let t = s.get_trigger("t1").unwrap().unwrap();
        assert_eq!(t.status, "rejected");
        assert_eq!(t.rejection_cause.as_deref(), Some("unauthorized"));
        assert_eq!(t.finished_at, Some(1100));
    }

    #[test]
    fn update_trigger_status_running_then_finished() {
        let s = mem();
        s.insert_trigger("t1", "deploy", "{}", "uds", None, 1000).unwrap();
        s.update_trigger_status("t1", TriggerStatus::Running, None, None).unwrap();
        assert_eq!(s.get_trigger("t1").unwrap().unwrap().status, "running");
        s.update_trigger_status("t1", TriggerStatus::Finished, None, Some(2000)).unwrap();
        let t = s.get_trigger("t1").unwrap().unwrap();
        assert_eq!(t.status, "finished");
        assert_eq!(t.finished_at, Some(2000));
    }

    #[test]
    fn update_trigger_status_on_nonexistent_is_noop() {
        mem().update_trigger_status("ghost", TriggerStatus::Finished, None, Some(1000)).unwrap();
    }

    #[test]
    fn list_triggers_returns_newest_first() {
        let s = mem();
        s.insert_trigger("t1", "a", "{}", "http", None, 1000).unwrap();
        s.insert_trigger("t2", "b", "{}", "http", None, 2000).unwrap();
        s.insert_trigger("t3", "c", "{}", "http", None, 3000).unwrap();
        let triggers = s.list_triggers(10, 0).unwrap();
        assert_eq!(triggers[0].id, "t3");
        assert_eq!(triggers[1].id, "t2");
        assert_eq!(triggers[2].id, "t1");
    }

    #[test]
    fn list_triggers_respects_limit_and_offset() {
        let s = mem();
        for i in 0..5u64 {
            s.insert_trigger(&format!("t{i}"), "w", "{}", "http", None, i * 1000).unwrap();
        }
        let page = s.list_triggers(2, 1).unwrap();
        assert_eq!(page.len(), 2);
    }

    #[test]
    fn get_trigger_returns_none_for_unknown() {
        assert!(mem().get_trigger("nope").unwrap().is_none());
    }

    // --- task_logs ---

    #[test]
    fn insert_and_retrieve_task_log() {
        let s = mem();
        s.insert_task_log("r1", "step", "stdout", "hello", 1000, Some(9999)).unwrap();
        let rows = s.get_task_logs("r1", "step").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].line, "hello");
        assert_eq!(rows[0].stream, "stdout");
        assert_eq!(rows[0].logged_at, 1000);
    }

    #[test]
    fn get_task_logs_returns_in_insertion_order() {
        let s = mem();
        s.insert_task_log("r1", "step", "stdout", "line1", 1000, None).unwrap();
        s.insert_task_log("r1", "step", "stdout", "line2", 2000, None).unwrap();
        s.insert_task_log("r1", "step", "stderr", "err1",  3000, None).unwrap();
        let rows = s.get_task_logs("r1", "step").unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].line, "line1");
        assert_eq!(rows[1].line, "line2");
        assert_eq!(rows[2].line, "err1");
    }

    #[test]
    fn get_task_logs_filters_by_task_id() {
        let s = mem();
        s.insert_task_log("r1", "step_a", "stdout", "a", 1000, None).unwrap();
        s.insert_task_log("r1", "step_b", "stdout", "b", 2000, None).unwrap();
        let rows = s.get_task_logs("r1", "step_a").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].line, "a");
    }

    #[test]
    fn cleanup_expired_logs_deletes_expired() {
        let s = mem();
        s.insert_task_log("r1", "t", "stdout", "old",  1000, Some(5000)).unwrap();
        s.insert_task_log("r1", "t", "stdout", "new",  2000, Some(15000)).unwrap();
        let deleted = s.cleanup_expired_logs(10000).unwrap();
        assert_eq!(deleted, 1);
        let rows = s.get_task_logs("r1", "t").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].line, "new");
    }

    #[test]
    fn cleanup_expired_logs_keeps_unexpired() {
        let s = mem();
        s.insert_task_log("r1", "t", "stdout", "line", 1000, Some(99999)).unwrap();
        assert_eq!(s.cleanup_expired_logs(10000).unwrap(), 0);
        assert_eq!(s.get_task_logs("r1", "t").unwrap().len(), 1);
    }

    #[test]
    fn cleanup_expired_logs_keeps_no_limit_rows() {
        let s = mem();
        s.insert_task_log("r1", "t", "stdout", "forever", 1000, None).unwrap();
        assert_eq!(s.cleanup_expired_logs(u64::MAX).unwrap(), 0);
        assert_eq!(s.get_task_logs("r1", "t").unwrap().len(), 1);
    }

    #[test]
    fn get_workflow_logs_joins_across_runs() {
        let s = mem();
        s.insert_run("r1", "deploy", "{}", 1000).unwrap();
        s.insert_run("r2", "deploy", "{}", 2000).unwrap();
        s.insert_run("r3", "other",  "{}", 3000).unwrap();
        s.insert_task_log("r1", "step", "stdout", "from r1", 1500, None).unwrap();
        s.insert_task_log("r2", "step", "stdout", "from r2", 2500, None).unwrap();
        s.insert_task_log("r3", "step", "stdout", "from r3", 3500, None).unwrap();
        let rows = s.get_workflow_logs("deploy", 50, 0).unwrap();
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().all(|r| r.run_id == "r1" || r.run_id == "r2"));
    }

    #[test]
    fn get_workflow_logs_respects_limit_and_offset() {
        let s = mem();
        s.insert_run("r1", "wf", "{}", 1000).unwrap();
        for i in 0..5u64 {
            s.insert_task_log("r1", "t", "stdout", &format!("line{i}"), i * 100 + 1000, None).unwrap();
        }
        let page = s.get_workflow_logs("wf", 2, 1).unwrap();
        assert_eq!(page.len(), 2);
    }
}
