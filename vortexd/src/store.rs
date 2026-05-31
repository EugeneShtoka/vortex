use std::collections::HashMap;

use anyhow::Result;
use rusqlite::{params, Connection};

/// Persistent key-value store backed by SQLite.
/// Pass `":memory:"` as `path` for ephemeral in-process storage (tests, dry-runs).
pub struct Store {
    conn: Connection,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RunRow {
    pub id: String,
    pub workflow: String,
    pub status: String,           // "running"|"success"|"failure"|"rejected"
    pub rejection: Option<String>,
    pub params: String,           // JSON object
    pub started_at: u64,
    pub finished_at: Option<u64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TaskRow {
    pub run_id: String,
    pub task_id: String,
    pub status: String,           // "running"|"success"|"failure"|"skipped"
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
                rejection    TEXT,
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
             );",
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
        let status = if success { "success" } else { "failure" };
        self.conn.execute(
            "UPDATE runs SET status = ?1, finished_at = ?2 WHERE id = ?3",
            params![status, finished_at as i64, id],
        )?;
        Ok(())
    }

    pub fn reject_run(&self, id: &str, reason: &str, finished_at: u64) -> Result<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO runs (id, workflow, status, rejection, params, started_at, finished_at)
             VALUES (?1, '', 'rejected', ?2, '{}', ?3, ?3)",
            params![id, reason, finished_at as i64],
        )?;
        Ok(())
    }

    pub fn upsert_task(
        &self,
        run_id: &str,
        task_id: &str,
        status: &str,
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
                status,
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
            "SELECT id, workflow, status, rejection, params, started_at, finished_at
             FROM runs ORDER BY started_at DESC LIMIT ?1 OFFSET ?2",
        )?;
        let rows = stmt.query_map(params![limit as i64, offset as i64], |row| {
            Ok(RunRow {
                id:          row.get(0)?,
                workflow:    row.get(1)?,
                status:      row.get(2)?,
                rejection:   row.get(3)?,
                params:      row.get(4)?,
                started_at:  row.get::<_, i64>(5)? as u64,
                finished_at: row.get::<_, Option<i64>>(6)?.map(|t| t as u64),
            })
        })?;
        rows.map(|r| r.map_err(Into::into)).collect()
    }

    pub fn get_run(&self, id: &str) -> Result<Option<RunDetail>> {
        let run = {
            let mut stmt = self.conn.prepare_cached(
                "SELECT id, workflow, status, rejection, params, started_at, finished_at
                 FROM runs WHERE id = ?1",
            )?;
            let mut rows = stmt.query(params![id])?;
            match rows.next()? {
                None => return Ok(None),
                Some(row) => RunRow {
                    id:          row.get(0)?,
                    workflow:    row.get(1)?,
                    status:      row.get(2)?,
                    rejection:   row.get(3)?,
                    params:      row.get(4)?,
                    started_at:  row.get::<_, i64>(5)? as u64,
                    finished_at: row.get::<_, Option<i64>>(6)?.map(|t| t as u64),
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
    fn finish_run_failure_sets_failure_status() {
        let s = mem();
        s.insert_run("r1", "deploy", "{}", 1000).unwrap();
        s.finish_run("r1", false, 2000).unwrap();
        assert_eq!(s.get_run("r1").unwrap().unwrap().run.status, "failure");
    }

    #[test]
    fn reject_run_stores_reason() {
        let s = mem();
        s.reject_run("r1", "unauthorized", 1000).unwrap();
        let run = s.get_run("r1").unwrap().unwrap().run;
        assert_eq!(run.status, "rejected");
        assert_eq!(run.rejection.as_deref(), Some("unauthorized"));
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
        s.upsert_task("r1", "pull", "success", Some(0), Some("ok\n"), Some(""), Some(1000), Some(1200)).unwrap();
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
        s.upsert_task("r1", "pull", "running", None, None, None, Some(1000), None).unwrap();
        s.upsert_task("r1", "pull", "success", Some(0), Some("done\n"), Some(""), Some(1000), Some(1500)).unwrap();
        let tasks = s.get_run("r1").unwrap().unwrap().tasks;
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].status, "success");
    }

    #[test]
    fn upsert_skipped_task_stores_without_exit_code() {
        let s = mem();
        s.insert_run("r1", "deploy", "{}", 1000).unwrap();
        s.upsert_task("r1", "notify", "skipped", None, None, None, None, None).unwrap();
        let t = &s.get_run("r1").unwrap().unwrap().tasks[0];
        assert_eq!(t.status, "skipped");
        assert_eq!(t.exit_code, None);
    }
}
