use anyhow::{bail, Result};

/// Resolve the bearer token from the configured source.
///
/// | `method` | Reads from |
/// |----------|-----------|
/// | `"env"`  | Environment variable named by `key` |
/// | `"cmd"`  | Trimmed stdout of running `key` as a shell command |
pub fn resolve_token(method: &str, key: &str) -> Result<String> {
    match method {
        "env" => std::env::var(key)
            .map_err(|_| anyhow::anyhow!("Environment variable '{key}' is not set")),
        "cmd" => {
            let output = std::process::Command::new("sh")
                .arg("-c")
                .arg(key)
                .output()
                .map_err(|e| anyhow::anyhow!("Failed to run auth command '{key}': {e}"))?;
            if !output.status.success() {
                bail!("Auth command '{key}' exited with non-zero status");
            }
            Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
        }
        m => bail!("Unknown auth_method '{m}' — expected 'env' or 'cmd'"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn env_method_reads_variable() {
        std::env::set_var("VORTEX_AUTH_TEST_TOKEN", "super-secret");
        let token = resolve_token("env", "VORTEX_AUTH_TEST_TOKEN").unwrap();
        assert_eq!(token, "super-secret");
    }

    #[test]
    fn env_method_errors_on_missing_variable() {
        std::env::remove_var("VORTEX_AUTH_DEFINITELY_ABSENT");
        assert!(resolve_token("env", "VORTEX_AUTH_DEFINITELY_ABSENT").is_err());
    }

    #[test]
    fn cmd_method_runs_command_and_returns_output() {
        let token = resolve_token("cmd", "echo hello-from-cmd").unwrap();
        assert_eq!(token, "hello-from-cmd");
    }

    #[test]
    fn cmd_method_trims_trailing_whitespace() {
        let token = resolve_token("cmd", "printf 'token-value  '").unwrap();
        assert_eq!(token, "token-value");
    }

    #[test]
    fn cmd_method_errors_on_nonzero_exit() {
        assert!(resolve_token("cmd", "exit 1").is_err());
    }

    #[test]
    fn unknown_method_returns_error() {
        assert!(resolve_token("keychain", "my-key").is_err());
    }
}
