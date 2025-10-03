// src/query_manager.rs
use anyhow::Result;
use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use tempfile::NamedTempFile;
use std::io::Write;
use tracing::{info, debug};
use crate::config::QueryDefinition;

pub struct QueryManager {
    working_dir: PathBuf,
    total_submitted: Arc<AtomicU32>,
    total_completed: Arc<AtomicU32>,
    total_failed: Arc<AtomicU32>,
    nebuli_binary: PathBuf,
}

impl QueryManager {
    pub fn new(working_dir: PathBuf) -> Self {
        // Todo: generalize
        let nebuli_binary = std::path::Path::new("/home/rudi/dima/nebulastream-public/cmake-build-relwithdebinfo/nes-nebuli/nes-nebuli").to_path_buf();
        // let nebuli_binary = working_dir.join("nes-nebuli");

        Self {
            working_dir,
            total_submitted: Arc::new(AtomicU32::new(0)),
            total_completed: Arc::new(AtomicU32::new(0)),
            total_failed: Arc::new(AtomicU32::new(0)),
            nebuli_binary,
        }
    }

    pub async fn submit_query(
        &self,
        query: QueryDefinition,
        instance_id: u32,
        output_path: PathBuf,
    ) -> Result<()> {
        info!("Submitting query '{}' instance {}", query.id, instance_id);

        // Read the original YAML
        let yaml_content = std::fs::read_to_string(&query.yaml_path)?;

        // Modify the output path in the YAML
        let modified_yaml = self.modify_yaml_output(&yaml_content, &output_path)?;

        // Create temporary file with modified YAML
        let mut temp_file = NamedTempFile::new()?;
        temp_file.write_all(modified_yaml.as_bytes())?;
        let temp_path = temp_file.path().to_path_buf();

        // Register the query
        let register_output = Command::new(&self.nebuli_binary)
            .args(&["-d", "-w", "-s", "localhost:8080", "register"])
            .stdin(std::fs::File::open(&temp_path)?)
            .current_dir(&self.working_dir)
            .output()?;

        if !register_output.status.success() {
            let stderr = String::from_utf8_lossy(&register_output.stderr);
            anyhow::bail!("Failed to register query: {}", stderr);
        }

        debug!("Query registered successfully");

        // Start the query
        let start_output = Command::new(&self.nebuli_binary)
            .args(&["-d", "-w", "-s", "localhost:8080", "start", &instance_id.to_string()])
            .current_dir(&self.working_dir)
            .output()?;

        if !start_output.status.success() {
            let stderr = String::from_utf8_lossy(&start_output.stderr);
            self.total_failed.fetch_add(1, Ordering::Relaxed);
            anyhow::bail!("Failed to start query: {}", stderr);
        }

        self.total_submitted.fetch_add(1, Ordering::Relaxed);
        info!("Query '{}' instance {} started successfully", query.id, instance_id);

        Ok(())
    }

    fn modify_yaml_output(&self, yaml: &str, output_path: &PathBuf) -> Result<String> {
        // Simple string replacement for the output path
        // This assumes the YAML has a standard format with filePath field
        let output_str = output_path.to_string_lossy();

        // Look for filePath line and replace it
        let lines: Vec<&str> = yaml.lines().collect();
        let mut modified_lines = Vec::new();

        for line in lines {
            if line.contains("filePath:") {
                modified_lines.push(format!("    filePath: \"{}\"", output_str));
            } else {
                modified_lines.push(line.to_string());
            }
        }

        Ok(modified_lines.join("\n"))
    }

    pub async fn all_queries_completed(&self) -> Result<bool> {
        // This would need to actually check query status via the NES API
        // For now, we'll use a simple heuristic
        // In production, you'd query the system's API for actual status

        // Placeholder - you'd implement actual status checking here
        Ok(true)
    }

    pub async fn get_stats(&self) -> Result<serde_json::Value> {
        Ok(serde_json::json!({
            "submitted": self.total_submitted.load(Ordering::Relaxed),
            "completed": self.total_completed.load(Ordering::Relaxed),
            "failed": self.total_failed.load(Ordering::Relaxed),
        }))
    }
}