// src/main.rs
use anyhow::Result;
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use tracing::{info, error};
use walkdir::WalkDir;

mod config;
mod runner;
mod tcp_manager;
mod query_manager;
mod system_manager;

use config::BenchmarkConfig;
use runner::BenchmarkRunner;

#[derive(Parser)]
#[command(name = "nes-benchmark")]
#[command(about = "NES Benchmarking Framework")]
struct Args {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run a single benchmark from a TOML file
    Run {
        #[arg(short, long)]
        config: PathBuf,

        #[arg(short, long)]
        dry_run: bool,
    },

    /// Run all benchmarks in a directory
    RunDir {
        #[arg(short, long)]
        dir: PathBuf,

        #[arg(short, long)]
        dry_run: bool,
    },

    /// Validate benchmark configuration(s)
    Validate {
        #[arg(short, long)]
        path: PathBuf,
    },

    /// Generate example benchmark configuration
    Example {
        #[arg(short, long, default_value = "example_benchmark.toml")]
        output: PathBuf,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .init();

    let args = Args::parse();

    match args.command {
        Commands::Run { config, dry_run } => {
            info!("Loading benchmark configuration from: {:?}", config);
            let benchmark_config = BenchmarkConfig::from_file(&config)?;

            if dry_run {
                info!("Dry run mode - validating configuration only");
                benchmark_config.validate()?;
                info!("Configuration is valid");
                return Ok(());
            }

            let runner = BenchmarkRunner::new(benchmark_config);
            runner.run().await?;
        }

        Commands::RunDir { dir, dry_run } => {
            info!("Running all benchmarks in directory: {:?}", dir);

            let mut configs = Vec::new();
            for entry in WalkDir::new(&dir)
                .into_iter()
                .filter_map(|e| e.ok())
                .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("toml"))
            {
                info!("Found benchmark file: {:?}", entry.path());
                match BenchmarkConfig::from_file(entry.path()) {
                    Ok(config) => configs.push((entry.path().to_path_buf(), config)),
                    Err(e) => error!("Failed to load {:?}: {}", entry.path(), e),
                }
            }

            if configs.is_empty() {
                error!("No valid benchmark configurations found in {:?}", dir);
                return Ok(());
            }

            for (path, config) in configs {
                info!("Running benchmark: {:?}", path);

                if dry_run {
                    info!("Dry run mode - validating configuration only");
                    config.validate()?;
                    info!("Configuration is valid");
                } else {
                    let runner = BenchmarkRunner::new(config);
                    runner.run().await?;
                }

                info!("Completed benchmark: {:?}", path);
                info!("-----------------------------------");
            }
        }

        Commands::Validate { path } => {
            if path.is_dir() {
                let mut all_valid = true;
                for entry in WalkDir::new(&path)
                    .into_iter()
                    .filter_map(|e| e.ok())
                    .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("toml"))
                {
                    print!("Validating {:?}... ", entry.path());
                    match BenchmarkConfig::from_file(entry.path()) {
                        Ok(config) => {
                            if let Err(e) = config.validate() {
                                println!("FAILED: {}", e);
                                all_valid = false;
                            } else {
                                println!("OK");
                            }
                        }
                        Err(e) => {
                            println!("FAILED to load: {}", e);
                            all_valid = false;
                        }
                    }
                }
                if !all_valid {
                    error!("Some configurations are invalid");
                    std::process::exit(1);
                }
            } else {
                print!("Validating {:?}... ", path);
                let config = BenchmarkConfig::from_file(&path)?;
                config.validate()?;
                println!("OK");
            }
        }

        Commands::Example { output } => {
            info!("Generating example benchmark configuration to: {:?}", output);
            BenchmarkConfig::write_example(&output)?;
            info!("Example configuration written successfully");
        }
    }

    Ok(())
}