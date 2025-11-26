mod config;
mod matcher;
mod processor;

use crate::config::Config;
use crate::matcher::{DomainMatcher, IPMatcher};
use crate::processor::FileProcessor;
use anyhow::Result;
use rayon::prelude::*;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use std::thread;
use walkdir::WalkDir;

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn main() -> Result<()> {
    let start_time = Instant::now();
    println!("Rust 脚本启动...");

    let config = Config::load("config.yaml")?;
    
    let ip_matcher = IPMatcher::new(&config.source_ip)?;
    let domain_matcher = DomainMatcher::new(&config.query_domain);
    
    // Shared processor (stateless/immutable part)
    let processor = Arc::new(FileProcessor::new(ip_matcher, domain_matcher));

    // Task 1: Aggregated Logs
    run_aggregated_log_search(&config, &processor)?;

    // Task 2: Native Logs
    if config.is_query_native_log.to_lowercase() == "yes" {
        run_native_log_search(&config, &processor)?;
    } else {
        println!("配置中 'isQueryNativeLog' 为 'no'，跳过原始日志检索。");
    }

    println!("所有任务执行完毕，总耗时: {:?}", start_time.elapsed());
    Ok(())
}

fn run_aggregated_log_search(config: &Config, processor: &Arc<FileProcessor>) -> Result<()> {
    println!("\n--- [任务1: 开始检索汇总日志] ---");
    let task_time = Instant::now();

    let files = find_files(&config.log_directory, &config.query_time_day, &config.query_time_hour, ".gz");
    if files.is_empty() {
        println!("任务1: 未找到符合条件的汇总日志文件。");
        return Ok(());
    }
    let total_files = files.len();
    println!("任务1: 发现 {} 个待处理的汇总日志文件...", total_files);

    // Prepare output
    let output_path = get_output_path(config, "aggregated", true);
    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let output_file = Arc::new(Mutex::new(File::create(&output_path)?));

    // Progress tracking
    let processed_count = Arc::new(AtomicUsize::new(0));
    let processed_count_clone = Arc::clone(&processed_count);
    let start_time = Instant::now();
    
    // Spawn progress reporter thread
    let progress_handle = thread::spawn(move || {
        let mut next_report_time = start_time + Duration::from_secs(120); // First report at 2 minutes
        loop {
            thread::sleep(Duration::from_secs(30)); // Check every 30 seconds
            let current_count = processed_count_clone.load(Ordering::Relaxed);
            let now = Instant::now();
            
            // Report every 2 minutes
            if now >= next_report_time {
                let elapsed = now.duration_since(start_time);
                let progress_pct = (current_count as f64 / total_files as f64 * 100.0) as usize;
                let files_per_sec = if elapsed.as_secs() > 0 {
                    current_count as f64 / elapsed.as_secs() as f64
                } else {
                    0.0
                };
                println!("任务1 进度: {}/{} ({}%) | 速度: {:.2} 文件/秒 | 已耗时: {:?}", 
                    current_count, total_files, progress_pct, files_per_sec, elapsed);
                next_report_time = now + Duration::from_secs(120); // Next report in 2 minutes
            }
            
            if current_count >= total_files {
                break;
            }
        }
    });

    // Parallel processing
    let pool_size = config.worker_pool_size.unwrap_or_else(num_cpus::get);
    let pool = rayon::ThreadPoolBuilder::new().num_threads(pool_size).build()?;

    let total_matches = pool.install(|| {
        files.par_iter().map(|path| {
            let file_clone = Arc::clone(&output_file);
            // Thread-local buffer to reduce lock contention
            let mut local_buffer = Vec::with_capacity(64 * 1024); // 64KB buffer
            
            let result = processor.process_aggregated_file(path, |line| {
                local_buffer.extend_from_slice(line);
                local_buffer.push(b'\n');
                
                // Flush if buffer is large enough
                if local_buffer.len() >= 64 * 1024 {
                    let mut file = file_clone.lock().unwrap();
                    file.write_all(&local_buffer).unwrap();
                    local_buffer.clear();
                }
            });
            
            // Flush remaining data
            if !local_buffer.is_empty() {
                let mut file = file_clone.lock().unwrap();
                file.write_all(&local_buffer).unwrap();
            }
            
            let match_count = match result {
                Ok(count) => count,
                Err(e) => {
                    eprintln!("Error processing file {:?}: {}", path, e);
                    0
                }
            };
            processed_count.fetch_add(1, Ordering::Relaxed);
            match_count
        }).sum::<usize>()
    });

    // Wait for progress reporter to finish
    let _ = progress_handle.join();

    println!("任务1: 结果成功保存至 {:?}，共写入 {} 条记录。", output_path, total_matches);
    println!("--- [任务1: 结束, 耗时: {:?}] ---", task_time.elapsed());
    Ok(())
}

fn run_native_log_search(config: &Config, processor: &Arc<FileProcessor>) -> Result<()> {
    println!("\n--- [任务2: 开始检索原始日志] ---");
    let task_time = Instant::now();

    let native_loc = config.native_log_loc.as_ref().expect("nativeLogLoc required");
    let files = find_files(native_loc, &config.query_time_day, &config.query_time_hour, ".gz");
    
    if files.is_empty() {
        println!("任务2: 未找到符合条件的原始日志文件。");
        return Ok(());
    }
    let total_files = files.len();
    println!("任务2: 发现 {} 个待处理的原始日志文件...", total_files);

    let output_path = get_output_path(config, "native", false);
    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let output_file = Arc::new(Mutex::new(File::create(&output_path)?));

    // Progress tracking
    let processed_count = Arc::new(AtomicUsize::new(0));
    let processed_count_clone = Arc::clone(&processed_count);
    let start_time = Instant::now();
    
    // Spawn progress reporter thread
    let progress_handle = thread::spawn(move || {
        let mut next_report_time = start_time + Duration::from_secs(120); // First report at 2 minutes
        loop {
            thread::sleep(Duration::from_secs(30)); // Check every 30 seconds
            let current_count = processed_count_clone.load(Ordering::Relaxed);
            let now = Instant::now();
            
            // Report every 2 minutes
            if now >= next_report_time {
                let elapsed = now.duration_since(start_time);
                let progress_pct = (current_count as f64 / total_files as f64 * 100.0) as usize;
                let files_per_sec = if elapsed.as_secs() > 0 {
                    current_count as f64 / elapsed.as_secs() as f64
                } else {
                    0.0
                };
                println!("任务2 进度: {}/{} ({}%) | 速度: {:.2} 文件/秒 | 已耗时: {:?}", 
                    current_count, total_files, progress_pct, files_per_sec, elapsed);
                next_report_time = now + Duration::from_secs(120); // Next report in 2 minutes
            }
            
            if current_count >= total_files {
                break;
            }
        }
    });

    let pool_size = config.worker_pool_size.unwrap_or_else(num_cpus::get);
    let pool = rayon::ThreadPoolBuilder::new().num_threads(pool_size).build()?;

    let total_matches = pool.install(|| {
        files.par_iter().map(|path| {
            let file_clone = Arc::clone(&output_file);
            // Thread-local buffer to reduce lock contention
            let mut local_buffer = Vec::with_capacity(64 * 1024); // 64KB buffer

            let result = processor.process_native_file(path, |line| {
                local_buffer.extend_from_slice(line);
                local_buffer.push(b'\n');
                
                // Flush if buffer is large enough
                if local_buffer.len() >= 64 * 1024 {
                    let mut file = file_clone.lock().unwrap();
                    file.write_all(&local_buffer).unwrap();
                    local_buffer.clear();
                }
            });
            
            // Flush remaining data
            if !local_buffer.is_empty() {
                let mut file = file_clone.lock().unwrap();
                file.write_all(&local_buffer).unwrap();
            }
            
            let match_count = match result {
                Ok(count) => count,
                Err(e) => {
                    eprintln!("Error processing file {:?}: {}", path, e);
                    0
                }
            };
            processed_count.fetch_add(1, Ordering::Relaxed);
            match_count
        }).sum::<usize>()
    });

    // Wait for progress reporter to finish
    let _ = progress_handle.join();

    println!("任务2: 结果成功保存至 {:?}，共写入 {} 条记录。", output_path, total_matches);
    println!("--- [任务2: 结束, 耗时: {:?}] ---", task_time.elapsed());
    Ok(())
}

fn find_files(dir: &str, days: &Option<Vec<String>>, hours: &Option<Vec<String>>, suffix: &str) -> Vec<PathBuf> {
    let mut files = Vec::new();
    let mut search_prefixes = Vec::new();
    
    if let Some(ds) = days {
        search_prefixes.extend(ds.clone());
    }
    if let Some(hs) = hours {
        search_prefixes.extend(hs.clone());
    }

    for entry in WalkDir::new(dir).into_iter().filter_map(|e| e.ok()) {
        let path = entry.path();
        if path.is_file() {
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                if name.ends_with(suffix) {
                    // Check if name contains any of the time prefixes
                    for prefix in &search_prefixes {
                        if name.contains(prefix) {
                            files.push(path.to_path_buf());
                            break;
                        }
                    }
                }
            }
        }
    }
    files
}

fn get_output_path(config: &Config, task_type: &str, is_aggregated: bool) -> PathBuf {
    let base_dir = if is_aggregated {
        config.aggregated_log_result_loc.clone().unwrap_or_else(|| "./".to_string())
    } else {
        config.native_log_result_loc.clone().unwrap_or_else(|| "./".to_string())
    };

    let date_part = if let Some(days) = &config.query_time_day {
        days.first().cloned().unwrap_or_else(|| "unknown".to_string())
    } else {
        "unknown".to_string()
    };

    let dir_name = format!("{}_{}_{}_results", 
        config.query_domain.replace("*", "wildcard"), 
        config.source_ip.replace("/", "_"), // sanitize CIDR
        date_part
    );

    Path::new(&base_dir).join(dir_name).join(format!("matched_{}_logs.txt", task_type))
}
