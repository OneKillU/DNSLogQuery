mod config;
mod matcher;
mod processor;

use crate::config::Config;
use crate::matcher::{DomainMatcher, IPMatcher};
use crate::processor::FileProcessor;
use anyhow::Result;
use rayon::prelude::*;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use std::thread;
use walkdir::WalkDir;
use crossbeam_channel::{bounded, Sender, Receiver};
use core_affinity;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;



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

    // Channel for async writing
    let (tx, rx) = bounded::<Vec<u8>>(1024);
    
    // Spawn writer thread
    let writer_handle = thread::spawn(move || -> Result<usize> {
        let file = File::create(&output_path)?;
        let mut writer = BufWriter::with_capacity(1024 * 1024, file); // 1MB buffer
        let mut total_bytes = 0;
        for chunk in rx {
            writer.write_all(&chunk)?;
            total_bytes += chunk.len();
        }
        writer.flush()?;
        Ok(total_bytes)
    });

    // Progress tracking
    let processed_count = Arc::new(AtomicUsize::new(0));
    let processed_count_clone = Arc::clone(&processed_count);
    let start_time = Instant::now();
    
    // Spawn progress reporter thread
    let progress_handle = thread::spawn(move || {
        let mut next_report_time = start_time + Duration::from_secs(120);
        loop {
            thread::sleep(Duration::from_secs(30));
            let current_count = processed_count_clone.load(Ordering::Relaxed);
            let now = Instant::now();
            
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
                next_report_time = now + Duration::from_secs(120);
            }
            
            if current_count >= total_files {
                break;
            }
        }
    });

    // IO-Compute Separation Model
    // 1. Channel for memory-resident file data (Bounded to limit memory usage)
    //    Capacity = 4 means max 4 files in memory waiting for CPU.
    //    If files are avg 100MB, max usage ~400MB + current processing file.
    let (data_tx, data_rx) = bounded::<(PathBuf, Vec<u8>)>(4);

    // 2. Spawn IO Thread (Read file to memory)
    //    This thread does SEQUENTIAL disk read, maximizing HDD throughput.
    let files_for_io = files.clone();
    let io_handle = thread::spawn(move || {
        for path in files_for_io {
            match File::open(&path) {
                Ok(mut file) => {
                    let mut buffer = Vec::with_capacity(10 * 1024 * 1024); // Start with 10MB
                    if let Err(e) = std::io::Read::read_to_end(&mut file, &mut buffer) {
                         eprintln!("Error reading file {:?}: {}", path, e);
                         continue;
                    }
                    // Send to workers (will block if channel is full, throttling IO)
                    if data_tx.send((path, buffer)).is_err() {
                        break;
                    }
                },
                Err(e) => eprintln!("Error opening file {:?}: {}", path, e),
            }
        }
    });

    // 3. Spawn Compute Workers (CPU Bound)
    let pool_size = config.worker_pool_size.unwrap_or_else(num_cpus::get);
    let mut handles = Vec::new();
    let core_ids = config.core_ids.clone();

    for i in 0..pool_size {
        let data_rx = data_rx.clone();
        let tx = tx.clone();
        let processor = Arc::clone(processor);
        let processed_count = Arc::clone(&processed_count);
        let core_id_to_bind = core_ids.as_ref().and_then(|ids| ids.get(i).cloned());

        let handle = thread::spawn(move || {
            // Bind to CPU Core
            if let Some(core_id) = core_id_to_bind {
                if let Some(core_ids) = core_affinity::get_core_ids() {
                    if let Some(core) = core_ids.into_iter().find(|c| c.id == core_id) {
                        core_affinity::set_for_current(core);
                    }
                }
            }

            let mut total_matches = 0;
            let mut local_buffer = Vec::with_capacity(128 * 1024); 
            
            while let Ok((path, data)) = data_rx.recv() {
                // Process from Memory
                let result = processor.process_aggregated_data(&data, |line| {
                    local_buffer.extend_from_slice(line);
                    local_buffer.push(b'\n');
                    
                    if local_buffer.len() >= 128 * 1024 {
                        let mut new_buf = Vec::with_capacity(128 * 1024);
                        std::mem::swap(&mut local_buffer, &mut new_buf);
                        tx.send(new_buf).unwrap();
                    }
                });
                
                if !local_buffer.is_empty() {
                    let mut new_buf = Vec::with_capacity(128 * 1024);
                    std::mem::swap(&mut local_buffer, &mut new_buf);
                    tx.send(new_buf).unwrap();
                }

                match result {
                    Ok(count) => total_matches += count,
                    Err(e) => eprintln!("Error processing file {:?}: {}", path, e),
                }
                
                processed_count.fetch_add(1, Ordering::Relaxed);
                
                // Explicitly drop large buffer to free memory immediately
                drop(data);
            }
            total_matches
        });
        handles.push(handle);
    }

    // Wait for IO thread
    io_handle.join().unwrap();
    
    // Wait for workers and sum results
    let total_matches: usize = handles.into_iter()
        .map(|h| h.join().unwrap())
        .sum();

    // Drop main thread's sender to close channel
    drop(tx);
    
    // Wait for writer and progress reporter
    let _ = writer_handle.join().unwrap();
    let _ = progress_handle.join();

    println!("任务1: 结果已保存，共写入 {} 条记录。", total_matches);
    println!("--- [任务1: 结束, 耗时: {:?}] ---", task_time.elapsed());
    Ok(())
}

fn run_native_log_search(config: &Config, processor: &Arc<FileProcessor>) -> Result<()> {
    println!("\n--- [任务2: 开始检索原始日志] ---");
    let task_time = Instant::now();

    let native_loc = config.native_log_loc.as_ref().expect("nativeLogLoc required");
    let native_loc = config.native_log_loc.as_ref().expect("nativeLogLoc required");
    let files = find_files_native(native_loc, &config.query_time_day, &config.query_time_hour, ".gz");
    
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

    // Channel for async writing
    let (tx, rx) = bounded::<Vec<u8>>(1024);
    
    // Spawn writer thread
    let writer_handle = thread::spawn(move || -> Result<usize> {
        let file = File::create(&output_path)?;
        let mut writer = BufWriter::with_capacity(1024 * 1024, file); // 1MB buffer
        let mut total_bytes = 0;
        for chunk in rx {
            writer.write_all(&chunk)?;
            total_bytes += chunk.len();
        }
        writer.flush()?;
        Ok(total_bytes)
    });

    // Progress tracking
    let processed_count = Arc::new(AtomicUsize::new(0));
    let processed_count_clone = Arc::clone(&processed_count);
    let start_time = Instant::now();
    
    // Spawn progress reporter thread
    let progress_handle = thread::spawn(move || {
        let mut next_report_time = start_time + Duration::from_secs(120);
        loop {
            thread::sleep(Duration::from_secs(30));
            let current_count = processed_count_clone.load(Ordering::Relaxed);
            let now = Instant::now();
            
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
                next_report_time = now + Duration::from_secs(120);
            }
            
            if current_count >= total_files {
                break;
            }
        }
    });

    // IO-Compute Separation Model
    let (data_tx, data_rx) = bounded::<(PathBuf, Vec<u8>)>(4);

    // Spawn IO Thread
    let files_for_io = files.clone();
    let io_handle = thread::spawn(move || {
        for path in files_for_io {
            match File::open(&path) {
                Ok(mut file) => {
                    let mut buffer = Vec::with_capacity(10 * 1024 * 1024);
                    if let Err(e) = std::io::Read::read_to_end(&mut file, &mut buffer) {
                         eprintln!("Error reading file {:?}: {}", path, e);
                         continue;
                    }
                    if data_tx.send((path, buffer)).is_err() {
                        break;
                    }
                },
                Err(e) => eprintln!("Error opening file {:?}: {}", path, e),
            }
        }
    });

    // Spawn Compute Workers
    let pool_size = config.worker_pool_size.unwrap_or_else(num_cpus::get);
    let mut handles = Vec::new();
    let core_ids = config.core_ids.clone();

    for i in 0..pool_size {
        let data_rx = data_rx.clone();
        let tx = tx.clone();
        let processor = Arc::clone(processor);
        let processed_count = Arc::clone(&processed_count);
        let core_id_to_bind = core_ids.as_ref().and_then(|ids| ids.get(i).cloned());

        let handle = thread::spawn(move || {
            if let Some(core_id) = core_id_to_bind {
                if let Some(core_ids) = core_affinity::get_core_ids() {
                    if let Some(core) = core_ids.into_iter().find(|c| c.id == core_id) {
                        core_affinity::set_for_current(core);
                    }
                }
            }

            let mut total_matches = 0;
            let mut local_buffer = Vec::with_capacity(128 * 1024); 
            
            while let Ok((path, data)) = data_rx.recv() {
                let result = processor.process_native_data(&data, |line| {
                    local_buffer.extend_from_slice(line);
                    local_buffer.push(b'\n');
                    
                    if local_buffer.len() >= 128 * 1024 {
                        let mut new_buf = Vec::with_capacity(128 * 1024);
                        std::mem::swap(&mut local_buffer, &mut new_buf);
                        tx.send(new_buf).unwrap();
                    }
                });
                
                if !local_buffer.is_empty() {
                    let mut new_buf = Vec::with_capacity(128 * 1024);
                    std::mem::swap(&mut local_buffer, &mut new_buf);
                    tx.send(new_buf).unwrap();
                }

                match result {
                    Ok(count) => total_matches += count,
                    Err(e) => eprintln!("Error processing file {:?}: {}", path, e),
                }
                
                processed_count.fetch_add(1, Ordering::Relaxed);
                drop(data);
            }
            total_matches
        });
        handles.push(handle);
    }

    // Wait for IO thread
    io_handle.join().unwrap();
    
    // Wait for workers
    let total_matches: usize = handles.into_iter()
        .map(|h| h.join().unwrap())
        .sum();

    // Drop main thread's sender
    drop(tx);

    // Wait for writer and progress reporter
    let _ = writer_handle.join().unwrap();
    let _ = progress_handle.join();

    println!("任务2: 结果已保存，共写入 {} 条记录。", total_matches);
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
            if let Some(path_str) = path.to_str() {
                if path_str.ends_with(suffix) {
                    // Check if full path contains any of the time prefixes
                    // This allows finding files in directories like ".../20250626/access.log.gz"
                    for prefix in &search_prefixes {
                        if path_str.contains(prefix) {
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

fn find_files_native(dir: &str, days: &Option<Vec<String>>, hours: &Option<Vec<String>>, suffix: &str) -> Vec<PathBuf> {
    let mut files = Vec::new();
    let mut search_prefixes = Vec::new();
    if let Some(ds) = days { search_prefixes.extend(ds.clone()); }
    if let Some(hs) = hours { search_prefixes.extend(hs.clone()); }

    for entry in WalkDir::new(dir).into_iter().filter_map(|e| e.ok()) {
        let path = entry.path();
        if path.is_file() {
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                if name.ends_with(suffix) {
                    // Check specific format: 250_132228145205_20251209151802_1.gz
                    let parts: Vec<&str> = name.split('_').collect();
                    if parts.len() >= 3 {
                        let timestamp = parts[2];
                        for prefix in &search_prefixes {
                            if timestamp.starts_with(prefix) {
                                files.push(path.to_path_buf());
                                break;
                            }
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

    let domain_part = if config.query_domain.is_empty() {
        "all_domains".to_string()
    } else if config.query_domain.len() == 1 {
        config.query_domain[0].replace("*", "wildcard")
    } else {
        "multi_domains".to_string()
    };

    let ip_part = if config.source_ip.is_empty() {
        "all_ips".to_string()
    } else if config.source_ip.len() == 1 {
        config.source_ip[0].replace("/", "_")
    } else {
        "multi_ips".to_string()
    };

    let dir_name = format!("{}_{}_{}_results", 
        domain_part, 
        ip_part, 
        date_part
    );

    Path::new(&base_dir).join(dir_name).join(format!("matched_{}_logs.txt", task_type))
}
