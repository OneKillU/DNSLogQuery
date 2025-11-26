use serde::Deserialize;
use std::fs;
use anyhow::Result;

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    #[serde(rename = "logDirectory")]
    pub log_directory: String,

    #[serde(rename = "workerPoolSize")]
    pub worker_pool_size: Option<usize>,

    #[serde(rename = "queryDomain")]
    pub query_domain: String,

    #[serde(rename = "sourceIP")]
    pub source_ip: String,

    #[serde(rename = "queryTime_hour")]
    pub query_time_hour: Option<Vec<String>>,

    #[serde(rename = "queryTime_day")]
    pub query_time_day: Option<Vec<String>>,

    #[serde(rename = "isQueryNativeLog")]
    pub is_query_native_log: String,

    #[serde(rename = "nativeLogLoc")]
    pub native_log_loc: Option<String>,

    #[serde(rename = "nativeLogResultLoc")]
    pub native_log_result_loc: Option<String>,

    #[serde(rename = "aggregatedLogResultLoc")]
    pub aggregated_log_result_loc: Option<String>,
}

impl Config {
    pub fn load(path: &str) -> Result<Self> {
        let content = fs::read_to_string(path)?;
        let config: Config = serde_yaml::from_str(&content)?;
        Ok(config)
    }
}
