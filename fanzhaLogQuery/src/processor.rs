use crate::matcher::{DomainMatcher, IPMatcher};
use anyhow::Result;
use flate2::read::GzDecoder;
use memchr::memchr_iter;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

// Constants for field indices (0-based)
const AGGREGATED_LOG_IP_INDEX: usize = 0;
const AGGREGATED_LOG_DOMAIN_INDEX: usize = 1;
const NATIVE_LOG_IP_INDEX: usize = 4;
const NATIVE_LOG_DOMAIN_INDEX: usize = 7;

pub struct FileProcessor {
    ip_matcher: IPMatcher,
    domain_matcher: DomainMatcher,
}

impl FileProcessor {
    pub fn new(ip_matcher: IPMatcher, domain_matcher: DomainMatcher) -> Self {
        Self {
            ip_matcher,
            domain_matcher,
        }
    }

    pub fn process_aggregated_file<P: AsRef<Path>, F>(&self, path: P, mut callback: F) -> Result<usize>
    where
        F: FnMut(&[u8]),
    {
        let file = File::open(path)?;
        let reader = BufReader::with_capacity(2 * 1024 * 1024, file);
        let decoder = GzDecoder::new(reader);
        let mut reader = BufReader::with_capacity(1024 * 1024, decoder);

        let filter_ip = !self.ip_matcher.is_none();
        let filter_domain = !self.domain_matcher.is_none();
        let mut match_count = 0;
        let mut line_buf = Vec::with_capacity(1024);

        loop {
            line_buf.clear();
            let bytes_read = reader.read_until(b'\n', &mut line_buf)?;
            if bytes_read == 0 {
                break;
            }

            if line_buf.last() == Some(&b'\n') {
                line_buf.pop();
            }
            if line_buf.is_empty() {
                continue;
            }

            // Optimization: Check IP first (Index 0) then Domain (Index 1)
            if filter_ip {
                if let Some(ip) = get_nth_field(&line_buf, b'|', AGGREGATED_LOG_IP_INDEX) {
                    if !self.ip_matcher.matches(ip) {
                        continue;
                    }
                } else {
                    continue;
                }
            }

            if filter_domain {
                if let Some(domain) = get_nth_field(&line_buf, b'|', AGGREGATED_LOG_DOMAIN_INDEX) {
                    if !self.domain_matcher.matches(domain) {
                        continue;
                    }
                } else {
                    continue;
                }
            }

            callback(&line_buf);
            match_count += 1;
        }

        Ok(match_count)
    }

    pub fn process_native_file<P: AsRef<Path>, F>(&self, path: P, mut callback: F) -> Result<usize>
    where
        F: FnMut(&[u8]),
    {
        let file = File::open(path)?;
        let reader = BufReader::with_capacity(2 * 1024 * 1024, file);
        let decoder = GzDecoder::new(reader);
        let mut reader = BufReader::with_capacity(1024 * 1024, decoder);

        let filter_ip = !self.ip_matcher.is_none();
        let filter_domain = !self.domain_matcher.is_none();
        let mut match_count = 0;
        let mut line_buf = Vec::with_capacity(1024);

        loop {
            line_buf.clear();
            let bytes_read = reader.read_until(b'\n', &mut line_buf)?;
            if bytes_read == 0 {
                break;
            }

            if line_buf.last() == Some(&b'\n') {
                line_buf.pop();
            }
            if line_buf.is_empty() {
                continue;
            }

            // Optimization: Check IP first (Index 4) then Domain (Index 7)
            if filter_ip {
                if let Some(ip) = get_nth_field(&line_buf, b'|', NATIVE_LOG_IP_INDEX) {
                    if !self.ip_matcher.matches(ip) {
                        continue;
                    }
                } else {
                    continue;
                }
            }

            if filter_domain {
                if let Some(domain) = get_nth_field(&line_buf, b'|', NATIVE_LOG_DOMAIN_INDEX) {
                    if !self.domain_matcher.matches(domain) {
                        continue;
                    }
                } else {
                    continue;
                }
            }

            callback(&line_buf);
            match_count += 1;
        }

        Ok(match_count)
    }
}

// Optimized field extraction using memchr
#[inline(always)]
fn get_nth_field(line: &[u8], separator: u8, n: usize) -> Option<&[u8]> {
    let mut iter = memchr_iter(separator, line);
    let mut start = 0;
    
    for _ in 0..n {
        match iter.next() {
            Some(idx) => start = idx + 1,
            None => return None,
        }
    }

    match iter.next() {
        Some(end) => Some(&line[start..end]),
        None => Some(&line[start..]),
    }
}
