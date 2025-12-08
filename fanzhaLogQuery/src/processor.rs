use crate::matcher::{DomainMatcher, IPMatcher};
use anyhow::Result;
use flate2::read::MultiGzDecoder;
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

    pub fn process_aggregated_file<P: AsRef<Path>, F>(&self, path: P, callback: F) -> Result<usize>
    where
        F: FnMut(&[u8]),
    {
        let file = File::open(path)?;
        let reader = BufReader::with_capacity(2 * 1024 * 1024, file);
        self.process_reader(reader, callback)
    }

    pub fn process_aggregated_data<F>(&self, data: &[u8], callback: F) -> Result<usize>
    where
        F: FnMut(&[u8]),
    {
        let reader = BufReader::with_capacity(2 * 1024 * 1024, data);
        self.process_reader(reader, callback)
    }

    fn process_reader<R: std::io::Read, F>(&self, reader: R, mut callback: F) -> Result<usize>
    where
        F: FnMut(&[u8]),
    {
        let decoder = MultiGzDecoder::new(reader);
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
            if line_buf.last() == Some(&b'\r') {
                line_buf.pop();
            }
            if line_buf.is_empty() {
                continue;
            }

            if self.check_line(&line_buf, filter_ip, filter_domain, AGGREGATED_LOG_IP_INDEX, AGGREGATED_LOG_DOMAIN_INDEX) {
                callback(&line_buf);
                match_count += 1;
            }
        }
        Ok(match_count)
    }

    pub fn process_native_file<P: AsRef<Path>, F>(&self, path: P, callback: F) -> Result<usize>
    where
        F: FnMut(&[u8]),
    {
        let file = File::open(path)?;
        let reader = BufReader::with_capacity(2 * 1024 * 1024, file);
        self.process_native_reader(reader, callback)
    }

    pub fn process_native_data<F>(&self, data: &[u8], callback: F) -> Result<usize>
    where
        F: FnMut(&[u8]),
    {
        let reader = BufReader::with_capacity(2 * 1024 * 1024, data);
        self.process_native_reader(reader, callback)
    }

    fn process_native_reader<R: std::io::Read, F>(&self, reader: R, mut callback: F) -> Result<usize>
    where
        F: FnMut(&[u8]),
    {
        let decoder = MultiGzDecoder::new(reader);
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
            if line_buf.last() == Some(&b'\r') {
                line_buf.pop();
            }
            if line_buf.is_empty() {
                continue;
            }

            if self.check_line(&line_buf, filter_ip, filter_domain, NATIVE_LOG_IP_INDEX, NATIVE_LOG_DOMAIN_INDEX) {
                callback(&line_buf);
                match_count += 1;
            }
        }
        Ok(match_count)
    }

    #[inline(always)]
    fn check_line(&self, line: &[u8], filter_ip: bool, filter_domain: bool, ip_idx: usize, domain_idx: usize) -> bool {
        // If no filters, match everything (though usually we have at least one)
        if !filter_ip && !filter_domain {
            return true;
        }

        let mut ip_matched = !filter_ip;
        let mut domain_matched = !filter_domain;

        let mut iter = memchr_iter(b'|', line);
        let mut current_idx = 0;
        let mut start = 0;

        // Optimization: Determine max index we need to reach
        let max_idx = if filter_ip && filter_domain {
            std::cmp::max(ip_idx, domain_idx)
        } else if filter_ip {
            ip_idx
        } else {
            domain_idx
        };

        while let Some(end) = iter.next() {
            if current_idx == ip_idx && filter_ip {
                let field = &line[start..end];
                if self.ip_matcher.matches(field) {
                    ip_matched = true;
                }
            }
            if current_idx == domain_idx && filter_domain {
                let field = &line[start..end];
                if self.domain_matcher.matches(field) {
                    domain_matched = true;
                }
            }

            if ip_matched && domain_matched {
                return true;
            }

            if current_idx >= max_idx {
                break;
            }

            start = end + 1;
            current_idx += 1;
        }

        // Handle the last field if it's the one we need
        if current_idx <= max_idx {
             let field = &line[start..];
             if current_idx == ip_idx && filter_ip {
                if self.ip_matcher.matches(field) {
                    ip_matched = true;
                }
            }
            if current_idx == domain_idx && filter_domain {
                if self.domain_matcher.matches(field) {
                    domain_matched = true;
                }
            }
        }

        ip_matched && domain_matched
    }
}
