use std::net::IpAddr;
use std::str::FromStr;
use cidr::IpCidr;
use anyhow::Result;

#[derive(Debug)]
pub enum IPMatcher {
    Exact(String),
    Cidr(IpCidr),
    Range(IpAddr, IpAddr),
    None,
}

impl IPMatcher {
    pub fn new(input: &str) -> Result<Self> {
        if input.is_empty() {
            return Ok(IPMatcher::None);
        }

        // Try CIDR
        if input.contains('/') {
            if let Ok(cidr) = IpCidr::from_str(input) {
                return Ok(IPMatcher::Cidr(cidr));
            }
        }

        // Try Range
        if input.contains('-') {
            let parts: Vec<&str> = input.split('-').collect();
            if parts.len() == 2 {
                let start = IpAddr::from_str(parts[0].trim())?;
                let end = IpAddr::from_str(parts[1].trim())?;
                return Ok(IPMatcher::Range(start, end));
            }
        }

        // Default Exact
        Ok(IPMatcher::Exact(input.to_string()))
    }

    pub fn matches(&self, ip_bytes: &[u8]) -> bool {
        match self {
            IPMatcher::None => true,
            IPMatcher::Exact(target) => ip_bytes == target.as_bytes(),
            IPMatcher::Cidr(cidr) => {
                // Convert bytes to string then to IpAddr (Optimization point: avoid alloc)
                // For high perf, we should parse bytes directly to IP, but let's start safe.
                if let Ok(s) = std::str::from_utf8(ip_bytes) {
                    if let Ok(ip) = IpAddr::from_str(s) {
                        return cidr.contains(&ip);
                    }
                }
                false
            }
            IPMatcher::Range(start, end) => {
                if let Ok(s) = std::str::from_utf8(ip_bytes) {
                    if let Ok(ip) = IpAddr::from_str(s) {
                        return ip >= *start && ip <= *end;
                    }
                }
                false
            }
        }
    }
    
    pub fn is_none(&self) -> bool {
        matches!(self, IPMatcher::None)
    }
}

#[derive(Debug)]
pub enum DomainMatcher {
    Exact(Vec<u8>),
    Wildcard(Vec<u8>), // Suffix
    None,
}

impl DomainMatcher {
    pub fn new(input: &str) -> Self {
        if input.is_empty() {
            return DomainMatcher::None;
        }
        if input.starts_with("*.") {
            return DomainMatcher::Wildcard(input[2..].as_bytes().to_vec());
        }
        DomainMatcher::Exact(input.as_bytes().to_vec())
    }

    pub fn matches(&self, domain: &[u8]) -> bool {
        match self {
            DomainMatcher::None => true,
            DomainMatcher::Exact(target) => domain == target.as_slice(),
            DomainMatcher::Wildcard(suffix) => {
                if domain.len() < suffix.len() {
                    return false;
                }
                // Check suffix
                if !domain.ends_with(suffix) {
                    return false;
                }
                // Check boundary: either exact match suffix or char before suffix is '.'
                domain.len() == suffix.len() || domain[domain.len() - suffix.len() - 1] == b'.'
            }
        }
    }

    pub fn is_none(&self) -> bool {
        matches!(self, DomainMatcher::None)
    }
}
