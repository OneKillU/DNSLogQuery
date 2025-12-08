use std::net::IpAddr;
use std::str::FromStr;
use cidr::IpCidr;
use anyhow::Result;

#[derive(Debug)]
enum IPRule {
    Exact(String),
    Cidr(IpCidr),
    Range(IpAddr, IpAddr),
    Prefix(Vec<u8>), // Optimization for /8, /16, /24
}

impl IPRule {
    fn parse(input: &str) -> Result<Self> {
        // Try CIDR
        if input.contains('/') {
            if let Ok(cidr) = IpCidr::from_str(input) {
                // Optimization: Convert common IPv4 CIDRs to prefix matches
                if let IpCidr::V4(v4_cidr) = cidr {
                    let mask = v4_cidr.network_length();
                    let ip = v4_cidr.first_address();
                    let octets = ip.octets();
                    
                    if mask == 24 {
                        let prefix = format!("{}.{}.{}.", octets[0], octets[1], octets[2]);
                        return Ok(IPRule::Prefix(prefix.into_bytes()));
                    } else if mask == 16 {
                        let prefix = format!("{}.{}.", octets[0], octets[1]);
                        return Ok(IPRule::Prefix(prefix.into_bytes()));
                    } else if mask == 8 {
                        let prefix = format!("{}.", octets[0]);
                        return Ok(IPRule::Prefix(prefix.into_bytes()));
                    }
                }
                return Ok(IPRule::Cidr(cidr));
            }
        }

        // Try Range
        if input.contains('-') {
            let parts: Vec<&str> = input.split('-').collect();
            if parts.len() == 2 {
                let start = IpAddr::from_str(parts[0].trim())?;
                let end = IpAddr::from_str(parts[1].trim())?;
                return Ok(IPRule::Range(start, end));
            }
        }

        // Default Exact
        Ok(IPRule::Exact(input.to_string()))
    }

    fn matches(&self, ip_bytes: &[u8]) -> bool {
        match self {
            IPRule::Exact(target) => ip_bytes == target.as_bytes(),
            IPRule::Prefix(prefix) => ip_bytes.starts_with(prefix),
            IPRule::Cidr(cidr) => {
                if let Some(ip) = parse_ip_from_bytes(ip_bytes) {
                    return cidr.contains(&ip);
                }
                false
            }
            IPRule::Range(start, end) => {
                if let Some(ip) = parse_ip_from_bytes(ip_bytes) {
                    return ip >= *start && ip <= *end;
                }
                false
            }
        }
    }
}

#[inline(always)]
fn parse_ip_from_bytes(bytes: &[u8]) -> Option<IpAddr> {
    // Try fast path for IPv4
    // IPv4 typically: d.d.d.d, max length 15.
    if bytes.len() > 15 {
        // Fallback for IPv6 or others
        return if let Ok(s) = std::str::from_utf8(bytes) {
            IpAddr::from_str(s).ok()
        } else {
            None
        };
    }

    let mut octets = [0u8; 4];
    let mut octet_idx = 0;
    let mut current = 0u16;
    let mut has_digit = false;

    for &b in bytes {
        if b == b'.' {
            if !has_digit || octet_idx >= 3 || current > 255 {
                 // Invalid IPv4 format, try fallback
                 // But since we checked len <= 15 and encountered '.', unlikely to be IPv6
                 return None; 
            }
            octets[octet_idx] = current as u8;
            octet_idx += 1;
            current = 0;
            has_digit = false;
        } else if b >= b'0' && b <= b'9' {
            current = current * 10 + (b - b'0') as u16;
            has_digit = true;
        } else {
            // Non-digit, non-dot. Could be IPv6 (':') or garbage.
            // Since we are in the <= 15 length block, handle fallback.
            if let Ok(s) = std::str::from_utf8(bytes) {
                return IpAddr::from_str(s).ok();
            } else {
                return None;
            }
        }
    }
    
    // Check last octet
    if octet_idx == 3 && has_digit && current <= 255 {
        octets[3] = current as u8;
        Some(IpAddr::V4(std::net::Ipv4Addr::new(octets[0], octets[1], octets[2], octets[3])))
    } else {
        None
    }
}

#[derive(Debug)]
pub struct IPMatcher {
    rules: Vec<IPRule>,
}

impl IPMatcher {
    pub fn new(inputs: &[String]) -> Result<Self> {
        let mut rules = Vec::new();
        for input in inputs {
            if !input.trim().is_empty() {
                rules.push(IPRule::parse(input)?);
            }
        }
        Ok(IPMatcher { rules })
    }

    pub fn matches(&self, ip_bytes: &[u8]) -> bool {
        if self.rules.is_empty() {
            return true;
        }
        self.rules.iter().any(|rule| rule.matches(ip_bytes))
    }

    pub fn is_none(&self) -> bool {
        self.rules.is_empty()
    }
}

#[derive(Debug)]
enum DomainRule {
    Exact(Vec<u8>),
    Wildcard(Vec<u8>), // Suffix
}

impl DomainRule {
    fn parse(input: &str) -> Self {
        if input.starts_with("*.") {
            DomainRule::Wildcard(input[2..].as_bytes().to_vec())
        } else {
            DomainRule::Exact(input.as_bytes().to_vec())
        }
    }

    fn matches(&self, domain: &[u8]) -> bool {
        match self {
            DomainRule::Exact(target) => domain == target.as_slice(),
            DomainRule::Wildcard(suffix) => {
                if domain.len() < suffix.len() {
                    return false;
                }
                if !domain.ends_with(suffix) {
                    return false;
                }
                domain.len() == suffix.len() || domain[domain.len() - suffix.len() - 1] == b'.'
            }
        }
    }
}

#[derive(Debug)]
pub struct DomainMatcher {
    rules: Vec<DomainRule>,
}

impl DomainMatcher {
    pub fn new(inputs: &[String]) -> Self {
        let mut rules = Vec::new();
        for input in inputs {
            if !input.trim().is_empty() {
                rules.push(DomainRule::parse(input));
            }
        }
        DomainMatcher { rules }
    }

    pub fn matches(&self, domain: &[u8]) -> bool {
        if self.rules.is_empty() {
            return true;
        }
        self.rules.iter().any(|rule| rule.matches(domain))
    }

    pub fn is_none(&self) -> bool {
        self.rules.is_empty()
    }
}
