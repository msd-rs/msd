// Copyright 2026 MSD-RS Project LiJia
// SPDX-License-Identifier: agpl-3.0-only

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::str::FromStr;

use anyhow::{Result, bail};

/// Trait to convert various IP/Socket address types into an `IpAddr`.
pub trait IntoIpAddr {
  fn into_ip_addr(self) -> IpAddr;
}

impl IntoIpAddr for IpAddr {
  #[inline]
  fn into_ip_addr(self) -> IpAddr {
    self
  }
}

impl IntoIpAddr for &IpAddr {
  #[inline]
  fn into_ip_addr(self) -> IpAddr {
    *self
  }
}

impl IntoIpAddr for Ipv4Addr {
  #[inline]
  fn into_ip_addr(self) -> IpAddr {
    IpAddr::V4(self)
  }
}

impl IntoIpAddr for &Ipv4Addr {
  #[inline]
  fn into_ip_addr(self) -> IpAddr {
    IpAddr::V4(*self)
  }
}

impl IntoIpAddr for Ipv6Addr {
  #[inline]
  fn into_ip_addr(self) -> IpAddr {
    IpAddr::V6(self)
  }
}

impl IntoIpAddr for &Ipv6Addr {
  #[inline]
  fn into_ip_addr(self) -> IpAddr {
    IpAddr::V6(*self)
  }
}

impl IntoIpAddr for SocketAddr {
  #[inline]
  fn into_ip_addr(self) -> IpAddr {
    self.ip()
  }
}

impl IntoIpAddr for &SocketAddr {
  #[inline]
  fn into_ip_addr(self) -> IpAddr {
    self.ip()
  }
}

/// An IP whitelist containing merged, sorted IPv4 and IPv6 numerical ranges
/// for high-performance $O(\log N)$ matching.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IPList {
  // Sorted and merged non-overlapping closed ranges: [start, end]
  v4_ranges: Vec<(u32, u32)>,
  v6_ranges: Vec<(u128, u128)>,
}

impl IPList {
  /// Parse from a string with entries separated by comma, semicolon, or whitespace.
  /// Each entry can be an exact IP (e.g. `127.0.0.1`, `::1`) or a CIDR subnet (e.g. `192.168.0.0/16`).
  pub fn parse(s: &str) -> Result<Self> {
    let tokens = s
      .split([',', ';', ' '])
      .map(|t| t.trim())
      .filter(|t| !t.is_empty());
    Self::from_items(tokens)
  }

  /// Construct an `IPList` from an iterator of string items.
  pub fn from_items<I, S>(items: I) -> Result<Self>
  where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
  {
    let mut v4_ranges = Vec::new();
    let mut v6_ranges = Vec::new();

    for item in items {
      let item_str = item.as_ref().trim();
      if item_str.is_empty() {
        continue;
      }

      if let Some((ip_str, prefix_str)) = item_str.split_once('/') {
        let prefix: u8 = prefix_str.trim().parse().map_err(|_| {
          anyhow::anyhow!("Invalid CIDR prefix length in '{}'", item_str)
        })?;

        if let Ok(ipv4) = Ipv4Addr::from_str(ip_str.trim()) {
          if prefix > 32 {
            bail!("IPv4 CIDR prefix must be <= 32 in '{}'", item_str);
          }
          let ip_num = u32::from(ipv4);
          let mask = if prefix == 0 {
            0
          } else {
            !0u32 << (32 - prefix)
          };
          let start = ip_num & mask;
          let end = start | !mask;
          v4_ranges.push((start, end));
        } else if let Ok(ipv6) = Ipv6Addr::from_str(ip_str.trim()) {
          if prefix > 128 {
            bail!("IPv6 CIDR prefix must be <= 128 in '{}'", item_str);
          }
          let ip_num = u128::from(ipv6);
          let mask = if prefix == 0 {
            0
          } else {
            !0u128 << (128 - prefix)
          };
          let start = ip_num & mask;
          let end = start | !mask;
          v6_ranges.push((start, end));
        } else {
          bail!("Invalid IP address before '/' in '{}'", item_str);
        }
      } else {
        // Direct IP address without CIDR prefix
        let ip: IpAddr = item_str
          .parse()
          .map_err(|_| anyhow::anyhow!("Invalid IP address or CIDR: '{}'", item_str))?;
        match ip {
          IpAddr::V4(ipv4) => {
            let n = u32::from(ipv4);
            v4_ranges.push((n, n));
          }
          IpAddr::V6(ipv6) => {
            let n = u128::from(ipv6);
            v6_ranges.push((n, n));
          }
        }
      }
    }

    Ok(Self {
      v4_ranges: merge_ranges_u32(v4_ranges),
      v6_ranges: merge_ranges_u128(v6_ranges),
    })
  }

  /// Check whether the given IP or Socket address is in the whitelist.
  pub fn contains(&self, ip: impl IntoIpAddr) -> bool {
    match ip.into_ip_addr() {
      IpAddr::V4(v4) => contains_u32(&self.v4_ranges, u32::from(v4)),
      IpAddr::V6(v6) => {
        if contains_u128(&self.v6_ranges, u128::from(v6)) {
          return true;
        }
        // Handle IPv4-mapped IPv6 address (e.g. ::ffff:192.168.1.1)
        if let Some(v4) = v6.to_ipv4_mapped() {
          return contains_u32(&self.v4_ranges, u32::from(v4));
        }
        false
      }
    }
  }

  /// Returns true if whitelist is empty (contains no IP ranges).
  #[inline]
  #[allow(dead_code)]
  pub fn is_empty(&self) -> bool {
    self.v4_ranges.is_empty() && self.v6_ranges.is_empty()
  }

  /// Number of merged IPv4 and IPv6 ranges.
  #[inline]
  #[allow(dead_code)]
  pub fn len(&self) -> usize {
    self.v4_ranges.len() + self.v6_ranges.len()
  }
}

impl FromStr for IPList {
  type Err = anyhow::Error;

  fn from_str(s: &str) -> Result<Self, Self::Err> {
    Self::parse(s)
  }
}

/// Helper function for clap argument parsing
pub fn parse_auth_whitelist(s: &str) -> Result<IPList> {
  IPList::parse(s)
}

fn contains_u32(ranges: &[(u32, u32)], val: u32) -> bool {
  ranges
    .binary_search_by(|&(start, end)| {
      if val < start {
        std::cmp::Ordering::Greater
      } else if val > end {
        std::cmp::Ordering::Less
      } else {
        std::cmp::Ordering::Equal
      }
    })
    .is_ok()
}

fn contains_u128(ranges: &[(u128, u128)], val: u128) -> bool {
  ranges
    .binary_search_by(|&(start, end)| {
      if val < start {
        std::cmp::Ordering::Greater
      } else if val > end {
        std::cmp::Ordering::Less
      } else {
        std::cmp::Ordering::Equal
      }
    })
    .is_ok()
}

fn merge_ranges_u32(mut ranges: Vec<(u32, u32)>) -> Vec<(u32, u32)> {
  if ranges.is_empty() {
    return Vec::new();
  }
  ranges.sort_unstable_by_key(|&(s, _)| s);
  let mut merged = Vec::with_capacity(ranges.len());
  let mut curr = ranges[0];

  for next in ranges.into_iter().skip(1) {
    if next.0 <= curr.1.saturating_add(1) {
      curr.1 = curr.1.max(next.1);
    } else {
      merged.push(curr);
      curr = next;
    }
  }
  merged.push(curr);
  merged
}

fn merge_ranges_u128(mut ranges: Vec<(u128, u128)>) -> Vec<(u128, u128)> {
  if ranges.is_empty() {
    return Vec::new();
  }
  ranges.sort_unstable_by_key(|&(s, _)| s);
  let mut merged = Vec::with_capacity(ranges.len());
  let mut curr = ranges[0];

  for next in ranges.into_iter().skip(1) {
    if next.0 <= curr.1.saturating_add(1) {
      curr.1 = curr.1.max(next.1);
    } else {
      merged.push(curr);
      curr = next;
    }
  }
  merged.push(curr);
  merged
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::net::SocketAddr;

  #[test]
  fn test_empty_iplist() {
    let list = IPList::parse("").unwrap();
    assert!(list.is_empty());
    assert!(!list.contains("127.0.0.1".parse::<IpAddr>().unwrap()));
  }

  #[test]
  fn test_exact_ips() {
    let list = IPList::parse("127.0.0.1, 192.168.1.100; ::1").unwrap();
    assert!(list.contains("127.0.0.1".parse::<IpAddr>().unwrap()));
    assert!(list.contains("192.168.1.100".parse::<IpAddr>().unwrap()));
    assert!(list.contains("::1".parse::<IpAddr>().unwrap()));
    assert!(!list.contains("192.168.1.101".parse::<IpAddr>().unwrap()));
    assert!(!list.contains("::2".parse::<IpAddr>().unwrap()));
  }

  #[test]
  fn test_cidr_ranges() {
    let list = IPList::parse("192.168.0.0/16, 10.0.0.0/8").unwrap();
    assert!(list.contains("192.168.0.1".parse::<IpAddr>().unwrap()));
    assert!(list.contains("192.168.255.254".parse::<IpAddr>().unwrap()));
    assert!(list.contains("10.123.45.67".parse::<IpAddr>().unwrap()));
    assert!(!list.contains("192.169.0.1".parse::<IpAddr>().unwrap()));
    assert!(!list.contains("11.0.0.1".parse::<IpAddr>().unwrap()));
  }

  #[test]
  fn test_overlapping_and_adjacent_merge() {
    // 192.168.1.0/24 and 192.168.2.0/24 are adjacent
    let list = IPList::parse("192.168.1.0/24, 192.168.2.0/24, 192.168.1.50").unwrap();
    assert_eq!(list.v4_ranges.len(), 1);
    assert_eq!(list.v4_ranges[0], (
      u32::from(Ipv4Addr::new(192, 168, 1, 0)),
      u32::from(Ipv4Addr::new(192, 168, 2, 255))
    ));
    assert!(list.contains(Ipv4Addr::new(192, 168, 1, 10)));
    assert!(list.contains(Ipv4Addr::new(192, 168, 2, 20)));
    assert!(!list.contains(Ipv4Addr::new(192, 168, 3, 1)));
  }

  #[test]
  fn test_socket_addr_and_ipv4_mapped() {
    let list = IPList::parse("192.168.1.0/24").unwrap();
    let sock: SocketAddr = "192.168.1.42:8080".parse().unwrap();
    assert!(list.contains(sock));
    assert!(list.contains(&sock));

    // IPv4-mapped IPv6 address ::ffff:192.168.1.42
    let mapped_ip: IpAddr = "::ffff:192.168.1.42".parse().unwrap();
    assert!(list.contains(mapped_ip));
  }

  #[test]
  fn test_ipv6_cidr() {
    let list = IPList::parse("2001:db8::/32").unwrap();
    assert!(list.contains("2001:db8::1".parse::<IpAddr>().unwrap()));
    assert!(list.contains("2001:db8:ffff:ffff::1".parse::<IpAddr>().unwrap()));
    assert!(!list.contains("2001:db9::1".parse::<IpAddr>().unwrap()));
  }

  #[test]
  fn test_invalid_input() {
    assert!(IPList::parse("not-an-ip").is_err());
    assert!(IPList::parse("192.168.1.1/33").is_err());
    assert!(IPList::parse("::1/129").is_err());
  }
}
