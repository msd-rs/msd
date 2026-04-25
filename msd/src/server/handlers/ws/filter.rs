use super::message::{Notify, Subscribe};

#[derive(Default, Debug)]
struct ObjTrieNode {
  children: Vec<(u8, usize)>,
  exact: i32,
  prefix: i32,
}

#[derive(Debug)]
struct ObjTrie {
  nodes: Vec<ObjTrieNode>,
}

impl Default for ObjTrie {
  fn default() -> Self {
    Self {
      nodes: vec![ObjTrieNode::default()],
    }
  }
}

impl ObjTrie {
  fn find_child(&self, curr: usize, b: u8) -> Option<usize> {
    let children = &self.nodes[curr].children;
    children
      .binary_search_by_key(&b, |&(k, _)| k)
      .ok()
      .map(|i| children[i].1)
  }

  fn add_child(&mut self, curr: usize, b: u8) -> usize {
    let children = &self.nodes[curr].children;
    match children.binary_search_by_key(&b, |&(k, _)| k) {
      Ok(i) => children[i].1,
      Err(i) => {
        let next = self.nodes.len();
        self.nodes.push(ObjTrieNode::default());
        self.nodes[curr].children.insert(i, (b, next));
        next
      }
    }
  }

  fn insert(&mut self, obj: &[u8], is_prefix: bool, delta: i32) {
    let mut curr = 0;
    for &b in obj {
      curr = self.add_child(curr, b);
    }
    if is_prefix {
      self.nodes[curr].prefix += delta;
    } else {
      self.nodes[curr].exact += delta;
    }
  }

  fn eval(&self, obj: &[u8]) -> Option<bool> {
    let mut result = None;
    let mut curr = 0;

    let node = &self.nodes[curr];
    if node.prefix > 0 {
      result = Some(true);
    } else if node.prefix < 0 {
      result = Some(false);
    }

    for &b in obj {
      if let Some(next) = self.find_child(curr, b) {
        curr = next;
        let node = &self.nodes[curr];
        if node.prefix > 0 {
          result = Some(true);
        } else if node.prefix < 0 {
          result = Some(false);
        }
      } else {
        return result;
      }
    }

    let node = &self.nodes[curr];
    if node.exact > 0 {
      result = Some(true);
    } else if node.exact < 0 {
      result = Some(false);
    }

    result
  }
}

#[derive(Default, Debug)]
struct TableTrieNode {
  children: Vec<(u8, usize)>,
  exact_objs: ObjTrie,
  prefix_objs: ObjTrie,
}

#[derive(Debug)]
struct TableTrie {
  nodes: Vec<TableTrieNode>,
}

impl Default for TableTrie {
  fn default() -> Self {
    Self {
      nodes: vec![TableTrieNode::default()],
    }
  }
}

impl TableTrie {
  fn new() -> Self {
    Self::default()
  }

  fn find_child(&self, curr: usize, b: u8) -> Option<usize> {
    let children = &self.nodes[curr].children;
    children
      .binary_search_by_key(&b, |&(k, _)| k)
      .ok()
      .map(|i| children[i].1)
  }

  fn add_child(&mut self, curr: usize, b: u8) -> usize {
    let children = &self.nodes[curr].children;
    match children.binary_search_by_key(&b, |&(k, _)| k) {
      Ok(i) => children[i].1,
      Err(i) => {
        let next = self.nodes.len();
        self.nodes.push(TableTrieNode::default());
        self.nodes[curr].children.insert(i, (b, next));
        next
      }
    }
  }

  fn insert(
    &mut self,
    table: &[u8],
    is_table_prefix: bool,
    obj: &[u8],
    is_obj_prefix: bool,
    delta: i32,
  ) {
    let mut curr = 0;
    for &b in table {
      curr = self.add_child(curr, b);
    }
    if is_table_prefix {
      self.nodes[curr]
        .prefix_objs
        .insert(obj, is_obj_prefix, delta);
    } else {
      self.nodes[curr]
        .exact_objs
        .insert(obj, is_obj_prefix, delta);
    }
  }

  fn eval(&self, table: &[u8], obj: &[u8]) -> bool {
    let mut result = false;
    let mut curr = 0;

    if let Some(r) = self.nodes[curr].prefix_objs.eval(obj) {
      result = r;
    }

    for &b in table {
      if let Some(next) = self.find_child(curr, b) {
        curr = next;
        if let Some(r) = self.nodes[curr].prefix_objs.eval(obj) {
          result = r;
        }
      } else {
        return result;
      }
    }

    if let Some(r) = self.nodes[curr].exact_objs.eval(obj) {
      result = r;
    }

    result
  }
}

pub struct Filter {
  trie: TableTrie,
}

impl Filter {
  pub fn new() -> Self {
    Self {
      trie: TableTrie::new(),
    }
  }

  fn parse_pattern(s: &str) -> (&[u8], bool) {
    if s.ends_with('*') {
      (s[..s.len() - 1].as_bytes(), true)
    } else {
      (s.as_bytes(), false)
    }
  }

  pub fn subscribe(&mut self, sub: &Subscribe) {
    let (table_pat, is_table_prefix) = Self::parse_pattern(&sub.table);
    for obj in &sub.objs {
      let (obj_pat, is_obj_prefix) = Self::parse_pattern(obj);
      self
        .trie
        .insert(table_pat, is_table_prefix, obj_pat, is_obj_prefix, 1);
    }
  }

  pub fn unsubscribe(&mut self, sub: &Subscribe) {
    let (table_pat, is_table_prefix) = Self::parse_pattern(&sub.table);
    for obj in &sub.objs {
      let (obj_pat, is_obj_prefix) = Self::parse_pattern(obj);
      self
        .trie
        .insert(table_pat, is_table_prefix, obj_pat, is_obj_prefix, -1);
    }
  }

  pub fn is_allowed(&self, msg: &Notify) -> bool {
    self.trie.eval(msg.table.as_bytes(), msg.obj.as_bytes())
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_filter_exact() {
    let mut filter = Filter::new();
    filter.subscribe(&Subscribe {
      table: "trade".to_string(),
      objs: vec!["SH600000".to_string()],
    });
    assert!(filter.is_allowed(&Notify {
      table: "trade".to_string(),
      obj: "SH600000".to_string(),
      ..Default::default()
    }));
    assert!(!filter.is_allowed(&Notify {
      table: "trade".to_string(),
      obj: "SH600001".to_string(),
      ..Default::default()
    }));
  }

  #[test]
  fn test_filter_prefix() {
    let mut filter = Filter::new();
    filter.subscribe(&Subscribe {
      table: "trade*".to_string(),
      objs: vec!["SH*".to_string()],
    });
    assert!(filter.is_allowed(&Notify {
      table: "trade_sh".to_string(),
      obj: "SH600000".to_string(),
      ..Default::default()
    }));
    assert!(!filter.is_allowed(&Notify {
      table: "quote".to_string(),
      obj: "SH600000".to_string(),
      ..Default::default()
    }));
  }

  #[test]
  fn test_filter_exclusion() {
    let mut filter = Filter::new();
    filter.subscribe(&Subscribe {
      table: "trade".to_string(),
      objs: vec!["SH6*".to_string()],
    });
    filter.unsubscribe(&Subscribe {
      table: "trade".to_string(),
      objs: vec!["SH600000".to_string()],
    });
    assert!(filter.is_allowed(&Notify {
      table: "trade".to_string(),
      obj: "SH600001".to_string(),
      ..Default::default()
    }));
    assert!(!filter.is_allowed(&Notify {
      table: "trade".to_string(),
      obj: "SH600000".to_string(),
      ..Default::default()
    }));
  }

  #[test]
  fn test_filter_multi_levels() {
    let mut filter = Filter::new();
    filter.subscribe(&Subscribe {
      table: "*".to_string(),
      objs: vec!["*".to_string()],
    });
    filter.unsubscribe(&Subscribe {
      table: "trade".to_string(),
      objs: vec!["SH*".to_string()],
    });
    filter.subscribe(&Subscribe {
      table: "trade".to_string(),
      objs: vec!["SH600*".to_string()],
    });
    filter.unsubscribe(&Subscribe {
      table: "trade".to_string(),
      objs: vec!["SH600000".to_string()],
    });

    // Other tables
    assert!(filter.is_allowed(&Notify {
      table: "quote".to_string(),
      obj: "SH600000".to_string(),
      ..Default::default()
    }));

    // Excluded prefix
    assert!(!filter.is_allowed(&Notify {
      table: "trade".to_string(),
      obj: "SH123456".to_string(),
      ..Default::default()
    }));

    // Re-included sub-prefix
    assert!(filter.is_allowed(&Notify {
      table: "trade".to_string(),
      obj: "SH600001".to_string(),
      ..Default::default()
    }));

    // Specifically excluded exact
    assert!(!filter.is_allowed(&Notify {
      table: "trade".to_string(),
      obj: "SH600000".to_string(),
      ..Default::default()
    }));
  }
}
