use msd_table::{Table, Variant};

use crate::errors::DbError;

/// ChanItem is a field how to send to other Table
#[derive(Debug, PartialEq)]
enum ChanItem {
  /// Copy the field with the index
  Copy { id: usize },
  /// Copy the field with value in id is changed, otherwise copy the field with value in no_change_id
  ChangedIf {
    id: usize,
    no_change_id: usize,
    prev: Variant,
  },
}

impl ChanItem {
  /// parse a chan item from string
  ///
  /// # Arguments
  ///
  /// * `tokens` - list of token that had been split by ',' and trimmed
  /// * `fields` - The fields of the table
  ///
  /// # Returns
  ///
  /// * `Ok((ChanItem, usize))` - The parsed ChanItem and how many tokens had been consumed
  /// * `Err(DbError)` - The error
  fn parse(tokens: &[&str], fields: &[&str]) -> Result<(Self, usize), DbError> {
    if tokens.is_empty() {
      return Err(DbError::ChanFormatError("empty chan".to_string()));
    }
    if tokens[0].starts_with("changed_if") {
      let end = 2;
      if tokens.len() < end || !tokens[1].ends_with(')') {
        return Err(DbError::ChanFormatError(format!(
          "invalid changed_if format: {}",
          tokens.join(",")
        )));
      }
      let id = Self::field_index(
        &tokens[0]
          .trim_start_matches("changed_if")
          .trim()
          .trim_start_matches('(')
          .trim(),
        fields,
      )?;
      let no_change_id = Self::field_index(&tokens[1].trim_end_matches(')'), fields)?;
      Ok((
        ChanItem::ChangedIf {
          id,
          no_change_id,
          prev: Variant::Null,
        },
        end,
      ))
    } else {
      let id = Self::field_index(tokens[0], fields)?;
      Ok((Self::Copy { id }, 1))
    }
  }

  fn field_index(field: &str, fields: &[&str]) -> Result<usize, DbError> {
    fields
      .iter()
      .position(|f| f.eq_ignore_ascii_case(field))
      .ok_or(DbError::ChanFormatError(format!(
        "field '{}' not found",
        field
      )))
  }
}

/// Chan is a list of ChanItem, describe how a row in current Table send to other Table
#[derive(Debug, Default)]
pub struct Chan {
  items: Vec<ChanItem>,
  target: Vec<String>,
}

impl Chan {
  pub fn apply(&mut self, row: &Vec<Variant>) -> Vec<Variant> {
    let mut new_row = Vec::with_capacity(self.items.len());
    for item in &mut self.items {
      match item {
        ChanItem::Copy { id } => new_row.push(row[*id].clone()),
        ChanItem::ChangedIf {
          id,
          no_change_id,
          prev,
        } => {
          if row[*id] != *prev {
            new_row.push(row[*id].clone());
            *prev = row[*id].clone();
          } else {
            new_row.push(row[*no_change_id].clone());
          }
        }
      }
    }
    new_row
  }

  pub fn tables(&self) -> &[String] {
    self.target.as_slice()
  }

  pub fn table(&self) -> Option<&String> {
    self.target.first()
  }

  pub fn match_target(&self, target: &Table) -> bool {
    self.items.len() == target.columns().len()
  }

  pub fn parse_targets(chan_str: &str) -> Result<Vec<&str>, DbError> {
    let target = chan_str
      .split_once(':')
      .map(|s| s.0.trim())
      .map(|s| {
        s.split(',')
          .map(|s| s.trim())
          .filter(|s| !s.is_empty())
          .collect::<Vec<_>>()
      })
      .ok_or(DbError::ChanFormatError("invalid chan format".to_string()))?;
    if target.is_empty() {
      return Err(DbError::ChanFormatError("empty target".to_string()));
    }
    Ok(target)
  }

  /// parse str to Chan
  ///
  /// char_str format: "target1,target2:field1,field2,changed_if(field3,field1),field4"
  /// ## Arguments
  ///
  /// * `chan_str` - The string to parse
  /// * `fields` - The fields of the table
  ///
  /// ## Returns
  ///
  /// * `Ok(Chan)` - The parsed Chan
  /// * `Err(DbError)` - The error
  pub fn parse(chan_str: &str, fields: &[&str]) -> Result<Self, DbError> {
    let (target, chan_str) = chan_str
      .split_once(':')
      .ok_or(DbError::ChanFormatError("invalid chan format".to_string()))?;

    let target = target
      .split(',')
      .map(|s| s.trim())
      .filter(|s| !s.is_empty())
      .map(|s| s.to_string())
      .collect::<Vec<_>>();

    if target.is_empty() {
      return Err(DbError::ChanFormatError("empty target".to_string()));
    }

    let tokens = chan_str
      .split(',')
      .map(|s| s.trim())
      .filter(|s| !s.is_empty())
      .collect::<Vec<_>>();

    if tokens.is_empty() {
      return Err(DbError::ChanFormatError("empty chan".to_string()));
    }

    let mut items = Vec::with_capacity(tokens.len());
    let mut i = 0;
    while i < tokens.len() {
      let (item, end) = ChanItem::parse(&tokens[i..], &fields)?;
      items.push(item);
      i += end;
    }
    Ok(Self { items, target })
  }
}

impl TryFrom<&Table> for Chan {
  type Error = DbError;

  fn try_from(table: &Table) -> Result<Self, Self::Error> {
    let chan_str = table
      .get_table_meta("chan")
      .and_then(|v| v.get_str())
      .unwrap_or_default();
    if chan_str.is_empty() {
      return Ok(Chan::default());
    }

    let fields = table
      .columns()
      .iter()
      .map(|f| f.name.as_str())
      .collect::<Vec<_>>();

    Chan::parse(chan_str, &fields)
  }
}

#[cfg(test)]
mod tests {
  use msd_table::v;

  use super::*;

  #[test]
  fn test_chan_parse_ok() {
    let chan_str = "table1, table2: field1, field2, changed_if ( field3 ,field1), field4";
    let fields = vec!["field1", "field2", "field3", "field4"];
    let chan = Chan::parse(chan_str, &fields).unwrap();
    assert_eq!(chan.target, vec!["table1", "table2"]);
    assert_eq!(chan.items.len(), 4);
    assert_eq!(chan.items[0], ChanItem::Copy { id: 0 });
    assert_eq!(chan.items[1], ChanItem::Copy { id: 1 });
    assert_eq!(
      chan.items[2],
      ChanItem::ChangedIf {
        id: 2,
        no_change_id: 0,
        prev: Variant::Null
      }
    );
    assert_eq!(chan.items[3], ChanItem::Copy { id: 3 });
  }

  #[test]
  fn test_chan_parse_err() {
    let fields = vec!["field1", "field2", "field3", "field4"];
    // empty is error
    assert!(Chan::parse("", &fields).is_err());
    // missing target is error
    assert!(
      Chan::parse(
        ": field1, field2, changed_if ( field3 ,field1), field4",
        &fields
      )
      .is_err()
    );
    assert!(
      Chan::parse(
        "field1, field2, changed_if ( field3 ,field1), field4",
        &fields
      )
      .is_err()
    );
    // missing chan is error
    assert!(Chan::parse("table1, table2:", &fields).is_err());

    // field not found is error
    assert!(
      Chan::parse(
        "table1, table2: field1, field5, changed_if ( field3 ,field1), field4",
        &fields
      )
      .is_err()
    );
  }

  #[test]
  fn test_chan_apply() {
    let fields = vec!["open", "high", "low", "close", "field1", "field2"];
    let mut chan = Chan::parse(
      "kline: changed_if ( open ,close), changed_if ( high ,close), changed_if ( low ,close), close",
      &fields,
    )
    .unwrap();
    // first row: should same as input
    let new_row = chan.apply(&vec![v!(2.0), v!(1.0), v!(10.0), v!(4.0), v!(5.0), v!(6.0)]);
    assert_eq!(new_row, vec![v!(2.0), v!(1.0), v!(10.0), v!(4.0)]);

    // second row: changed field should same as input
    let new_row = chan.apply(&vec![v!(2.0), v!(0.5), v!(11.0), v!(9.0), v!(6.0), v!(6.0)]);
    assert_eq!(new_row, vec![v!(9.0), v!(0.5), v!(11.0), v!(9.0)]);

    // third row: no changed field should same as close
    let new_row = chan.apply(&vec![
      v!(2.0),
      v!(0.5),
      v!(11.0),
      v!(10.0),
      v!(6.0),
      v!(6.0),
    ]);
    assert_eq!(new_row, vec![v!(10.0), v!(10.0), v!(10.0), v!(10.0)]);
  }
}
