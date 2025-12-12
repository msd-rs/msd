//! SQL parser for MSD requests.
//!

use crate::sql::msd_dialect::MsdSqlDialect;
use crate::{DeleteRequest, InsertRequest, QueryRequest, RequestError};

use msd_table::{DataType as TableDataType, Field, Table, Variant};
use sqlparser::ast::{
  BinaryOperator, ColumnOption, CreateTableOptions, Expr, FromTable, Ident, LimitClause,
  ObjectName, Query, Select, SelectItem, SetExpr, Statement, TableFactor, Value, ValueWithSpan,
};
use sqlparser::parser::Parser;

mod msd_dialect;

use crate::{AggStateId, InsertData, RequestKey};
use std::collections::{HashMap, HashSet};

#[derive(Debug)]
pub enum SqlRequest {
  Query(QueryRequest),
  CreateTable(String, Table),
  Insert(InsertRequest),
  Delete(DeleteRequest),
}

#[derive(Debug)]
pub enum SqlRequestType {
  Unknown,
  Query,
  CreateTable,
  Insert,
  Delete,
}

/// Determine the type of SQL request, based on the first command word.
/// Only supports single-statement SQL.
/// It's a quick check before full parsing to test if the SQL is supported.
pub fn sql_request_type(sql: &str) -> SqlRequestType {
  if let Some((first_line, _)) = sql.split_once('\n') {
    let command = first_line.split_whitespace().next().unwrap_or("");
    if command.eq_ignore_ascii_case("COPY") {
      return SqlRequestType::Insert;
    } else if command.eq_ignore_ascii_case("INSERT") {
      return SqlRequestType::Insert;
    } else if command.eq_ignore_ascii_case("SELECT") {
      return SqlRequestType::Query;
    } else if command.eq_ignore_ascii_case("CREATE") {
      return SqlRequestType::CreateTable;
    } else if command.eq_ignore_ascii_case("DELETE") {
      return SqlRequestType::Delete;
    }
  }
  return SqlRequestType::Unknown;
}

/// Parse SQL string to a list of SqlRequest
pub fn sql_to_request(sql: &str) -> Result<Vec<SqlRequest>, RequestError> {
  let sql = sql.trim();

  if let Some((first_line, rest)) = sql.split_once('\n') {
    let mut part = first_line.split_whitespace();
    if part.next().map(|s| s.eq_ignore_ascii_case("COPY")) == Some(true) {
      while let Some(token) = part.next() {
        // skip empty tokens
        if token.is_empty() {
          continue;
        }
        // next non-empty token is table name
        return parse_copy(token, rest);
      }
    }
  }

  let dialect = MsdSqlDialect {};
  let ast = Parser::parse_sql(&dialect, sql)?;

  let r = ast
    .into_iter()
    .map(parse_stmt)
    .collect::<Result<Vec<_>, _>>()?;
  Ok(r.into_iter().flatten().collect())
}

fn parse_stmt(stmt: Statement) -> Result<Vec<SqlRequest>, RequestError> {
  match stmt {
    Statement::Insert(_) => parse_insert(stmt),
    Statement::Query(_) => parse_query(stmt),
    Statement::CreateTable(_) => parse_create_table(stmt),
    Statement::Delete(_) => parse_delete(stmt),
    _ => Err(RequestError::UnsupportedSqlStatement),
  }
}

/// Parse INSERT statement
/// values will be grouped by object name (obj), which is the fixed first column in the table
/// Example:
/// INSERT INTO kline1d VALUES
///   ('SH600000', '2023-01-01', 100.0, 110.0, 90.0, 105.0),
///   ('SH600000', '2023-01-02', 106.0, 112.0, 91.0, 108.0),
///   ('SZ000001', '2023-01-01', 50.0, 55.0, 45.0, 52.0);
/// will be parsed to two InsertRequest with obj 'SH600000' and 'SZ000001'
///
/// Columns definition is ignored in this function, assuming the order of values matches the table schema
///
/// COPY statement will be treated as a variant of INSERT statement, only CSV from STDIN is supported
/// the cvs data will be like the values in INSERT statement
fn parse_insert(stmt: Statement) -> Result<Vec<SqlRequest>, RequestError> {
  match stmt {
    Statement::Insert(insert) => {
      let table = insert.table.to_string();
      let columns = insert.columns;
      let source = insert.source.ok_or(RequestError::UnsupportedSqlStatement)?;
      let obj_idx = columns
        .iter()
        .position(|c| c.value.eq_ignore_ascii_case("obj"))
        .unwrap_or(0);

      let mut rows: Vec<SqlRequest> = Vec::new();
      let mut current_obj = String::default();
      let mut current_rows = Vec::new();
      match *source.body {
        SetExpr::Values(values) => {
          for row in values.rows {
            let mut obj = String::new();
            let mut parsed_row = Vec::with_capacity(row.len());
            for (idx, expr) in row.into_iter().enumerate() {
              if idx == obj_idx {
                obj = expr_to_string(expr)?;
                continue;
              }
              parsed_row.push(expr_to_variant(expr, columns.get(idx))?);
            }
            if parsed_row.is_empty() {
              continue;
            }
            if obj != current_obj && !current_obj.is_empty() {
              let req = SqlRequest::Insert(InsertRequest {
                key: RequestKey::new(&table, &current_obj),
                data: InsertData::Rows(std::mem::take(&mut current_rows)),
              });
              rows.push(req);
            }
            current_obj = obj;
            current_rows.push(parsed_row);
          }
          if current_rows.len() > 0 {
            let req = SqlRequest::Insert(InsertRequest {
              key: RequestKey::new(&table, &current_obj),
              data: InsertData::Rows(std::mem::take(&mut current_rows)),
            });
            rows.push(req);
          }
          Ok(rows)
        }
        _ => return Err(RequestError::UnsupportedSqlStatement),
      }
    }
    _ => Err(RequestError::UnsupportedSqlStatement),
  }
}

fn parse_query(stmt: Statement) -> Result<Vec<SqlRequest>, RequestError> {
  match stmt {
    Statement::Query(query) => parse_query_inner(*query).map(|req| vec![req]),
    _ => Err(RequestError::UnsupportedSqlStatement),
  }
}

fn parse_delete(stmt: Statement) -> Result<Vec<SqlRequest>, RequestError> {
  match stmt {
    Statement::Delete(delete) => {
      // Try 'from' field if 'tables' is empty, or assume 'from' is the correct one for DELETE FROM
      let tables = match delete.from {
        FromTable::WithFromKeyword(tables) => tables,
        FromTable::WithoutKeyword(tables) => tables,
      };

      if tables.len() != 1 {
        return Err(RequestError::UnsupportedSqlStatement);
      }

      let table_name = match &tables[0].relation {
        TableFactor::Table { name, .. } => object_name_to_string(name),
        _ => return Err(RequestError::UnsupportedSqlStatement),
      };

      // Note: sqlparser Delete struct: pub tables: Vec<TableFactor>
      // DELETE [FROM] table_name [WHERE ...]

      let mut req = DeleteRequest {
        key: RequestKey::new(table_name.clone(), "".to_string()),
        date_range: Default::default(),
      };
      let mut objects = HashSet::new();
      let mut object = String::default();

      if let Some(selection) = delete.selection {
        parse_filter_common(selection, &mut object, &mut objects, &mut req.date_range)?;
      }

      if !objects.is_empty() {
        if !object.is_empty() {
          objects.insert(object);
        }
        Ok(
          objects
            .into_iter()
            .map(|obj| {
              SqlRequest::Delete(DeleteRequest {
                key: RequestKey::new(table_name.clone(), obj),
                date_range: req.date_range.clone(),
              })
            })
            .collect(),
        )
      } else {
        req.key.obj = object;
        Ok(vec![SqlRequest::Delete(req)])
      }
    }
    _ => Err(RequestError::UnsupportedSqlStatement),
  }
}

fn parse_query_inner(query: Query) -> Result<SqlRequest, RequestError> {
  let Query {
    body,
    order_by,
    limit_clause,
    ..
  } = query;

  let select = match *body {
    SetExpr::Select(select) => select,
    _ => return Err(RequestError::UnsupportedSqlStatement),
  };

  let Select {
    projection,
    from,
    selection,
    ..
  } = *select;

  let table_name = from
    .first()
    .and_then(|t| match &t.relation {
      TableFactor::Table { name, .. } => Some(object_name_to_string(name)),
      _ => None,
    })
    .ok_or(RequestError::UnsupportedSqlStatement)?;

  let mut req = QueryRequest::default();
  req.key.table = table_name;
  req.fields = parse_projection(&projection);
  let mut objects = HashSet::new();

  if let Some(expr) = selection {
    parse_filter_common(expr, &mut req.key.obj, &mut objects, &mut req.date_range)?;
  }

  if let Some(order) = parse_order_by(order_by.as_ref()) {
    req.ascending = Some(order);
  }

  if let Some(limit) = limit_clause {
    if let LimitClause::LimitOffset { limit, offset, .. } = limit {
      if let Some(limit_expr) = limit {
        if let Some(limit_value) = expr_to_usize(&limit_expr)? {
          req.limit = Some(limit_value);
        }
      }

      if let Some(off) = offset {
        if let Some(off_val) = expr_to_usize(&off.value)? {
          req.limit = req.limit.map(|l| l + off_val).or(Some(off_val));
        }
      }
    }
  }
  if !objects.is_empty() {
    if !req.key.obj.is_empty() {
      objects.insert(std::mem::take(&mut req.key.obj));
    }
    req.objects = Some(objects.into_iter().collect());
  }

  Ok(SqlRequest::Query(req))
}

fn parse_create_table(stmt: Statement) -> Result<Vec<SqlRequest>, RequestError> {
  match stmt {
    Statement::CreateTable(ct) => {
      let name = ct.name;
      let columns = ct.columns;
      let mut fields: Vec<Field> = Vec::with_capacity(columns.len());
      for col in columns {
        let dtype = sql_datatype_to_table(&col.data_type)?;
        let mut field = Field::new(col.name.value.clone(), dtype, 0);

        let mut metadata: HashMap<String, Variant> = HashMap::new();
        for opt in col.options {
          if let ColumnOption::DialectSpecific(tokens) = opt.option {
            if let Some(agg) = tokens
              .first()
              .and_then(|t| match t {
                sqlparser::tokenizer::Token::Word(w) => Some(w.value.clone()),
                _ => None,
              })
              .as_deref()
            {
              if let Some(agg_id) = agg_keyword_to_state(agg) {
                metadata.insert("agg".into(), Variant::String(agg_id.to_string()));
              }
            }
          }
        }

        if !metadata.is_empty() {
          field.metadata = Some(metadata);
        }

        fields.push(field);
      }

      let mut table = Table::from_columns(fields);
      let mut meta: HashMap<String, Variant> = HashMap::new();
      if let CreateTableOptions::With(options) = ct.table_options {
        for opt in options {
          let kv = opt.to_string();
          if let Some((k, v_raw)) = kv.split_once('=') {
            let key = k.trim();
            let v_clean = v_raw.trim().trim_matches('"').trim_matches('\'');
            let value = if key.eq_ignore_ascii_case("chunksize") {
              Variant::UInt32(v_clean.parse::<u32>().unwrap_or_default())
            } else if let Ok(n) = v_clean.parse::<f64>() {
              Variant::Float64(n)
            } else {
              Variant::String(v_clean.to_string())
            };
            meta.insert(key.to_string(), value);
          }
        }
      }

      if !meta.is_empty() {
        table = table.with_metadata(meta);
      }

      Ok(vec![SqlRequest::CreateTable(
        object_name_to_string(&name),
        table,
      )])
    }
    _ => Err(RequestError::UnsupportedSqlStatement),
  }
}

fn parse_copy(table_name: &str, csv_data: &str) -> Result<Vec<SqlRequest>, RequestError> {
  let mut rows = Vec::new();
  let mut current_obj = "";
  let mut current_rows = String::new();
  for line in csv_data.lines() {
    let line = line.trim();
    if line.is_empty() {
      continue;
    }
    if let Some((obj, values)) = line.split_once(',') {
      let obj = obj.trim().trim_matches(|c| c == '\'' || c == '"');
      if current_obj != "" && current_obj != obj {
        let req = SqlRequest::Insert(InsertRequest {
          key: RequestKey::new(table_name, current_obj),
          data: InsertData::Csv(std::mem::take(&mut current_rows)),
        });
        rows.push(req);
      }
      current_obj = obj;
      if !current_rows.is_empty() {
        current_rows.push('\n');
      }
      current_rows.push_str(values);
    }
  }
  if !current_rows.is_empty() {
    let req = SqlRequest::Insert(InsertRequest {
      key: RequestKey::new(table_name, current_obj),
      data: InsertData::Csv(std::mem::take(&mut current_rows)),
    });
    rows.push(req);
  }
  Ok(rows)
}

fn parse_projection(items: &[SelectItem]) -> Option<Vec<String>> {
  if items
    .iter()
    .any(|item| matches!(item, SelectItem::Wildcard(_)))
  {
    return None;
  }

  let mut fields = Vec::new();
  for item in items {
    match item {
      SelectItem::UnnamedExpr(Expr::Identifier(ident)) => fields.push(ident.value.clone()),
      SelectItem::ExprWithAlias { expr, alias } => match expr {
        Expr::Identifier(_) => fields.push(alias.value.clone()),
        _ => return None,
      },
      _ => return None,
    }
  }

  Some(fields)
}

fn parse_filter_common(
  expr: Expr,
  obj: &mut String,
  objects: &mut HashSet<String>,
  date_range: &mut crate::DateRange,
) -> Result<(), RequestError> {
  match expr {
    Expr::BinaryOp { left, op, right } => {
      if op == BinaryOperator::And {
        parse_filter_common(*left, obj, objects, date_range)?;
        parse_filter_common(*right, obj, objects, date_range)?;
        return Ok(());
      }

      match (*left, *right) {
        (Expr::Identifier(ident), right_expr) => {
          apply_predicate_common(ident, op, right_expr, obj, date_range)?;
        }
        (left_expr, Expr::Identifier(ident)) => {
          apply_predicate_common(ident, op, left_expr, obj, date_range)?;
        }
        _ => {}
      }
      Ok(())
    }
    Expr::InList { expr, list, .. } => {
      let expr = expr_to_string(*expr)?;
      if expr == "obj" {
        for item in list {
          let item = expr_to_string(item)?;
          objects.insert(item);
        }
      }
      Ok(())
    }
    _ => Ok(()),
  }
}

fn apply_predicate_common(
  ident: Ident,
  op: BinaryOperator,
  value_expr: Expr,
  obj: &mut String,
  date_range: &mut crate::DateRange,
) -> Result<(), RequestError> {
  let name = ident.value;
  match name.to_ascii_lowercase().as_str() {
    "obj" if op == BinaryOperator::Eq => {
      *obj = expr_to_string(value_expr)?;
    }
    "ts" => {
      let ts = expr_to_datetime(value_expr)?;
      match op {
        BinaryOperator::Gt => date_range.start = Some((ts, false)),
        BinaryOperator::GtEq => date_range.start = Some((ts, true)),
        BinaryOperator::Lt => date_range.end = Some((ts, false)),
        BinaryOperator::LtEq => date_range.end = Some((ts, true)),
        _ => {}
      }
    }
    _ => {}
  }
  Ok(())
}

fn parse_order_by(order_by: Option<&sqlparser::ast::OrderBy>) -> Option<bool> {
  let ob = order_by?;
  let repr = ob.to_string().to_ascii_uppercase();
  if repr.contains("DESC") {
    Some(false)
  } else if repr.contains("ASC") {
    Some(true)
  } else {
    None
  }
}

fn expr_to_variant(expr: Expr, col: Option<&Ident>) -> Result<Variant, RequestError> {
  match expr {
    Expr::Value(v) => value_to_variant(v.value, col),
    _ => Ok(Variant::Null),
  }
}

fn expr_to_string(expr: Expr) -> Result<String, RequestError> {
  match expr {
    Expr::Value(v) => match v.value {
      Value::Number(s, _) => Ok(s),
      _ => v.into_string().ok_or(RequestError::UnsupportedSqlStatement),
    },
    Expr::Identifier(ident) => Ok(ident.value),
    _ => Err(RequestError::UnsupportedSqlStatement),
  }
}

fn expr_to_datetime(expr: Expr) -> Result<i64, RequestError> {
  let s = expr_to_string(expr)?;
  msd_table::parse_datetime(&s).map_err(RequestError::from)
}

fn expr_to_usize(expr: &Expr) -> Result<Option<usize>, RequestError> {
  match expr {
    Expr::Value(ValueWithSpan {
      value: Value::Number(s, _),
      ..
    }) => Ok(s.parse::<usize>().ok()),
    _ => Ok(None),
  }
}

fn value_to_variant(value: Value, col: Option<&Ident>) -> Result<Variant, RequestError> {
  match value {
    Value::Null => Ok(Variant::Null),
    Value::Number(s, _) => {
      if s.contains('.') || s.contains('e') || s.contains('E') {
        Ok(Variant::Float64(s.parse::<f64>().unwrap_or_default()))
      } else {
        Ok(Variant::Int64(s.parse::<i64>().unwrap_or_default()))
      }
    }
    Value::SingleQuotedString(s) => {
      if let Some(col) = col {
        if col.value.eq_ignore_ascii_case("ts") {
          return msd_table::parse_datetime(&s)
            .map(Variant::DateTime)
            .map_err(RequestError::from);
        }
      }
      Ok(Variant::String(s))
    }
    Value::Boolean(b) => Ok(Variant::Bool(b)),
    _ => Ok(Variant::Null),
  }
}

fn sql_datatype_to_table(dt: &sqlparser::ast::DataType) -> Result<TableDataType, RequestError> {
  let dtype = dt.to_string().to_ascii_uppercase();

  if dtype.contains("TIME") && dtype.contains("DATE") {
    return Ok(TableDataType::DateTime);
  }

  if dtype.contains("DECIMAL128") {
    return Ok(TableDataType::Decimal128);
  }

  if dtype.contains("DECIMAL64") {
    return Ok(TableDataType::Decimal64);
  }

  if dtype.starts_with('U') {
    return Ok(TableDataType::UInt64);
  }

  if dtype.starts_with("BOOL") {
    return Ok(TableDataType::Bool);
  }

  if dtype.starts_with("F32") || dtype.starts_with("FLOAT32") {
    return Ok(TableDataType::Float32);
  }

  if dtype.starts_with('F') || dtype.starts_with("DOUBLE") {
    return Ok(TableDataType::Float64);
  }

  if dtype.starts_with('I') || dtype.contains("INT") {
    return Ok(TableDataType::Int64);
  }

  if dtype.contains("CHAR") || dtype.contains("STRING") || dtype.contains("TEXT") {
    return Ok(TableDataType::String);
  }

  Ok(TableDataType::String)
}

fn agg_keyword_to_state(kw: &str) -> Option<AggStateId> {
  match kw.to_ascii_uppercase().as_str() {
    "AGG_FIRST" => Some(AggStateId::First),
    "AGG_MIN" => Some(AggStateId::Min),
    "AGG_MAX" => Some(AggStateId::Max),
    "AGG_SUM" => Some(AggStateId::Sum),
    "AGG_COUNT" => Some(AggStateId::Count),
    "AGG_AVG" => Some(AggStateId::Avg),
    "AGG_UNIQ_COUNT" => Some(AggStateId::UniqCount),
    _ => None,
  }
}

fn object_name_to_string(name: &ObjectName) -> String {
  name.to_string()
}

#[cfg(test)]
mod tests;
