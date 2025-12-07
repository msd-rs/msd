use sqlparser::{
  ast::ColumnOption,
  dialect::Dialect,
  keywords::Keyword,
  parser::{Parser, ParserError},
  tokenizer::Token,
};

#[derive(Debug, Default)]
pub struct MsdSqlDialect;

const AGGREGATE_KEYWORDS: &[&str] = &[
  "AGG_FIRST",
  "AGG_MIN",
  "AGG_MAX",
  "AGG_SUM",
  "AGG_COUNT",
  "AGG_AVG",
  "AGG_UNIQ_COUNT",
];

impl Dialect for MsdSqlDialect {
  // see https://www.sqlite.org/lang_keywords.html
  // parse `...`, [...] and "..." as identifier
  // TODO: support depending on the context tread '...' as identifier too.
  fn is_delimited_identifier_start(&self, ch: char) -> bool {
    ch == '`' || ch == '"' || ch == '['
  }

  fn identifier_quote_style(&self, _identifier: &str) -> Option<char> {
    Some('`')
  }

  fn is_identifier_start(&self, ch: char) -> bool {
    // See https://www.sqlite.org/draft/tokenreq.html
    ch.is_ascii_lowercase()
      || ch.is_ascii_uppercase()
      || ch == '_'
      || ('\u{007f}'..='\u{ffff}').contains(&ch)
  }

  fn supports_filter_during_aggregation(&self) -> bool {
    true
  }

  fn supports_start_transaction_modifier(&self) -> bool {
    true
  }

  fn is_identifier_part(&self, ch: char) -> bool {
    self.is_identifier_start(ch) || ch.is_ascii_digit()
  }

  fn supports_limit_comma(&self) -> bool {
    true
  }

  // msd specific column options
  // AGG_*: aggregate function, will be parse to a [`super::AggStateId`] to msd table column metadata as 'agg' key
  fn parse_column_option(
    &self,
    parser: &mut Parser,
  ) -> Result<Option<Result<Option<ColumnOption>, ParserError>>, ParserError> {
    let t = parser.peek_token();
    println!("parse_column_option: {:?}", t);
    match t.token {
      Token::Word(ref w) if w.keyword == Keyword::NoKeyword => {
        if AGGREGATE_KEYWORDS
          .iter()
          .any(|&kw| kw.eq_ignore_ascii_case(&w.value))
        {
          parser.next_token();
          Ok(Some(Ok(Some(ColumnOption::DialectSpecific(vec![t.token])))))
        } else {
          Ok(None)
        }
      }
      _ => Ok(None),
    }
  }
}
