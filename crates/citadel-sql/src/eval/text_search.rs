use crate::error::{Result, SqlError};
use crate::fts::{self, TokenizerKind};
use crate::types::{DataType, Value};

#[derive(Clone, Copy)]
pub(super) enum Constructor {
    Vector,
    Query,
    PlainQuery,
    PhraseQuery,
    WebQuery,
}

impl Constructor {
    pub(super) fn from_name(name: &str) -> Option<Self> {
        match name {
            "TO_TSVECTOR" => Some(Self::Vector),
            "TO_TSQUERY" => Some(Self::Query),
            "PLAINTO_TSQUERY" => Some(Self::PlainQuery),
            "PHRASETO_TSQUERY" => Some(Self::PhraseQuery),
            "WEBSEARCH_TO_TSQUERY" => Some(Self::WebQuery),
            _ => None,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Vector => "to_tsvector",
            Self::Query => "to_tsquery",
            Self::PlainQuery => "plainto_tsquery",
            Self::PhraseQuery => "phraseto_tsquery",
            Self::WebQuery => "websearch_to_tsquery",
        }
    }

    pub(super) fn result_type(self) -> DataType {
        match self {
            Self::Vector => DataType::TsVector,
            Self::Query | Self::PlainQuery | Self::PhraseQuery | Self::WebQuery => {
                DataType::TsQuery
            }
        }
    }

    pub(super) fn evaluate(
        self,
        args: &[Value],
        cancel: Option<&citadel::CancelToken>,
    ) -> Result<Value> {
        let (config, text) = match args {
            [text] => (None, text),
            [config, text] => (Some(config), text),
            _ => {
                return Err(SqlError::InvalidValue(format!(
                    "{} requires 1 or 2 arguments",
                    self.name()
                )))
            }
        };
        if args.iter().any(Value::is_null) {
            return Ok(Value::Null);
        }
        let config_name = match config {
            Some(Value::Text(name)) => Some(name.as_str()),
            Some(value) => {
                return Err(SqlError::TypeMismatch {
                    expected: "TEXT (config)".into(),
                    got: value.data_type().to_string(),
                })
            }
            None => None,
        };
        let Value::Text(text) = text else {
            return Err(SqlError::TypeMismatch {
                expected: "TEXT".into(),
                got: text.data_type().to_string(),
            });
        };
        let kind = match config_name {
            Some(name) => TokenizerKind::from_name(name)?,
            None => TokenizerKind::English,
        };
        match self {
            Self::Vector => fts::fn_to_tsvector_with_cancel(kind, text, cancel),
            Self::Query => fts::fn_to_tsquery_with_cancel(kind, text, cancel),
            Self::PlainQuery => fts::fn_plainto_tsquery_with_cancel(kind, text, cancel),
            Self::PhraseQuery => fts::fn_phraseto_tsquery_with_cancel(kind, text, cancel),
            Self::WebQuery => fts::fn_websearch_to_tsquery_with_cancel(kind, text, cancel),
        }
    }
}
