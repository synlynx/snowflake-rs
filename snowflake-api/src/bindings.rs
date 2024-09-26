use std::any::type_name;
use std::fmt;
use std::fmt::{Formatter, Write};
use std::string::FromUtf8Error;
use bytes::{BufMut, BytesMut};
use serde::Serialize;
use thiserror::Error;
use crate::responses::SnowflakeType;



#[derive(Error, Debug)]
pub struct WrongType {
    snowflake: SnowflakeType,
    rust: &'static str
}

impl WrongType {
    pub fn new<T>(snowflake: SnowflakeType) -> Self {
        Self { snowflake, rust: type_name::<T>() }
    }
}
impl fmt::Display for WrongType {
    fn fmt(&self, fmt: &mut Formatter<'_>) -> fmt::Result {
        write!(
            fmt,
            "cannot convert between the Rust type `{}` and the snowflake type `{}`",
            self.rust, self.snowflake
        )
    }
}

#[derive(Error, Debug)]
pub enum BindingError {
    #[error(transparent)]
    WrongType(#[from] WrongType),
    #[error(transparent)]
    Utf8EncodingError(#[from] FromUtf8Error),
    #[error(transparent)]
    SerialisationError(#[from] serde_json::Error),
}

#[macro_export]
macro_rules! sql_type {
    ($type_param: expr) => {
        fn sql_type(&self) -> $crate::SnowflakeType {
            $type_param
        }
    };
}

#[macro_export]
macro_rules! default_encode {
    () => {
        fn encode_format(&self) -> String {
            "".into()
        }
    };
}

pub trait ToSql {

    fn sql_type(&self) -> SnowflakeType;

    fn to_sql(&self, out: &mut BytesMut) -> Result<Option<()>, BindingError>;

    fn encode_format(&self) -> String;
}


impl <'a, T> ToSql for &'a T
where
    T: ToSql {
    fn sql_type(&self) -> SnowflakeType {
        (*self).sql_type()
    }

    fn to_sql(&self, out: &mut BytesMut) -> Result<Option<()>, BindingError>
    {
        (*self).to_sql(out)
    }
    fn encode_format(&self) -> String {
        (*self).encode_format()
    }
}

impl ToSql for String {

    sql_type!(SnowflakeType::Text);
    default_encode!();

    fn to_sql(&self, out: &mut BytesMut) -> Result<Option<()>, BindingError>
    {
        let _ = out.write_str(&self);
        Ok(Some(()))
    }

}

impl ToSql for &str {

    sql_type!(SnowflakeType::Text);
    default_encode!();

    fn to_sql(&self, out: &mut BytesMut) -> Result<Option<()>, BindingError>
    {
        let _ = out.write_str(&self);
        Ok(Some(()))
    }
}


impl ToSql for char {

    sql_type!(SnowflakeType::Text);
    default_encode!();

    fn to_sql(&self, out: &mut BytesMut) -> Result<Option<()>, BindingError>
    {
        let _ = out.write_char(*self);
        Ok(Some(()))
    }
}


macro_rules! serializable_impl {
    ($impl_type: ty, $snowflake_type: expr) => {
        impl ToSql for $impl_type {

            sql_type!($snowflake_type);
            default_encode!();

            fn to_sql(&self, out: &mut BytesMut) -> Result<Option<()>, BindingError>
            {
                let _ = out.write_str(&self.to_string());
                Ok(Some(()))
            }
        }
    };
}


serializable_impl!(i8, SnowflakeType::Fixed);
serializable_impl!(u8, SnowflakeType::Fixed);
serializable_impl!(i16, SnowflakeType::Fixed);
serializable_impl!(u16, SnowflakeType::Fixed);
serializable_impl!(i32, SnowflakeType::Fixed);
serializable_impl!(u32, SnowflakeType::Fixed);
serializable_impl!(i64, SnowflakeType::Fixed);
serializable_impl!(u64, SnowflakeType::Fixed);

serializable_impl!(f32, SnowflakeType::Real);
serializable_impl!(f64, SnowflakeType::Real);

impl <T: ToSql> ToSql for Box<T> {
    fn sql_type(&self) -> SnowflakeType {
        T::sql_type(&*self)
    }

    fn to_sql(&self, out: &mut BytesMut) -> Result<Option<()>, BindingError>
    {
        T::to_sql(&*self, out)
    }

    fn encode_format(&self) -> String {
        T::encode_format(&*self)
    }
}

impl ToSql for Box<dyn ToSql> {
    fn sql_type(&self) -> SnowflakeType {
        self.as_ref().sql_type()
    }

    fn to_sql(&self, out: &mut BytesMut) -> Result<Option<()>, BindingError>
    {
        self.as_ref().to_sql(out)
    }

    fn encode_format(&self) -> String {
        self.as_ref().encode_format()
    }
}


pub struct Variant(Box<dyn ToSql>);
impl ToSql for Variant {
    sql_type!(SnowflakeType::Variant);

    fn encode_format(&self) -> String {
        self.0.encode_format()
    }

    fn to_sql(&self, out: &mut BytesMut) -> Result<Option<()>, BindingError>
    {
        Box::<dyn ToSql>::to_sql(&self.0, out)
    }
}

pub struct Object<T: Serialize>(T);
impl <T: Serialize> ToSql for Object<T> {
    sql_type!(SnowflakeType::Object);

    fn to_sql(&self, out: &mut BytesMut) -> Result<Option<()>, BindingError>
    {
        serde_json::to_writer(out.writer(), &self.0)?;
        Ok(Some(()))
    }

    fn encode_format(&self) -> String {
        "json".into()
    }
}


impl <T: ToSql> ToSql for Option<T> {
    fn sql_type(&self) -> SnowflakeType {
        self.as_ref().map(|e| e.sql_type())
            .unwrap_or(SnowflakeType::UnknownNull)
    }

    fn to_sql(&self, out: &mut BytesMut) -> Result<Option<()>, BindingError>
    {
        match &self {
            None => Ok(None),
            Some(s) => {
                s.to_sql(out)?;
                Ok(Some(()))
            }
        }
    }

    fn encode_format(&self) -> String {
        self.as_ref().map(|e| e.encode_format())
            .unwrap_or("".into())
    }
}

#[cfg(feature = "chrono")]
pub use chrono_impls::*;

#[cfg(feature = "chrono")]
mod chrono_impls {
    use std::fmt::Write;
    use bytes::BytesMut;
    use chrono::{FixedOffset, Local, Utc};
    use crate::bindings::BindingError;
    use crate::responses::SnowflakeType;
    use crate::ToSql;

    macro_rules! date_time_impl {
        ($impl_type: ty, $snowflake_type: expr, $format_str: expr) => {
            impl ToSql for $impl_type {

                sql_type!($snowflake_type);
                default_encode!();

                fn to_sql(&self, out: &mut BytesMut) -> Result<Option<()>, BindingError>
                {
                    let _ = out.write_str(&self.format($format_str).to_string());
                    Ok(Some(()))
                }
            }
        };
    }

    date_time_impl!(chrono::NaiveDate, SnowflakeType::Text, "%Y-%m-%d");
    date_time_impl!(chrono::NaiveDateTime, SnowflakeType::TimestampNtz, "%Y-%m-%d %H:%M:%S.%.3f");
    date_time_impl!(chrono::DateTime<Local>, SnowflakeType::TimestampLtz, "%Y-%m-%d %H:%M:%S.%.3f");
    date_time_impl!(chrono::DateTime<FixedOffset>, SnowflakeType::TimestampTz, "%Y-%m-%d %H:%M:%S.%.3f %z");
    date_time_impl!(chrono::DateTime<Utc>, SnowflakeType::TimestampTz, "%Y-%m-%d %H:%M:%S.%.3f %z");


}



