use std::collections::BTreeMap;
use bytes::BytesMut;
use serde::Serialize;
use crate::bindings::{BindingError, ToSql};
use crate::responses::SnowflakeType;


#[derive(Serialize, Debug)]
pub struct EmptyRequest {}



#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase", untagged)]
pub enum BindingValue {
    SingleBind(String),
    MultiBind(Vec<String>)
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ParameterBinding {
    #[serde(rename = "type")]
    pub type_: Option<SnowflakeType>,
    pub fmt: Option<String>,
    pub value: BindingValue
}

impl TryFrom<Box<dyn ToSql>> for ParameterBinding {
    type Error = BindingError;

    fn try_from(value: Box<dyn ToSql>) -> Result<Self, Self::Error> {
        let mut buffer = BytesMut::new();
        value.to_sql(&mut buffer)?;
        let buffer = buffer.freeze();

        let params = String::from_utf8(buffer.to_vec())?;
        Ok(ParameterBinding {
            type_: Some(value.sql_type()),
            fmt: None,
            value: BindingValue::SingleBind(params)
        })
    }
}

pub type Bindings = BTreeMap<String, ParameterBinding>;

#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ExecRequest {
    pub sql_text: String,
    pub async_exec: bool,
    pub sequence_id: u64,
    pub is_internal: bool,
    pub bindings: Option<Bindings>
}

#[derive(Serialize, Debug)]
pub struct LoginRequest<T> {
    pub data: T,
}

pub type PasswordLoginRequest = LoginRequest<PasswordRequestData>;
#[cfg(feature = "cert-auth")]
pub type CertLoginRequest = LoginRequest<CertRequestData>;

#[derive(Serialize, Debug)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub struct LoginRequestCommon {
    pub client_app_id: String,
    pub client_app_version: String,
    pub svn_revision: String,
    pub account_name: String,
    pub login_name: String,
    pub session_parameters: SessionParameters,
    pub client_environment: ClientEnvironment,
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub struct SessionParameters {
    pub client_validate_default_parameters: bool,
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub struct ClientEnvironment {
    pub application: String,
    pub os: String,
    pub os_version: String,
    pub ocsp_mode: String,
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub struct PasswordRequestData {
    #[serde(flatten)]
    pub login_request_common: LoginRequestCommon,
    pub password: String,
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub struct CertRequestData {
    #[serde(flatten)]
    pub login_request_common: LoginRequestCommon,
    pub authenticator: String,
    pub token: String,
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct RenewSessionRequest {
    pub old_session_token: String,
    pub request_type: String,
}
