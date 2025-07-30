use std::fmt;
use std::str::FromStr;

/// “master” or “slave”
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Master,
    Slave,
}

impl Role {
    /// for INFO and other outputs
    pub fn as_str(&self) -> &'static str {
        match self {
            Role::Master => "master",
            Role::Slave => "slave",
        }
    }
}

impl fmt::Display for Role {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for Role {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "master" => Ok(Role::Master),
            "slave" => Ok(Role::Slave),
            other => Err(format!("invalid role: {}", other)),
        }
    }
}
