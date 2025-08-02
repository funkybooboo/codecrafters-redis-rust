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
        let lower = s.to_lowercase();
        println!("[Role::from_str] Parsing role from input: '{}'", lower);
        match lower.as_str() {
            "master" => {
                println!("[Role::from_str] Parsed successfully: master");
                Ok(Role::Master)
            }
            "slave" => {
                println!("[Role::from_str] Parsed successfully: slave");
                Ok(Role::Slave)
            }
            other => {
                eprintln!("[Role::from_str] Invalid role string: {}", other);
                Err(format!("invalid role: {other}"))
            }
        }
    }
}
