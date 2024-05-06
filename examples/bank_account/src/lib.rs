use anyhow::bail;
use kameo_es::{Command, Entity, EventType};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Default)]
pub struct BankAccount {
    balance: i64,
}

impl Entity for BankAccount {
    type Event = BankAccountEvent;
    type Metadata = Value;

    fn category() -> &'static str {
        "BankAccount"
    }

    fn apply(&mut self, event: Self::Event) {
        match event {
            BankAccountEvent::MoneyWithdrawn { amount } => self.balance -= amount as i64,
            BankAccountEvent::MoneyDeposited { amount } => self.balance += amount as i64,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum BankAccountEvent {
    MoneyWithdrawn { amount: u32 },
    MoneyDeposited { amount: u32 },
}

impl EventType for BankAccountEvent {
    fn event_type(&self) -> &'static str {
        match self {
            BankAccountEvent::MoneyWithdrawn { .. } => "MoneyWithdrawn",
            BankAccountEvent::MoneyDeposited { .. } => "MoneyDeposited",
        }
    }
}

#[derive(Clone)]
pub struct Withdraw {
    pub amount: u32,
}

impl Command<Withdraw> for BankAccount {
    type Error = anyhow::Error;

    fn handle(&self, cmd: Withdraw) -> Result<Vec<Self::Event>, Self::Error> {
        if self.balance < cmd.amount as i64 {
            bail!("insufficient balance");
        }

        Ok(vec![BankAccountEvent::MoneyWithdrawn {
            amount: cmd.amount,
        }])
    }
}

#[derive(Clone)]
pub struct Deposit {
    pub amount: u32,
}

impl Command<Deposit> for BankAccount {
    type Error = anyhow::Error;

    fn handle(&self, cmd: Deposit) -> Result<Vec<Self::Event>, Self::Error> {
        Ok(vec![BankAccountEvent::MoneyDeposited {
            amount: cmd.amount,
        }])
    }
}
