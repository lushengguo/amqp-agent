use rusqlite::{params, Connection, Result};
use crate::amqp::Message;

pub struct DB {
    conn: Connection,
}

impl DB {
    pub fn new() -> Result<Self> {
        let conn = Connection::open("messages.db")?;
        // 创建表（如果不存在）
        conn.execute(
            "CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT NOT NULL,
                exchange TEXT NOT NULL,
                routing_key TEXT NOT NULL,
                message TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )",
            [],
        )?;
        Ok(Self { conn })
    }

    pub fn insert(&mut self, message: &Message) -> Result<()> {
        self.conn.execute(
            "INSERT INTO messages (url, exchange, routing_key, message, timestamp) 
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                message.url,
                message.exchange,
                message.routing_key,
                message.message,
                message.timestamp
            ],
        )?;
        Ok(())
    }

    pub fn batch_insert(&mut self, messages: &[Message]) -> Result<()> {
        let tx = self.conn.transaction()?;
        {
            let mut stmt = tx.prepare(
                "INSERT INTO messages (url, exchange, routing_key, message, timestamp) 
                 VALUES (?1, ?2, ?3, ?4, ?5)"
            )?;
            
            for message in messages {
                stmt.execute(params![
                    message.url,
                    message.exchange,
                    message.routing_key,
                    message.message,
                    message.timestamp
                ])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    pub fn get_recent_messages(&self, limit: u64) -> Result<Vec<Message>> {
        let mut stmt = self.conn.prepare(
            "SELECT url, exchange, routing_key, message, timestamp 
             FROM messages 
             ORDER BY created_at DESC 
             LIMIT ?"
        )?;
        
        let messages = stmt.query_map(
            params![limit],
            |row| {
                Ok(Message {
                    url: row.get(0)?,
                    exchange: row.get(1)?,
                    routing_key: row.get(2)?,
                    message: row.get(3)?,
                    timestamp: row.get(4)?,
                })
            },
        )?
        .collect::<Result<Vec<Message>>>()?;

        Ok(messages)
    }

    pub fn get_messages_by_exchange(&self, exchange: &str, limit: u64) -> Result<Vec<Message>> {
        let mut stmt = self.conn.prepare(
            "SELECT url, exchange, routing_key, message, timestamp 
             FROM messages 
             WHERE exchange = ?
             ORDER BY created_at DESC 
             LIMIT ?"
        )?;
        
        let messages = stmt.query_map(
            params![exchange, limit],
            |row| {
                Ok(Message {
                    url: row.get(0)?,
                    exchange: row.get(1)?,
                    routing_key: row.get(2)?,
                    message: row.get(3)?,
                    timestamp: row.get(4)?,
                })
            },
        )?
        .collect::<Result<Vec<Message>>>()?;

        Ok(messages)
    }
}
