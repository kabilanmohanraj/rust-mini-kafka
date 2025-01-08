use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex, RwLock};

use crate::broker::utils::process_request;

pub struct Broker {
    // network management
    listening_socket: TcpListener,
    connection_pool: RwLock<Vec<TcpStream>>,
    max_concurrent_connections: u8,
    current_connections: Mutex<u8>,

    // cluster metadata management
    
}

impl Broker {
    // create a new broker
    pub fn new(address: &str, max_concurrent_connections: u8) -> std::io::Result<Self> {
        let listener = TcpListener::bind(address)?;
        Ok(Broker {
            listening_socket: listener,
            connection_pool: RwLock::new(Vec::new()),
            max_concurrent_connections,
            current_connections: Mutex::new(0),
        })
    }

    // accepting new connections
    pub fn accept_new_connections(self: Arc<Self>) -> std::io::Result<()> {
        for stream in self.listening_socket.incoming() {
            let stream = stream?;
            {
                let mut pool = self.connection_pool.write().unwrap();
                let mut current = self.current_connections.lock().unwrap();

                if *current < self.max_concurrent_connections {
                    pool.push(stream);
                    *current += 1;

                    println!("Spawing thread to handle connection...");
                    let broker_clone = Arc::clone(&self);
                    std::thread::spawn(move || {
                        if let Some(connection) = broker_clone.borrow_connection() {
                            // Use the connection
                            println!("Processing request...");
                            process_request(connection, broker_clone);
                        } else {
                            println!("No available connections.");
                        }
                    });
                } else {
                    eprintln!("Connection pool full, rejecting connection.");
                }
            }
        }
        Ok(())
    }

    // borrow a connection from the pool
    pub fn borrow_connection(&self) -> Option<TcpStream> {
        let mut pool = self.connection_pool.write().unwrap();
        pool.pop()
    }

    // return a connection to the pool
    pub fn return_connection(&self, stream: TcpStream) {
        let mut pool = self.connection_pool.write().unwrap();
        pool.push(stream);
    }

}
