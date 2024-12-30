use std::{
    collections::VecDeque,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::{Arc, Mutex},
    thread,
};

#[derive(Clone, Debug)]
struct Backend {
    address: String,
}

struct LoadBalancer {
    backends: Arc<Mutex<VecDeque<Backend>>>,
}

impl LoadBalancer {
    fn new(backend_addresses: Vec<String>) -> Self {
        let mut backend = VecDeque::new();

        for addr in backend_addresses {
            backend.push_back(Backend { address: addr });
        }

        LoadBalancer {
            backends: Arc::new(Mutex::new(backend)),
        }
    }

    /*
     * Generate next backend using round robin fashion
     */
    fn next_backend(&self) -> Option<Backend> {
        let mut backends = self.backends.lock().unwrap();
        if let Some(backend) = backends.pop_front() {
            backends.push_back(backend.clone());
            Some(backend)
        } else {
            None
        }
    }

    /*
     * Handles the incomming client
     */
    fn handle_client(&self, mut client_stream: TcpStream) -> std::io::Result<()> {
        let mut buffer = [0; 4096];
        let n = client_stream.read(&mut buffer)?;
        let request = String::from_utf8_lossy(&buffer[..n]);

        if let Some(backend) = self.next_backend() {
            println!("Forwarding request to backend: {}", backend.address);

            match TcpStream::connect(&backend.address) {
                Ok(mut backend_stream) => {
                    backend_stream.write_all(&buffer[..n])?;

                    let mut response = Vec::new();
                    backend_stream.read_to_end(&mut response)?;

                    client_stream.write_all(&response)?;
                }
                Err(e) => {
                    println!("Failed to connect to backend {}: {}", backend.address, e);
                    let error_response =
                        "HTTP/1.1 502 Bad Gateway\r\n\r\nBackend server unavailable";
                    client_stream.write_all(error_response.as_bytes())?;
                }
            }
        } else {
            let error_response =
                "HTTP/1.1 503 Service Unavailable\r\n\r\nNo backend servers available";
            client_stream.write_all(error_response.as_bytes())?;
        }

        Ok(())
    }

    fn start(&self, listen_addr: &str) -> std::io::Result<()> {
        let listener = TcpListener::bind(listen_addr)?;
        println!("Load balancer listening on {}", listen_addr);

        for stream in listener.incoming() {
            match stream {
                Ok(client_stream) => {
                    let balancer = self.clone();
                    thread::spawn(move || {
                        if let Err(e) = balancer.handle_client(client_stream) {
                            println!("Error handling client: {}", e)
                        }
                    });
                }
                Err(e) => {
                    println!("Error accepting connection {}", e);
                }
            }
        }
        Ok(())
    }
}

impl Clone for LoadBalancer {
    fn clone(&self) -> Self {
        LoadBalancer {
            backends: Arc::clone(&self.backends),
        }
    }
}

fn main() -> std::io::Result<()> {
    let backend_servers = vec![
        "127.0.0.1:8081".to_string(),
        "127.0.0.1:8082".to_string(),
        "127.0.0.1:8083".to_string(),
    ];

    let load_balancer = LoadBalancer::new(backend_servers);
    load_balancer.start("127.0.0.1:8080")
}
