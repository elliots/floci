//! Floci on the Mac, for the tests of the wakes that Floci sends: the JVM
//! build, run with ports, a data directory and a Docker namespace of its own,
//! the AWS CLI against it, and an MQTT client for its IoT broker.

use std::env;
use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::thread;
use std::time::{Duration, Instant};

use crate::cluster::free_port;
use crate::cmd;
use crate::http::http_get;
use crate::json::Json;
use crate::step;

/// The account and region of what the tests make in Floci.
pub const ACCOUNT: &str = "000000000000";
pub const REGION: &str = "us-east-1";

/// How long Floci may take to answer its health check once started, and to
/// exit after SIGTERM.
const START_TIMEOUT: Duration = Duration::from_secs(120);
const STOP_TIMEOUT: Duration = Duration::from_secs(60);
const AWS_TIMEOUT: Duration = Duration::from_secs(120);

/// Floci, run from its JVM build with persistent storage. Its data, its log
/// and the files that a test gives it are in [`Floci::dir`]. Dropping it
/// stops it, removes the Docker containers and volumes of its namespace and,
/// unless the test failed or keeps what it made, its directory.
pub struct Floci {
    dir: PathBuf,
    java: PathBuf,
    jar: PathBuf,
    env: Vec<(String, String)>,
    port: u16,
    mqtt_port: u16,
    namespace: String,
    child: Option<Child>,
    keep: bool,
}

impl Floci {
    /// Starts Floci as `name`, which names its directory and its Docker
    /// namespace, with `env` added to its environment, and waits until it
    /// answers. Every port it listens on is a free one.
    ///
    /// The build is `SMOLVM_PODS_E2E_FLOCI_JAR`, by default
    /// `target/quarkus-app/quarkus-run.jar` of the repository, which
    /// `./mvnw package -DskipTests` makes; it runs on `$JAVA_HOME/bin/java`,
    /// or `java`.
    pub fn start(name: &str, env: &[(&str, &str)]) -> Floci {
        let jar = env::var_os("SMOLVM_PODS_E2E_FLOCI_JAR")
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                let repo = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../..");
                repo.join("target/quarkus-app/quarkus-run.jar")
            });
        assert!(
            jar.is_file(),
            "{} is missing: build Floci with ./mvnw package -DskipTests, or set \
             SMOLVM_PODS_E2E_FLOCI_JAR",
            jar.display()
        );
        let java = env::var_os("JAVA_HOME")
            .map(|home| PathBuf::from(home).join("bin/java"))
            .unwrap_or_else(|| PathBuf::from("java"));
        let dir = env::temp_dir().join(format!("{name}-floci"));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(dir.join("data")).unwrap_or_else(|e| panic!("{}: {e}", dir.display()));
        let port = free_port(39000..=39499);
        let mqtt_port = free_port(39500..=39999);
        let data = dir.join("data").display().to_string();
        let mut floci_env: Vec<(String, String)> = [
            ("FLOCI_PORT", port.to_string()),
            ("FLOCI_BASE_URL", format!("http://localhost:{port}")),
            ("FLOCI_DEFAULT_ACCOUNT_ID", ACCOUNT.to_string()),
            ("FLOCI_DEFAULT_REGION", REGION.to_string()),
            ("FLOCI_STORAGE_MODE", "persistent".to_string()),
            ("FLOCI_STORAGE_PERSISTENT_PATH", data.clone()),
            ("FLOCI_STORAGE_HOST_PERSISTENT_PATH", data),
            ("FLOCI_STORAGE_PRUNE_VOLUMES_ON_DELETE", "true".to_string()),
            ("FLOCI_DOCKER_RESOURCE_NAMESPACE", name.to_string()),
            ("FLOCI_SERVICES_IOT_MQTT_PORT", mqtt_port.to_string()),
            (
                "FLOCI_SERVICES_IOT_MQTT_TLS_PORT",
                free_port(37500..=37999).to_string(),
            ),
            (
                "FLOCI_SERVICES_EC2_IMDS_PORT",
                free_port(37000..=37499).to_string(),
            ),
            (
                "FLOCI_SERVICES_EC2_RECONCILE_CONTAINERS_ON_STARTUP",
                "false".to_string(),
            ),
            (
                "QUARKUS_LOG_CATEGORY__IO_GITHUB_HECTORVENT_FLOCI_RUNTIME__LEVEL",
                "DEBUG".to_string(),
            ),
        ]
        .into_iter()
        .map(|(key, value)| (key.to_string(), value))
        .collect();
        for (range, base, max) in [
            (
                36000..=36499,
                "FLOCI_SERVICES_AMAZONMQ_AMQP_HOST_PORT_BASE",
                "FLOCI_SERVICES_AMAZONMQ_AMQP_HOST_PORT_MAX",
            ),
            (
                36500..=36999,
                "FLOCI_SERVICES_AMAZONMQ_CONSOLE_HOST_PORT_BASE",
                "FLOCI_SERVICES_AMAZONMQ_CONSOLE_HOST_PORT_MAX",
            ),
        ] {
            let port = free_port(range).to_string();
            floci_env.push((base.to_string(), port.clone()));
            floci_env.push((max.to_string(), port));
        }
        floci_env.extend(env.iter().map(|(k, v)| (k.to_string(), v.to_string())));
        let mut floci = Floci {
            dir,
            java,
            jar,
            env: floci_env,
            port,
            mqtt_port,
            namespace: name.to_string(),
            child: None,
            keep: env::var("SMOLVM_PODS_E2E_KEEP").as_deref() == Ok("1"),
        };
        floci.run();
        floci
    }

    /// The directory of Floci's data, its log and the test's files for it.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Floci's port on the Mac's loopback.
    pub fn port(&self) -> u16 {
        self.port
    }

    /// The port of Floci's MQTT broker, which a call to the IoT API starts.
    pub fn mqtt_port(&self) -> u16 {
        self.mqtt_port
    }

    /// The URL of Floci for clients on the Mac.
    pub fn url(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    /// The file that holds what Floci printed.
    pub fn log_path(&self) -> PathBuf {
        self.dir.join("floci.log")
    }

    /// What Floci printed so far.
    pub fn log(&self) -> String {
        fs::read_to_string(self.log_path()).unwrap_or_default()
    }

    /// Stops Floci with SIGTERM, as a user does, and starts it again with the
    /// same data and settings.
    pub fn restart(&mut self) -> Result<(), String> {
        self.stop()?;
        step("starting Floci again");
        self.run();
        Ok(())
    }

    /// Runs the AWS CLI against Floci with `args`, a service and an operation
    /// with its options, and returns what it printed as JSON (null for
    /// nothing).
    pub fn aws(&self, args: &[&str]) -> Result<Json, String> {
        let program = env::var("SMOLVM_PODS_E2E_AWS").unwrap_or_else(|_| "aws".to_string());
        let mut cmd = Command::new(program);
        cmd.args(["--endpoint-url", &self.url(), "--region", REGION])
            .args(["--output", "json"])
            .args(args)
            .env("AWS_ACCESS_KEY_ID", "test")
            .env("AWS_SECRET_ACCESS_KEY", "test")
            .env("AWS_CONFIG_FILE", self.dir.join("aws-config"))
            .env(
                "AWS_SHARED_CREDENTIALS_FILE",
                self.dir.join("aws-credentials"),
            )
            .env("AWS_PAGER", "")
            .env("AWS_CLI_AUTO_PROMPT", "off")
            .env("AWS_EC2_METADATA_DISABLED", "true");
        for name in [
            "AWS_PROFILE",
            "AWS_DEFAULT_PROFILE",
            "AWS_SESSION_TOKEN",
            "AWS_ENDPOINT_URL",
            "AWS_DEFAULT_REGION",
        ] {
            cmd.env_remove(name);
        }
        let stdout = cmd::run_ok(&mut cmd, AWS_TIMEOUT)?;
        if stdout.trim().is_empty() {
            return Ok(Json::Null);
        }
        Json::parse(&stdout).map_err(|e| format!("aws {}: {e}: {stdout}", args.join(" ")))
    }

    fn run(&mut self) {
        let log = File::options()
            .create(true)
            .append(true)
            .open(self.log_path())
            .unwrap_or_else(|e| panic!("{}: {e}", self.log_path().display()));
        let err = log.try_clone().expect("clone the log file");
        let child = Command::new(&self.java)
            .arg("-jar")
            .arg(&self.jar)
            .envs(self.env.iter().map(|(k, v)| (k, v)))
            .current_dir(&self.dir)
            .stdout(log)
            .stderr(err)
            .spawn()
            .unwrap_or_else(|e| {
                panic!(
                    "start {} -jar {}: {e}",
                    self.java.display(),
                    self.jar.display()
                )
            });
        step(&format!(
            "Floci runs as pid {} on port {}; its log is {}",
            child.id(),
            self.port,
            self.log_path().display()
        ));
        self.child = Some(child);
        let addr = SocketAddr::from(([127, 0, 0, 1], self.port));
        let deadline = Instant::now() + START_TIMEOUT;
        loop {
            if let Some(status) = self
                .child
                .as_mut()
                .and_then(|c| c.try_wait().ok().flatten())
            {
                panic!("Floci exited with {status}:\n{}", tail(&self.log(), 40));
            }
            match http_get(addr, "/_floci/health", Duration::from_secs(2)) {
                Ok((200, _)) => return,
                Ok(_) | Err(_) if Instant::now() < deadline => {
                    thread::sleep(Duration::from_millis(250))
                }
                answer => panic!(
                    "Floci did not answer its health check within {START_TIMEOUT:?}: \
                     {answer:?}\n{}",
                    tail(&self.log(), 40)
                ),
            }
        }
    }

    /// Sends SIGTERM and waits for Floci to exit, then SIGKILL if it does not.
    fn stop(&mut self) -> Result<(), String> {
        let Some(mut child) = self.child.take() else {
            return Ok(());
        };
        step(&format!("stopping Floci, pid {}", child.id()));
        cmd::run(
            Command::new("kill").args(["-TERM", &child.id().to_string()]),
            Duration::from_secs(10),
        )?;
        let deadline = Instant::now() + STOP_TIMEOUT;
        loop {
            match child.try_wait() {
                Ok(Some(_)) => return Ok(()),
                Ok(None) if Instant::now() < deadline => thread::sleep(Duration::from_millis(100)),
                _ => {
                    let _ = child.kill();
                    let _ = child.wait();
                    return Err(format!(
                        "Floci did not stop within {STOP_TIMEOUT:?} of SIGTERM"
                    ));
                }
            }
        }
    }

    /// Removes the Docker containers and volumes of Floci's namespace.
    fn remove_docker_resources(&self) -> Vec<String> {
        let label = format!("label=floci_namespace={}", self.namespace);
        let mut errors = Vec::new();
        let listings: [(&[&str], &[&str]); 2] = [
            (&["ps", "-a", "-q", "--filter"], &["rm", "-f", "-v"]),
            (&["volume", "ls", "-q", "--filter"], &["volume", "rm", "-f"]),
        ];
        for (list, remove) in listings {
            let found = cmd::run_ok(
                Command::new("docker").args(list).arg(&label),
                Duration::from_secs(60),
            );
            match found {
                Ok(ids) if ids.trim().is_empty() => {}
                Ok(ids) => {
                    let removed = cmd::run_ok(
                        Command::new("docker")
                            .args(remove)
                            .args(ids.split_whitespace()),
                        Duration::from_secs(120),
                    );
                    errors.extend(removed.err());
                }
                Err(e) => errors.push(e),
            }
        }
        errors
    }
}

impl Drop for Floci {
    fn drop(&mut self) {
        let mut errors: Vec<String> = self.stop().err().into_iter().collect();
        errors.extend(self.remove_docker_resources());
        if self.keep || thread::panicking() {
            step(&format!("kept Floci's directory {}", self.dir.display()));
        } else if let Err(e) = fs::remove_dir_all(&self.dir) {
            errors.push(format!("remove {}: {e}", self.dir.display()));
        }
        if errors.is_empty() {
            return;
        }
        let message = format!("Floci teardown failed:\n{}", errors.join("\n"));
        if thread::panicking() {
            eprintln!("{message}");
        } else {
            panic!("{message}");
        }
    }
}

/// The last `lines` lines of `text`.
pub fn tail(text: &str, lines: usize) -> String {
    let all: Vec<&str> = text.lines().collect();
    all[all.len().saturating_sub(lines)..].join("\n")
}

/// Publishes `payload` to `topic` at QoS 1 through the MQTT 3.1.1 broker on
/// the Mac's `port`, as the client `client_id`, and waits for the broker's
/// acknowledgement.
pub fn mqtt_publish(port: u16, client_id: &str, topic: &str, payload: &[u8]) -> Result<(), String> {
    let failed = |e: io::Error| format!("MQTT publish to port {port}: {e}");
    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_secs(5)).map_err(failed)?;
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .map_err(failed)?;
    stream.write_all(&mqtt_connect(client_id)).map_err(failed)?;
    let connack = read_packet(&mut stream).map_err(failed)?;
    if connack != [0x20, 0x02, 0x00, 0x00] {
        return Err(format!(
            "MQTT broker on port {port} refused the connection: {connack:02x?}"
        ));
    }
    stream
        .write_all(&mqtt_publish_packet(topic, 1, payload))
        .map_err(failed)?;
    let puback = read_packet(&mut stream).map_err(failed)?;
    if puback != [0x40, 0x02, 0x00, 0x01] {
        return Err(format!(
            "MQTT broker on port {port} did not acknowledge the publish: {puback:02x?}"
        ));
    }
    stream.write_all(&[0xe0, 0x00]).map_err(failed)?;
    Ok(())
}

/// An MQTT 3.1.1 CONNECT with a clean session and a 60 s keepalive.
pub fn mqtt_connect(client_id: &str) -> Vec<u8> {
    let mut body = Vec::new();
    push_string(&mut body, "MQTT");
    body.extend_from_slice(&[0x04, 0x02, 0x00, 60]);
    push_string(&mut body, client_id);
    packet(0x10, &body)
}

/// An MQTT 3.1.1 PUBLISH at QoS 1 with the packet id `id`.
pub fn mqtt_publish_packet(topic: &str, id: u16, payload: &[u8]) -> Vec<u8> {
    let mut body = Vec::new();
    push_string(&mut body, topic);
    body.extend_from_slice(&id.to_be_bytes());
    body.extend_from_slice(payload);
    packet(0x32, &body)
}

fn push_string(out: &mut Vec<u8>, text: &str) {
    let len = u16::try_from(text.len()).expect("an MQTT string of at most 65535 bytes");
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(text.as_bytes());
}

/// A packet with its fixed header: the type and flags, and the remaining
/// length as a variable-length integer.
fn packet(header: u8, body: &[u8]) -> Vec<u8> {
    let mut out = vec![header];
    let mut len = body.len();
    loop {
        let mut byte = (len % 128) as u8;
        len /= 128;
        if len > 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if len == 0 {
            break;
        }
    }
    out.extend_from_slice(body);
    out
}

/// Reads one packet with a remaining length below 128, as CONNACK and PUBACK
/// are.
fn read_packet(stream: &mut TcpStream) -> io::Result<Vec<u8>> {
    let mut head = [0u8; 2];
    stream.read_exact(&mut head)?;
    let mut rest = vec![0u8; usize::from(head[1] & 0x7f)];
    stream.read_exact(&mut rest)?;
    Ok([&head[..], &rest[..]].concat())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;

    #[test]
    fn encodes_mqtt_packets() {
        assert_eq!(
            mqtt_connect("e2e"),
            [0x10, 15, 0, 4, b'M', b'Q', b'T', b'T', 4, 2, 0, 60, 0, 3, b'e', b'2', b'e']
        );
        assert_eq!(
            mqtt_publish_packet("a/b", 1, b"hi"),
            [0x32, 9, 0, 3, b'a', b'/', b'b', 0, 1, b'h', b'i']
        );
        let long = packet(0x32, &[0u8; 200]);
        assert_eq!(&long[..3], &[0x32, 0xc8, 0x01]);
        assert_eq!(long.len(), 203);
    }

    #[test]
    fn publishes_through_a_broker() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let port = listener.local_addr().unwrap().port();
        let broker = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut connect = [0u8; 17];
            stream.read_exact(&mut connect).unwrap();
            stream.write_all(&[0x20, 0x02, 0x00, 0x00]).unwrap();
            let mut publish = [0u8; 11];
            stream.read_exact(&mut publish).unwrap();
            stream.write_all(&[0x40, 0x02, 0x00, 0x01]).unwrap();
            let mut disconnect = [0u8; 2];
            stream.read_exact(&mut disconnect).unwrap();
            (connect.to_vec(), publish.to_vec(), disconnect)
        });
        mqtt_publish(port, "e2e", "a/b", b"hi").unwrap();
        let (connect, publish, disconnect) = broker.join().unwrap();
        assert_eq!(connect, mqtt_connect("e2e"));
        assert_eq!(publish, mqtt_publish_packet("a/b", 1, b"hi"));
        assert_eq!(disconnect, [0xe0, 0x00]);
    }

    #[test]
    fn keeps_the_last_lines() {
        assert_eq!(tail("a\nb\nc\n", 2), "b\nc");
        assert_eq!(tail("a", 5), "a");
    }
}
