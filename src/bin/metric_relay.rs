use env_logger;

use metric_relay::runtime;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
	env_logger::init();
	let config_s = std::fs::read_to_string("config.toml")?;
	let config: runtime::Config = toml::from_str(&config_s)?;
	let _runtime = config.build()?;
	loop {
		tokio::time::sleep(core::time::Duration::new(20, 0)).await;
	}
}
