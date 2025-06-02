# PLC Data Collector

A Python application for collecting data from OPC UA PLC devices and storing it in a TimescaleDB database.

## Features

- Connect to multiple OPC UA PLC sources
- Auto-discover available nodes on PLCs
- Real-time data collection via subscriptions (with fallback to polling)
- Configurable value filtering with deadband support
- Efficient batch storage in TimescaleDB
- Automatic data retention policies
- Robust error handling and reconnection

## Requirements

- Python 3.10+
- PostgreSQL with TimescaleDB extension

## Installation

### 1. Clone the repository

```bash
git clone https://github.com/jensjvh/thesis-metropolia.git
cd thesis-metropolia
```

### 2. Set up Python environment

Using Poetry (recommended):
```bash
pip install poetry

poetry install
```

Using pip:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

pip install -r requirements.txt
```

### 3. Set up the database

Install PostgreSQL and TimescaleDB:
```bash
sudo apt install postgresql
# Follow TimescaleDB installation from https://docs.timescale.com/self-hosted/latest/install/

# Create database
sudo -u postgres createuser -P username
sudo -u postgres createdb -O username username

# Enable TimescaleDB
sudo -u postgres psql -d username -c "CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;"
```

### 4. Configure the application

Edit the `config.yaml` file to configure your PLC connections and database settings:

```yaml
plcs:
  - name: plc1
    url: "opc.tcp://localhost:4840/freeopcua/server/"
    auto_discover: true
    # Add other PLC settings as needed

database:
  dbname: username
  user: username
  password: yourpassword
  host: localhost
  port: 5432
```

## Usage

### 1. Start the OPC UA server for testing (optional)

For testing, you can start the built-in OPC UA server:
```bash
cd src
python server.py
```

### 2. Start the PLC collector

```bash
cd src
python run.py
```

The collector will connect to all configured PLCs, discover nodes if enabled, and start collecting data.

### 3. View logs

Check the `plc_collector.log` file for detailed logs.

## Configuration Options

### PLC Configuration

| Option | Description | Default |
|--------|-------------|---------|
| `name` | Unique name for the PLC | Required |
| `url` | OPC UA endpoint URL | Required |
| `auto_discover` | Automatically discover nodes | False |
| `discover_depth` | Max depth for auto discovery | 3 |
| `discover_throttle_ms` | Delay between discovery requests (ms) | 100 |
| `discover_paths` | Starting paths for discovery | ["/Objects"] |
| `nodes` | Manual node mapping {node_id: name} | {} |
| `value_deadband_percent` | % change needed to record value | 1.0 |
| `value_deadband_absolute` | Absolute change needed to record | 0.0 |
| `buffer_size` | Max records before writing to DB | 100 |
| `buffer_flush_interval` | Max seconds before writing to DB | 5 |
| `username` | Authentication username | None |
| `password` | Authentication password | None |

### Database Configuration

| Option | Description | Default |
|--------|-------------|---------|
| `dbname` | Database name | Required |
| `user` | Database user | Required |
| `password` | Database password | Required |
| `host` | Database host | Required |
| `port` | Database port | Required |
| `min_connections` | Min pool connections | 1 |
| `max_connections` | Max pool connections | 5 |

## Troubleshooting

### Database Connection Issues

If you see errors like: `Database error: current transaction is aborted, commands ignored until end of transaction block`, make sure the db module checks for TimescaleDB hypertable correctly.