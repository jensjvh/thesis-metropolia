import psycopg2
import psycopg2.extras
import psycopg2.pool
import logging
import time
from contextlib import contextmanager
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from config import DBConfig

logger = logging.getLogger(__name__)

class Database:
    def __init__(self, config: DBConfig):
        self.config = config
        self.connection_params = {
            "dbname": config.dbname,
            "user": config.user,
            "password": config.password,
            "host": config.host,
            "port": config.port
        }
        
        min_conn = getattr(config, 'min_connections', 1)
        max_conn = getattr(config, 'max_connections', 5)
        
        self.pool = psycopg2.pool.ThreadedConnectionPool(
            min_conn, 
            max_conn,
            **self.connection_params
        )
        
        self._last_retention_check = datetime.now()
        self._retention_check_interval = timedelta(hours=24)
    
    def setup(self):
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (self.config.dbname,))
                if not cursor.fetchone():
                    logger.error(f"Database {self.config.dbname} does not exist")
                    raise Exception(f"Database {self.config.dbname} does not exist")
                
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS plc_data (
                        time TIMESTAMPTZ NOT NULL DEFAULT now(),
                        plc_name TEXT NOT NULL,
                        node_id TEXT NOT NULL,
                        node_name TEXT NOT NULL,
                        data_type TEXT NOT NULL,
                        value_number DOUBLE PRECISION NULL,
                        value_boolean BOOLEAN NULL,
                        value_string TEXT NULL,
                        value_timestamp TIMESTAMPTZ NULL,
                        value_json JSONB NULL
                    );
                """)
                
                cursor.execute("SELECT 1 FROM pg_extension WHERE extname = 'timescaledb'")
                has_timescaledb = cursor.fetchone() is not None
                
                if has_timescaledb:
                    try:
                        cursor.execute("""
                            SELECT create_hypertable('plc_data', 'time', 
                                if_not_exists => TRUE,
                                chunk_time_interval => INTERVAL '1 day'
                            );
                        """)
                        logger.info("TimescaleDB hypertable created")
                    except Exception as e:
                        logger.warning(f"Could not create hypertable: {e}")
                else:
                    logger.warning("TimescaleDB extension not available, using regular table")
                
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_plc_data_plc_name ON plc_data(plc_name);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_plc_data_node_id ON plc_data(node_id);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_plc_data_type ON plc_data(data_type);")
                
                if has_timescaledb:
                    try:
                        cursor.execute("""
                            SELECT 1 FROM timescaledb_information.hypertables 
                            WHERE hypertable_name = 'plc_data';
                        """)
                        is_hypertable = cursor.fetchone() is not None
                        
                        if not is_hypertable:
                            cursor.execute("""
                                SELECT create_hypertable('plc_data', 'time', 
                                    if_not_exists => TRUE,
                                    chunk_time_interval => INTERVAL '1 day'
                                );
                            """)
                            logger.info("TimescaleDB hypertable created")
                        else:
                            logger.info("plc_data is already a hypertable")
                    except Exception as e:
                        logger.warning(f"Could not create hypertable: {e}")
                        conn.rollback()
                        conn.begin()
                
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS plc_nodes (
                        plc_name TEXT NOT NULL,
                        node_id TEXT NOT NULL,
                        node_name TEXT NOT NULL,
                        data_type TEXT,
                        description TEXT,
                        units TEXT,
                        last_seen TIMESTAMPTZ NOT NULL DEFAULT now(),
                        PRIMARY KEY (plc_name, node_id)
                    );
                """)
                
                if has_timescaledb:
                    try:
                        cursor.execute("SELECT 1 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE c.relname = 'plc_data_hourly'")
                        if cursor.fetchone():
                            cursor.execute("""
                                SELECT add_continuous_aggregate_policy('plc_data_hourly',
                                    start_offset => INTERVAL '2 hours',
                                    end_offset => INTERVAL '30 minutes',
                                    schedule_interval => INTERVAL '1 hour');
                            """)
                            logger.info("TimescaleDB continuous aggregate policy created")
                    except Exception as e:
                        logger.debug(f"Could not create continuous aggregate policy: {e}")
                
                logger.info("Database schema setup complete")
                
                self._apply_retention_policy(cursor)
    
    @contextmanager
    def get_connection(self):
        conn = None
        try:
            conn = self.pool.getconn()
            yield conn
            conn.commit()
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Database error: {e}")
            raise
        finally:
            if conn:
                self.pool.putconn(conn)
    
    def store_data_batch(self, records: List[Dict[str, Any]], check_retention=True):
        if not records:
            return
            
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                values_list = []
                args_list = []
                
                for record in records:
                    plc_name = record["plc_name"]
                    node_id = record["node_id"]
                    node_name = record["node_name"]
                    value = record["value"]
                    
                    if value is None:
                        data_type = "NULL"
                        values_list.append("(%s, %s, %s, %s, NULL, NULL, NULL, NULL, NULL)")
                        args_list.extend([plc_name, node_id, node_name, data_type])
                    elif isinstance(value, (int, float)):
                        data_type = "NUMBER"
                        values_list.append("(%s, %s, %s, %s, %s, NULL, NULL, NULL, NULL)")
                        args_list.extend([plc_name, node_id, node_name, data_type, value])
                    elif isinstance(value, bool):
                        data_type = "BOOLEAN"
                        values_list.append("(%s, %s, %s, %s, NULL, %s, NULL, NULL, NULL)")
                        args_list.extend([plc_name, node_id, node_name, data_type, value])
                    elif isinstance(value, str):
                        data_type = "STRING"
                        values_list.append("(%s, %s, %s, %s, NULL, NULL, %s, NULL, NULL)")
                        args_list.extend([plc_name, node_id, node_name, data_type, value])
                    elif hasattr(value, 'isoformat'):
                        data_type = "TIMESTAMP" 
                        values_list.append("(%s, %s, %s, %s, NULL, NULL, NULL, %s, NULL)")
                        args_list.extend([plc_name, node_id, node_name, data_type, value])
                    else:
                        try:
                            data_type = "JSON"
                            values_list.append("(%s, %s, %s, %s, NULL, NULL, NULL, NULL, %s)")
                            args_list.extend([plc_name, node_id, node_name, data_type, psycopg2.extras.Json(value)])
                        except (TypeError, ValueError):
                            data_type = "STRING"
                            values_list.append("(%s, %s, %s, %s, NULL, NULL, %s, NULL, NULL)")
                            args_list.extend([plc_name, node_id, node_name, data_type, str(value)])
                    
                    cursor.execute("""
                        INSERT INTO plc_nodes (plc_name, node_id, node_name, data_type, last_seen)
                        VALUES (%s, %s, %s, %s, now())
                        ON CONFLICT (plc_name, node_id) DO UPDATE
                        SET node_name = EXCLUDED.node_name,
                            data_type = EXCLUDED.data_type,
                            last_seen = now();
                    """, (plc_name, node_id, node_name, data_type))
                
                query = f"""
                    INSERT INTO plc_data 
                        (plc_name, node_id, node_name, data_type, 
                         value_number, value_boolean, value_string, 
                         value_timestamp, value_json)
                    VALUES {", ".join(values_list)}
                """
                cursor.execute(query, args_list)
                
                logger.debug(f"Stored {len(records)} records")
                
                if check_retention:
                    now = datetime.now()
                    if now - self._last_retention_check > self._retention_check_interval:
                        self._apply_retention_policy(cursor)
                        self._last_retention_check = now
    
    def _apply_retention_policy(self, cursor):
        try:
            cursor.execute("""
                SELECT drop_chunks('plc_data', INTERVAL '90 days');
            """)
            logger.info("Applied retention policy: dropped chunks older than 90 days")
        except Exception as e:
            try:
                cursor.execute("""
                    DELETE FROM plc_data
                    WHERE time < now() - INTERVAL '90 days';
                """)
                logger.info("Applied retention policy: deleted data older than 90 days")
            except Exception as e2:
                logger.error(f"Failed to apply retention policy: {e2}")
    
    def close(self):
        if hasattr(self, 'pool'):
            self.pool.closeall()
            logger.info("Database connections closed")