import json
import os
import sqlite3
import time
from datetime import datetime
from typing import Any, Optional

from caribou.common import constants
from caribou.common.models.remote_client.remote_client import RemoteClient
from caribou.deployment.common.deploy.models.resource import Resource


class IntegrationTestRemoteClient(RemoteClient):  # pylint: disable=too-many-public-methods
    def __init__(self) -> None:
        self._db_path = os.environ.get(
            "MULTI_X_SERVERLESS_INTEGRATION_TEST_DB_PATH", os.path.join(os.getcwd(), "db.sqlite")
        )
        self._initialize_db()

    def _db_connection(self) -> sqlite3.Connection:
        return sqlite3.connect(self._db_path)

    def _initialize_db(self) -> None:
        conn = self._db_connection()
        cursor = conn.cursor()

        for table in dir(constants):
            if getattr(constants, table) == constants.AVAILABLE_REGIONS_TABLE:
                cursor.execute(
                    f"""
                        CREATE TABLE IF NOT EXISTS {getattr(constants, table)} (
                            key TEXT PRIMARY KEY, 
                            value TEXT, 
                            provider_collector INTEGER, 
                            carbon_collector INTEGER, 
                            performance_collector INTEGER
                        )
                    """
                )
            elif table.endswith("_TABLE"):
                cursor.execute(
                    f"CREATE TABLE IF NOT EXISTS {getattr(constants, table)} (key TEXT PRIMARY KEY, value TEXT)"
                )
        cursor.execute("""CREATE TABLE IF NOT EXISTS resources (key TEXT PRIMARY KEY, value BLOB)""")
        cursor.execute(
            """
                CREATE TABLE IF NOT EXISTS messages (
                    identifier TEXT, 
                    timestamp INTEGER, 
                    message TEXT, 
                    PRIMARY KEY (identifier, timestamp)
                )
            """
        )
        cursor.execute("""CREATE TABLE IF NOT EXISTS roles (name TEXT PRIMARY KEY, policy TEXT, trust_policy TEXT)""")
        cursor.execute(
            """
                CREATE TABLE IF NOT EXISTS functions (
                    name TEXT PRIMARY KEY,
                    function_identifier TEXT,
                    role_identifier TEXT,
                    runtime TEXT,
                    handler TEXT,
                    environment_variables TEXT, 
                    timeout INTEGER, 
                    memory_size INTEGER
                )
            """
        )
        cursor.execute(
            """
                CREATE TABLE IF NOT EXISTS sync_node_data (
                    sync_node_name TEXT,
                    workflow_instance_id TEXT,
                    message TEXT,
                    PRIMARY KEY (sync_node_name, message)
                )
            """
        )
        cursor.execute(
            """
                CREATE TABLE IF NOT EXISTS predecessor_reached (
                    predecessor_name TEXT,
                    sync_node_name TEXT,
                    workflow_instance_id TEXT,
                    direct_call INTEGER,
                    PRIMARY KEY (predecessor_name, sync_node_name, workflow_instance_id)
                )
            """
        )
        cursor.execute(
            """
                CREATE TABLE IF NOT EXISTS messaging_topics (
                    topic_identifier TEXT PRIMARY KEY
                )
            """
        )
        cursor.execute(
            """
                CREATE TABLE IF NOT EXISTS messaging_subscriptions (
                    subscription_identifier TEXT,
                    topic_identifier TEXT,
                    function_identifier TEXT,
                    PRIMARY KEY (subscription_identifier, topic_identifier, function_identifier)
                )
            """
        )
        conn.commit()
        conn.close()

    def add_function_permission(self, function_identifier: str, topic_identifier: str) -> None:
        pass

    def create_messaging_topic(self, topic_name: str) -> str:
        conn = self._db_connection()
        cursor = conn.cursor()
        cursor.execute("INSERT INTO messaging_topics (topic_identifier) VALUES (?)", (topic_name,))
        conn.commit()
        conn.close()
        return topic_name

    def subscribe_messaging_topic(self, topic_identifier: str, function_identifier: str) -> str:
        conn = self._db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
                INSERT INTO messaging_subscriptions (
                    subscription_identifier,
                    topic_identifier,
                    function_identifier
                )
                VALUES (?, ?, ?)
            """,
            (f"{topic_identifier}_{function_identifier}", topic_identifier, function_identifier),
        )
        conn.commit()
        conn.close()
        return f"{topic_identifier}_{function_identifier}"

    def resource_exists(self, resource: Resource) -> bool:
        conn = self._db_connection()
        cursor = conn.cursor()
        if resource.resource_type == "iam_role":
            cursor.execute("SELECT * FROM roles WHERE name=?", (resource.name,))
        elif resource.resource_type == "function":
            cursor.execute("SELECT * FROM functions WHERE name=?", (resource.name,))
        else:
            raise ValueError(f"Resource type {resource.resource_type} not supported")
        result = cursor.fetchone()
        conn.close()
        return result is not None

    def create_role(self, role_name: str, policy: str, trust_policy: dict) -> str:
        trust_policy_str = json.dumps(trust_policy)
        conn = self._db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO roles (name, policy, trust_policy) VALUES (?, ?, ?)", (role_name, policy, trust_policy_str)
        )
        conn.commit()
        conn.close()
        return role_name

    def update_role(self, role_name: str, policy: str, trust_policy: dict) -> str:
        trust_policy_str = json.dumps(trust_policy)
        conn = self._db_connection()
        cursor = conn.cursor()
        cursor.execute("UPDATE roles SET policy=?, trust_policy=? WHERE name=?", (policy, trust_policy_str, role_name))
        conn.commit()
        conn.close()
        return role_name

    def send_message_to_messaging_service(self, identifier: str, message: str) -> None:
        current_timestamp = int(time.time())
        conn = self._db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO messages (identifier, timestamp, message) VALUES (?, ?, ?)",
            (identifier, current_timestamp, message),
        )
        conn.commit()
        conn.close()

    def get_predecessor_data(
        self, current_instance_name: str, workflow_instance_id: str, consistent_read: bool = True
    ) -> tuple[list[str], float]:
        conn = self._db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT message FROM sync_node_data WHERE sync_node_name=? AND workflow_instance_id=?",
            (current_instance_name, workflow_instance_id),
        )
        result = cursor.fetchall()
        conn.close()
        return [data[0] for data in result], 0.0

    def upload_predecessor_data_at_sync_node(
        self, function_name: str, workflow_instance_id: str, message: str
    ) -> float:
        conn = self._db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO sync_node_data (sync_node_name, workflow_instance_id, message) VALUES (?, ?, ?)",
            (function_name, workflow_instance_id, message),
        )
        conn.commit()
        conn.close()

        return 0.0

    def set_value_in_table(self, table_name: str, key: str, value: str) -> None:
        conn = self._db_connection()
        cursor = conn.cursor()
        cursor.execute(f"INSERT INTO {table_name} (key, value) VALUES (?, ?)", (key, value))
        conn.commit()
        conn.close()

    def get_value_from_table(self, table_name: str, key: str, consistent_read: bool = True) -> tuple[str, float]:
        conn = self._db_connection()
        cursor = conn.cursor()
        cursor.execute(f"SELECT value FROM {table_name} WHERE key=?", (key,))
        result = cursor.fetchone()
        conn.close()
        return (result[0], 0.0) if result else ("", 0.0)

    def upload_resource(self, key: str, resource: bytes) -> None:
        conn = self._db_connection()
        cursor = conn.cursor()
        cursor.execute("INSERT INTO resources (key, value) VALUES (?, ?)", (key, resource))
        conn.commit()
        conn.close()

    def download_resource(self, key: str) -> bytes:
        conn = self._db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT value FROM resources WHERE key=?", (key,))
        result = cursor.fetchone()
        conn.close()
        return result[0] if result else b""

    # pylint: disable=unused-argument
    def get_key_present_in_table(self, table_name: str, key: str, consistent_read: bool = True) -> bool:
        conn = self._db_connection()
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {table_name} WHERE key=?", (key,))
        result = cursor.fetchone()
        conn.close()
        return result is not None

    def set_predecessor_reached(
        self, predecessor_name: str, sync_node_name: str, workflow_instance_id: str, direct_call: bool
    ) -> tuple[list[bool], float]:
        conn = self._db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO predecessor_reached (predecessor_name, sync_node_name, workflow_instance_id, direct_call) VALUES (?, ?, ?, ?)",  # pylint: disable=line-too-long
            (predecessor_name, sync_node_name, workflow_instance_id, int(direct_call)),
        )
        cursor.execute(
            "SELECT direct_call FROM predecessor_reached WHERE sync_node_name=? AND workflow_instance_id=?",
            (sync_node_name, workflow_instance_id),
        )
        result = cursor.fetchone()
        conn.commit()
        conn.close()
        return [bool(res) for res in result], 0.0

    def get_all_values_from_table(self, table_name: str) -> dict:
        conn = self._db_connection()
        cursor = conn.cursor()
        cursor.execute(f"SELECT key, value FROM {table_name}")
        result = cursor.fetchall()
        conn.close()
        return {data[0]: data[1] for data in result}

    def set_value_in_table_column(
        self, table_name: str, key: str, column_type_value: list[tuple[str, str, str]]
    ) -> None:
        conn = self._db_connection()
        cursor = conn.cursor()

        cursor.execute(f"SELECT * FROM {table_name} WHERE key=?", (key,))
        result = cursor.fetchone()

        if result:
            update_query = f"UPDATE {table_name} SET"
            for column, _, _ in column_type_value:
                update_query += f" {column} = ?,"
            update_query = update_query[:-1] + " WHERE key = ?"
            cursor.execute(update_query, [value for _, _, value in column_type_value] + [key])
        else:
            insert_query = f"INSERT INTO {table_name} (key"
            for column, _, _ in column_type_value:
                insert_query += f", {column}"
            insert_query += ") VALUES (?"
            for _, _, _ in column_type_value:
                insert_query += ", ?"
            insert_query += ")"
            cursor.execute(insert_query, [key] + [value for _, _, value in column_type_value])
        conn.commit()
        conn.close()

    def get_keys(self, table_name: str) -> list[str]:
        conn = self._db_connection()
        cursor = conn.cursor()
        cursor.execute(f"SELECT key FROM {table_name}")
        result = cursor.fetchall()
        conn.close()
        return [data[0] for data in result]

    def remove_value_from_table(self, table_name: str, key: str) -> None:
        conn = self._db_connection()
        cursor = conn.cursor()
        cursor.execute(f"DELETE FROM {table_name} WHERE key=?", (key,))
        conn.commit()
        conn.close()

    def select_all_from_table(self, table_name: str) -> list[list[Any]]:
        conn = self._db_connection()
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {table_name}")
        result = cursor.fetchall()
        conn.close()
        return result

    def create_function(
        self,
        function_name: str,
        role_identifier: str,
        zip_contents: bytes,
        runtime: str,
        handler: str,
        environment_variables: dict[str, str],
        timeout: int,
        memory_size: int,
        additional_docker_commands: Optional[list[str]] = None,
    ) -> str:
        conn = self._db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
                INSERT INTO functions (
                    name,
                    function_identifier,
                    role_identifier,
                    runtime,
                    handler,
                    environment_variables,
                    timeout,
                    memory_size
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                function_name,
                f"{function_name}_identifier",
                role_identifier,
                runtime,
                handler,
                str(environment_variables),
                timeout,
                memory_size,
            ),
        )
        conn.commit()
        conn.close()
        return function_name

    def update_function(
        self,
        function_name: str,
        role_identifier: str,
        zip_contents: bytes,
        runtime: str,
        handler: str,
        environment_variables: dict[str, str],
        timeout: int,
        memory_size: int,
        additional_docker_commands: Optional[list[str]] = None,
    ) -> str:
        conn = self._db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
                UPDATE functions
                SET 
                    function_identifier=?, 
                    role_identifier=?, 
                    runtime=?, 
                    handler=?, 
                    environment_variables=?, 
                    timeout=?, 
                    memory_size=?
                WHERE name=?
            """,
            (
                f"{function_name}_identifier",
                role_identifier,
                runtime,
                handler,
                str(environment_variables),
                timeout,
                memory_size,
                function_name,
            ),
        )
        conn.commit()
        conn.close()
        return function_name

    def create_sync_tables(self) -> None:
        pass

    def get_logs_since(self, function_instance: str, since: datetime) -> list[str]:
        return []

    def remove_key(self, table_name: str, key: str) -> None:
        conn = self._db_connection()
        cursor = conn.cursor()
        cursor.execute(f"DELETE FROM {table_name} WHERE key=?", (key,))
        conn.commit()
        conn.close()

    def remove_function(self, function_name: str) -> None:
        conn = self._db_connection()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM functions WHERE name=?", (function_name,))
        conn.commit()
        conn.close()

    def remove_role(self, role_name: str) -> None:
        conn = self._db_connection()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM roles WHERE name=?", (role_name,))
        conn.commit()
        conn.close()

    def remove_messaging_topic(self, topic_identifier: str) -> None:
        conn = self._db_connection()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM messaging_topics WHERE topic_identifier=?", (topic_identifier,))
        conn.commit()
        conn.close()

    def get_topic_identifier(self, topic_name: str) -> str:
        conn = self._db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT topic_identifier FROM messaging_topics WHERE topic_identifier=?", (topic_name,))
        result = cursor.fetchone()
        conn.close()
        return result[0] if result else ""

    def remove_resource(self, key: str) -> None:
        conn = self._db_connection()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM resources WHERE key=?", (key,))
        conn.commit()
        conn.close()

    def update_value_in_table(self, table_name: str, key: str, value: str) -> None:
        conn = self._db_connection()
        cursor = conn.cursor()
        cursor.execute(f"UPDATE {table_name} SET value=? WHERE key=?", (value, key))
        conn.commit()
        conn.close()

    def get_current_provider_region(self) -> str:
        return "test_provider-rivendell"

    def get_logs_between(self, function_instance: str, start: datetime, end: datetime) -> list[str]:
        return []

    # pylint: disable=unused-argument
    def get_insights_logs_between(self, function_instance: str, start: datetime, end: datetime) -> list[str]:
        return []
