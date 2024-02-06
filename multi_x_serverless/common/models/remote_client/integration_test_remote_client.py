import os
import sqlite3
import time
from typing import Any

from multi_x_serverless.common import constants
from multi_x_serverless.common.models.remote_client.remote_client import RemoteClient
from multi_x_serverless.deployment.common.deploy.models.resource import Resource


class IntegrationTestRemoteClient(RemoteClient):
    def __init__(self) -> None:
        self._db_path = os.environ.get("DB_PATH", os.path.join(os.getcwd(), "db.sqlite3"))
        self._initialize_db()

    def _db_connection(self) -> sqlite3.Connection:
        return sqlite3.connect(self._db_path)

    def _initialize_db(self) -> None:
        conn = self._db_connection()
        cursor = conn.cursor()

        for table in dir(constants):
            if table == constants.AVAILABLE_REGIONS_TABLE:
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
            elif table == constants.WORKFLOW_SUMMARY_TABLE:
                cursor.execute(
                    f"""
                        CREATE TABLE IF NOT EXISTS {getattr(constants, table)} 
                            key TEXT PRIMARY KEY,
                            value TEXT,
                            sort_key INTEGER
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
        cursor.execute("""CREATE TABLE IF NOT EXISTS functions (name TEXT PRIMARY KEY, role_arn TEXT)""")
        cursor.execute(
            """
                CREATE TABLE IF NOT EXISTS sync_node_data (
                    sync_node_name TEXT,
                    workflow_instance_id TEXT,
                    message TEXT,
                    PRIMARY KEY (sync_node_id, message)
                )
            """
        )
        cursor.execute(
            """
                CREATE TABLE IF NOT EXISTS predecessor_reached (
                    predecessor_name TEXT,
                    sync_node_name TEXT,
                    workflow_instance_id TEXT,
                    PRIMARY KEY (predecessor_name, sync_node_name, workflow_instance_id)
                )
            """
        )
        conn.commit()
        conn.close()

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
        conn = self._db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO roles (name, policy, trust_policy) VALUES (?, ?, ?)", (role_name, policy, trust_policy)
        )
        conn.commit()
        conn.close()
        return role_name

    def update_role(self, role_name: str, policy: str, trust_policy: dict) -> str:
        conn = self._db_connection()
        cursor = conn.cursor()
        cursor.execute("UPDATE roles SET policy=?, trust_policy=? WHERE name=?", (policy, trust_policy, role_name))
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

    def get_predecessor_data(self, current_instance_name: str, workflow_instance_id: str) -> list[str]:
        conn = self._db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT message FROM sync_node_data WHERE sync_node_name=? AND workflow_instance_id=?",
            (current_instance_name, workflow_instance_id),
        )
        result = cursor.fetchall()
        conn.close()
        return [data[0] for data in result]

    def upload_predecessor_data_at_sync_node(self, function_name: str, workflow_instance_id: str, message: str) -> None:
        conn = self._db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO sync_node_data (sync_node_name, workflow_instance_id, message) VALUES (?, ?, ?)",
            (function_name, workflow_instance_id, message),
        )
        conn.commit()
        conn.close()

    def set_value_in_table(self, table_name: str, key: str, value: str) -> None:
        conn = self._db_connection()
        cursor = conn.cursor()
        cursor.execute(f"INSERT INTO {table_name} (key, value) VALUES (?, ?)", (key, value))
        conn.commit()
        conn.close()

    def get_value_from_table(self, table_name: str, key: str) -> str:
        conn = self._db_connection()
        cursor = conn.cursor()
        cursor.execute(f"SELECT value FROM {table_name} WHERE key=?", (key,))
        result = cursor.fetchone()
        conn.close()
        return result[0] if result else ""

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

    def get_key_present_in_table(self, table_name: str, key: str) -> bool:
        conn = self._db_connection()
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {table_name} WHERE key=?", (key,))
        result = cursor.fetchone()
        conn.close()
        return result is not None

    def set_predecessor_reached(self, predecessor_name: str, sync_node_name: str, workflow_instance_id: str) -> int:
        conn = self._db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO predecessor_reached (predecessor_name, sync_node_name, workflow_instance_id) VALUES (?, ?, ?)",
            (predecessor_name, sync_node_name, workflow_instance_id),
        )
        cursor.execute(
            "SELECT COUNT(*) FROM predecessor_reached WHERE sync_node_name=? AND workflow_instance_id=?",
            (sync_node_name, workflow_instance_id),
        )
        result = cursor.fetchone()
        conn.commit()
        conn.close()
        return result[0]

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
        insert_query = f"INSERT INTO {table_name} (key"
        for column, _, _ in column_type_value:
            insert_query += f", {column}"
        insert_query += ") VALUES (?, ?"
        for _, _, value in column_type_value:
            insert_query += ", ?"
        insert_query += ")"
        cursor.execute(insert_query, [key] + [value for _, _, value in column_type_value])
        conn.commit()
        conn.close()

    def get_all_values_from_sort_key_table(self, table_name: str, key: str) -> list[dict[str, Any]]:
        conn = self._db_connection()
        cursor = conn.cursor()
        cursor.execute(f"SELECT value, sort_key FROM {table_name} WHERE key=?", (key,))
        result = cursor.fetchall()
        conn.close()
        return [{"value": data[0], "sort_key": data[1]} for data in result]

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

    def create_function(
        self,
        function_name: str,
        role_arn: str,
        zip_contents: bytes,
        runtime: str,
        handler: str,
        environment_variables: dict[str, str],
        timeout: int,
        memory_size: int,
    ) -> str:
        return ""

    def update_function(
        self,
        function_name: str,
        role_arn: str,
        zip_contents: bytes,
        runtime: str,
        handler: str,
        environment_variables: dict[str, str],
        timeout: int,
        memory_size: int,
    ) -> str:
        return ""
