import datetime
import logging
import logging.config
import os
from contextlib import contextmanager
from pathlib import Path

import boto3
import yaml
from botocore.exceptions import NoCredentialsError
from jsonschema import validate


class Logger:
    def __init__(
        self,
        config_path: str = "./multi_x_serverless/config/logging/config.yaml",
        schema_path: str = "./multi_x_serverless/config/logging/config_schema.yaml",
        component_name: str = "remote_logging",
        bucket: str = "multi-x-serverless-logs",
    ) -> None:
        with open(config_path, "r", encoding="utf-8") as file:
            self.__config = yaml.safe_load(file)

        with open(schema_path, "r", encoding="utf-8") as file:
            schema = yaml.safe_load(file)

        if not self.__config or not schema:
            raise ValueError("Config or schema is empty!")

        aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
        aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
        self.__bucket = bucket

        self.__s3 = boto3.client("s3", aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

        validate(instance=self.__config, schema=schema)

        self.__component_name: str = component_name

        self.update_logger()

    def store_log(self, reset_logger: bool = False) -> None:
        if os.path.exists(self.log_filename) and self.connect():
            self.upload_file(self.log_filename, self.log_filename.name)

        if reset_logger:
            self.update_logger()

    def connect(self) -> bool:
        try:
            self.__s3.list_buckets()
        except NoCredentialsError:
            return False
        return True

    def upload_file(self, file_name_and_path: Path, file_name: str) -> None:
        self.__s3.upload_file(file_name_and_path, self.__bucket, file_name)

    def update_logger(self) -> None:
        parent_parent_parent_dir = Path(__file__).parent.parent.parent
        logs_dir = parent_parent_parent_dir / "logs" / self.__component_name
        Path(logs_dir).mkdir(parents=True, exist_ok=True)

        log_filename = (
            logs_dir / f"{self.__component_name}_{datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S-%f')}.log"
        )

        self.log_filename: Path = Path(log_filename)

        self.__config["handlers"]["file_handler"]["filename"] = str(self.log_filename)

        logging.config.dictConfig(self.__config)
        self.logger = logging.getLogger(self.__component_name)

    def info(self, message: str) -> None:
        self.logger.info(message)

    def error(self, message: str) -> None:
        self.logger.error(message)

    def warning(self, message: str) -> None:
        self.logger.warning(message)

    def debug(self, message: str) -> None:
        self.logger.debug(message)


@contextmanager
def get_logger(component_name: str = "test", remote: bool = False) -> Logger:
    # usage: with get_logger(__name__) as logger:
    if remote:
        logger = Logger(component_name=component_name)
        try:
            yield logger
        finally:
            logger.store_log()
    else:
        logger = logging.getLogger(component_name)
        logger.setLevel(logging.DEBUG)
        logger.addHandler(logging.StreamHandler())
        try:
            yield logger
        finally:
            pass
