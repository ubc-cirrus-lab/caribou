from __future__ import annotations

import functools
import hashlib
import os
import shutil
import subprocess
import sys
import tempfile
import zipfile
from typing import Any

from chalice.compat import pip_import_string

from multi_x_serverless.deployment.client.config import Config
from multi_x_serverless.deployment.client.deploy.models import DeploymentPackage


class DeploymentPackager(object):
    def __init__(self, config: Config) -> None:
        self._config = config

    def build(self, config: Config, deployment_package: DeploymentPackage) -> None:
        zip_file = self._create_deployment_package(config.project_dir, config.python_version)
        deployment_package.filename = zip_file

    def _create_deployment_package(self, project_dir: str, python_version: str) -> str:
        package_filename = self._get_package_filename(project_dir, python_version)
        self._create_deployment_package_dir(package_filename)
        if os.path.exists(package_filename):
            return package_filename
        with tempfile.TemporaryDirectory() as temp_dir:
            requirements_filename = self._get_requirements_filename(project_dir)
            self._build_dependencies(requirements_filename, temp_dir)
            with zipfile.ZipFile(package_filename, "w", zipfile.ZIP_DEFLATED) as z:
                self._add_py_dependencies(z, temp_dir)
                self._add_application_files(z, project_dir)
        return package_filename

    def _add_py_dependencies(self, zip_file: zipfile.ZipFile, deps_dir: str) -> None:
        prefix_len = len(deps_dir) + 1
        for root, _, files in os.walk(deps_dir):
            for filename in files:
                full_path = os.path.join(root, filename)
                zip_path = full_path[prefix_len:]
                zip_file.write(full_path, zip_path)

    def _add_application_files(self, zip_file: zipfile.ZipFile, project_dir: str) -> None:
        for root, _, files in os.walk(project_dir):
            for filename in files:
                if filename.endswith(".pyc"):
                    continue

                full_path = os.path.join(root, filename)
                if full_path == os.path.join(project_dir, "app.py") or full_path.startswith(
                    os.path.join(project_dir, "lib")
                ):
                    zip_path = full_path[len(project_dir) + 1 :]
                    zip_file.write(full_path, zip_path)

    def _get_package_filename(self, project_dir: str, python_version: str) -> str:
        requirements = self._get_requirements_filename(project_dir)
        hash = self._hash_project_dir(requirements, project_dir)
        filename = f"{hash}-{python_version}.zip"
        deployment_package_filename = os.path.join(project_dir, ".multi-x-serverless", "deployment-packages", filename)
        return deployment_package_filename

    def _get_requirements_filename(self, project_dir: str) -> str:
        requirements_filename = os.path.join(project_dir, "requirements.txt")
        return requirements_filename

    def _hash_project_dir(self, requirements: str, project_dir: str) -> str:
        contents = b""
        if os.path.exists(requirements):
            with open(requirements, "rb") as f:
                contents = f.read()
        hash = hashlib.sha256(contents)
        for root, _, files in os.walk(project_dir):
            for file in files:
                with open(os.path.join(root, file), "rb") as f:
                    reader = functools.partial(f.read, 4096)
                    for chunk in iter(reader, b""):
                        hash.update(chunk)
        return hash.hexdigest()

    def _create_deployment_package_dir(self, package_filename: str) -> None:
        os.makedirs(os.path.dirname(package_filename), exist_ok=True)

    def _build_dependencies(self, requirements_filename: str, temp_dir: str) -> None:
        with open(requirements_filename, "r") as file:
            requirements = file.read().splitlines()

        temp_install_dir = tempfile.mkdtemp(dir=temp_dir, prefix="temp_install_")

        try:
            pip = Pip()
            pip_args = ["install", "--target", temp_install_dir] + requirements
            pip.execute("main", pip_args)

            for item in os.listdir(temp_install_dir):
                item_path = os.path.join(temp_install_dir, item)
                if os.path.isdir(item_path):
                    shutil.move(item_path, temp_dir)
        finally:
            shutil.rmtree(temp_install_dir)


class Pip(object):
    def __init__(self) -> None:
        self._import_string = pip_import_string()

    def execute(self, command: str, args: list[str]) -> None:
        main_args = [command] + args
        env_vars = self._osutils.environ()
        python_exe = sys.executable
        run_pip = f"import sys; {self._import_string}; sys.exit(main({main_args}))"
        out, err = subprocess.Popen(
            [python_exe, "-c", run_pip],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env_vars,
        ).communicate()
        return out, err
