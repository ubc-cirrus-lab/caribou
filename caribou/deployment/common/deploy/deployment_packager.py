from __future__ import annotations

import functools
import hashlib
import inspect
import os
import shutil
import subprocess
import sys
import tempfile
import zipfile
from typing import Optional

import boto3
import pip
import yaml
import zstandard

import caribou
from caribou.common.models.remote_client.remote_client import RemoteClient
from caribou.deployment.common.config.config import Config
from caribou.deployment.common.deploy.models.workflow import Workflow


class DeploymentPackager:
    def __init__(self, config: Config) -> None:
        self._config = config
        self._pytz_version_cache: Optional[str] = None

    def build(self, config: Config, workflow: Workflow) -> None:
        if config.project_dir is None:
            raise RuntimeError("project_dir must be defined")
        zip_file = self._create_deployment_package(config.project_dir, config.python_version)
        for deployment_package in workflow.get_deployment_packages():
            deployment_package.filename = zip_file

    def re_build(self, workflow: Workflow, remote_client: RemoteClient) -> None:
        zip_file = self._download_deployment_package(remote_client)
        for deployment_package in workflow.get_deployment_packages():
            deployment_package.filename = zip_file

    def _download_deployment_package(self, remote_client: RemoteClient) -> str:
        deployment_package_filename = tempfile.mktemp(suffix=".zip")

        deployment_package_content = remote_client.download_resource(f"deployment_package_{self._config.workflow_id}")

        if deployment_package_content is None:
            raise RuntimeError("Could not download deployment package")

        with open(deployment_package_filename, "wb") as f:
            f.write(deployment_package_content)

        return deployment_package_filename

    def _create_deployment_package(self, project_dir: str, python_version: str) -> str:
        package_filename = self._get_package_filename(project_dir, python_version)
        self._create_deployment_package_dir(package_filename)
        if os.path.exists(package_filename):
            return package_filename
        # with tempfile.TemporaryDirectory() as temp_dir:
        requirements_filename = self._get_requirements_filename(project_dir)
        if not os.path.exists(requirements_filename):
            raise RuntimeError(f"Could not find requirements file: {requirements_filename}")
        self._ensure_requirements_filename_complete(requirements_filename)
        # self._build_dependencies(requirements_filename, temp_dir) # TODO: Re-add when we add more providers
        with zipfile.ZipFile(package_filename, "w", zipfile.ZIP_DEFLATED) as z:
            # self._add_py_dependencies(z, temp_dir) # TODO: Re-add when we add more providers
            self._add_application_files(z, project_dir)
            self._add_caribou_dependency(z)
            self._add_requirements_file(z, requirements_filename)
        return package_filename

    def _ensure_requirements_filename_complete(self, requirements_filename: str) -> None:
        with open(requirements_filename, "r", encoding="utf-8") as file:
            requirements = file.read().splitlines()

        requirements = [requirement.split("==")[0] for requirement in requirements]

        with open(requirements_filename, "a", encoding="utf-8") as file:
            if "boto3" not in requirements:
                file.write(f"\nboto3=={boto3.__version__}\n")
            if "pyyaml" not in requirements:
                file.write(f"\npyyaml=={yaml.__version__}\n")
            if "pytz" not in requirements:
                file.write(f"\npytz=={self._pytz_version}\n")
            if "zstandard" not in requirements:
                file.write(f"\nzstandard=={zstandard.__version__}\n")

    @property
    def _pytz_version(self) -> str:
        # pytz sadly does not have a __version__ attribute
        if self._pytz_version_cache is not None:
            return self._pytz_version_cache
        pytz_version = subprocess.check_output([sys.executable, "-m", "pip", "show", "pytz"]).decode("utf-8")
        pytz_version = next(
            line.split(":")[1].strip() for line in pytz_version.splitlines() if line.startswith("Version:")
        )
        if pytz_version is None:
            raise RuntimeError("Could not find pytz version")
        self._pytz_version_cache = pytz_version
        return pytz_version

    def _add_requirements_file(self, zip_file: zipfile.ZipFile, requirements_filename: str) -> None:
        zip_file.write(requirements_filename, "requirements.txt")

    def _add_caribou_dependency(self, zip_file: zipfile.ZipFile) -> None:
        caribou_path = inspect.getfile(caribou)
        if caribou_path.endswith(".pyc"):
            caribou_path = caribou_path[:-1]
        caribou_deployment_path = os.path.join(os.path.dirname(caribou_path), "deployment")

        deployment_paths = [
            ("client", "__init__.py"),
            ("client", "caribou_workflow.py"),
            ("client", "caribou_function.py"),
            ("common", "__init__.py"),
            ("common", "deploy", "__init__.py"),
            ("common", "deploy", "models", "__init__.py"),
            ("common", "deploy", "models", "resource.py"),
            ("common", "deploy", "models", "instructions.py"),
        ]

        for deployment_path in deployment_paths:
            full_path = os.path.join(caribou_deployment_path, *deployment_path)
            if os.path.exists(full_path):
                zip_file.write(
                    full_path,
                    os.path.join("caribou", "deployment", *deployment_path),
                )
            else:
                raise RuntimeError(f"Could not find file: {full_path}")

        self._add_init_file(zip_file, caribou_deployment_path, "deployment")

        common_paths = [
            ("models", "endpoints.py"),
            ("models", "__init__.py"),
            ("constants.py",),
            ("provider.py",),
            ("utils.py",),
            ("models", "remote_client", "__init__.py"),
            ("models", "remote_client", "aws_remote_client.py"),
            ("models", "remote_client", "integration_test_remote_client.py"),
            ("models", "remote_client", "mock_remote_client.py"),
            ("models", "remote_client", "remote_client.py"),
            ("models", "remote_client", "remote_client_factory.py"),
        ]

        caribou_path = os.path.dirname(caribou_path)

        for common_path in common_paths:
            full_path = os.path.join(caribou_path, "common", *common_path)
            if os.path.exists(full_path):
                zip_file.write(
                    full_path,
                    os.path.join("caribou", "common", *common_path),
                )
            else:
                raise RuntimeError(f"Could not find file: {full_path}")

        self._add_init_file(zip_file, caribou_path, "common")
        self._add_init_file(zip_file, caribou_path, "")

    def _add_init_file(self, zip_file: zipfile.ZipFile, path: str, destination_location: str) -> None:
        super_init = os.path.join(path, "__init__.py")
        if os.path.exists(super_init):
            if destination_location != "":
                zip_file.write(super_init, os.path.join("caribou", destination_location, "__init__.py"))
            else:
                zip_file.write(super_init, os.path.join("caribou", "__init__.py"))
        else:
            raise RuntimeError(f"Could not find file: {super_init}")

    def _add_py_dependencies(self, zip_file: zipfile.ZipFile, deps_dir: str) -> None:
        prefix_len = len(deps_dir) + 1
        for root, _, files in os.walk(deps_dir):
            for filename in files:
                full_path = os.path.join(root, filename)
                zip_path = full_path[prefix_len:]
                zip_file.write(full_path, zip_path)

    def _add_application_files(self, zip_file: zipfile.ZipFile, project_dir: str) -> None:
        src_dir = os.path.join(project_dir, "src")
        if not os.path.exists(src_dir):
            os.makedirs(src_dir)

        with open(os.path.join(src_dir, ".gitkeep"), "w", encoding="utf-8"):
            pass

        for root, _, files in os.walk(project_dir):
            for filename in files:
                if filename.endswith(".pyc"):
                    continue

                full_path = os.path.join(root, filename)
                if full_path == os.path.join(project_dir, "app.py") or full_path.startswith(
                    os.path.join(project_dir, "src")
                ):
                    zip_path = full_path[len(project_dir) + 1 :]
                    zip_file.write(full_path, zip_path)

    def _get_package_filename(self, project_dir: str, python_version: str) -> str:
        requirements = self._get_requirements_filename(project_dir)
        hashed_project_dir = self._hash_project_dir(requirements, project_dir)
        filename = (
            f"{hashed_project_dir}-{python_version}-{self._config.workflow_name}-{self._config.workflow_version}.zip"
        )
        deployment_package_filename = os.path.join(project_dir, ".caribou", "deployment-packages", filename)
        return deployment_package_filename

    def _get_requirements_filename(self, project_dir: str) -> str:
        requirements_filename = os.path.join(project_dir, "requirements.txt")
        return requirements_filename

    def _hash_project_dir(self, requirements: str, project_dir: str) -> str:
        contents = b""
        if os.path.exists(requirements):
            with open(requirements, "rb") as f:
                contents = f.read()
        hashed_content = hashlib.sha256(contents)
        for root, _, files in os.walk(project_dir):
            for file in files:
                with open(os.path.join(root, file), "rb") as f:
                    reader = functools.partial(f.read, 4096)
                    for chunk in iter(reader, b""):
                        hashed_content.update(chunk)
        return hashed_content.hexdigest()

    def _create_deployment_package_dir(self, package_filename: str) -> None:
        os.makedirs(os.path.dirname(package_filename), exist_ok=True)

    def _build_dependencies(self, requirements_filename: str, temp_dir: str) -> None:
        with open(requirements_filename, "r", encoding="utf-8") as file:
            requirements = file.read().splitlines()

        # Add version of boto3 if not present in requirements
        if "boto3" not in requirements:
            requirements.append(f"boto3=={boto3.__version__}")

        # Add version of pyyaml if not present in requirements
        if "pyyaml" not in requirements:
            requirements.append(f"pyyaml=={yaml.__version__}")

        if "pytz" not in requirements:
            requirements.append(f"pytz=={self._pytz_version}")

        # Add version of z_standard if not present in requirements
        if "zstandard" not in requirements:
            requirements.append(f"zstandard=={zstandard.__version__}")

        temp_install_dir = tempfile.mkdtemp(dir=temp_dir, prefix="temp_install_")

        try:
            pip_args = ["--target", temp_install_dir] + requirements
            pip_execute("install", pip_args)

            for item in os.listdir(temp_install_dir):
                item_path = os.path.join(temp_install_dir, item)
                if os.path.isdir(item_path):
                    shutil.move(item_path, temp_dir)
        finally:
            shutil.rmtree(temp_install_dir)

    def create_framework_package(self, project_dir: str, tmpdirname: str) -> str:
        filename = "caribou_framework_cli.zip"
        package_filename = os.path.join(tmpdirname, ".caribou", "deployment-packages", filename)
        self._create_deployment_package_dir(package_filename)
        if os.path.exists(package_filename):
            return package_filename

        with zipfile.ZipFile(package_filename, "w", zipfile.ZIP_DEFLATED) as z:
            self._add_framework_deployment_files(z, project_dir)
            self._add_framework_files(z, project_dir)
            self._add_framework_go_files(z, project_dir)

        return package_filename

    def _add_framework_deployment_files(self, zip_file: zipfile.ZipFile, project_dir: str) -> None:
        for root, _, files in os.walk(project_dir):
            for filename in files:
                if filename.endswith(".pyc"):
                    continue

                full_path = os.path.join(root, filename)
                if (
                    full_path == os.path.join(project_dir, "app.py")
                    or full_path.startswith(os.path.join(project_dir, "src"))
                    or filename == "pyproject.toml"
                    or filename == "poetry.lock"
                    or filename == "README.md"
                ):
                    zip_path = full_path[len(project_dir) + 1 :]
                    zip_file.write(full_path, zip_path)

        # Copy and rename remote_caribou handler
        remote_cli_handler_path = os.path.join(
            project_dir, "caribou", "deployment", "client", "remote_cli", "remote_cli_handler.py"
        )
        zip_file.write(remote_cli_handler_path, "app.py")

    def _add_framework_files(self, zip_file: zipfile.ZipFile, project_dir: str) -> None:
        framework_dir = os.path.join(project_dir, "caribou")

        for root, _, files in os.walk(project_dir):
            for filename in files:
                if not filename.endswith(".py") or filename.startswith("test_"):  # Only add .py files, also skip tests
                    continue

                full_path = os.path.join(root, filename)
                if full_path.startswith(framework_dir):
                    zip_path = full_path[len(project_dir) + 1 :]
                    zip_file.write(full_path, zip_path)

    def _add_framework_go_files(self, zip_file: zipfile.ZipFile, project_dir: str) -> None:
        framework_go_dir = os.path.join(project_dir, "caribou-go")
        allowed_extensions = [".go", ".py", ".sum", ".mod", ".sh", ".so"]

        for root, _, files in os.walk(project_dir):
            for filename in files:
                if not any(
                    filename.endswith(ext) for ext in allowed_extensions
                ):  # Check if file has an allowed extension
                    continue

                # Skip the tests directory
                if filename.endswith("_test.go"):
                    continue

                full_path = os.path.join(root, filename)
                if full_path.startswith(framework_go_dir):
                    zip_path = full_path[len(project_dir) + 1 :]
                    zip_file.write(full_path, zip_path)


def pip_execute(command: str, args: list[str]) -> tuple[bytes, bytes]:
    import_string = pip_import_string()
    main_args = [command] + args
    env_vars = os.environ.copy()
    python_exe = sys.executable
    run_pip = f"import sys; {import_string}; main({main_args})"

    with subprocess.Popen(
        [python_exe, "-c", run_pip],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env_vars,
    ) as process:
        out, err = process.communicate()
        if process.returncode != 0:
            raise RuntimeError(f"Error installing dependencies: {err.decode('utf-8')}")
    return out, err


def pip_import_string() -> str:
    # This is a copy of the pip_import_string function from chalice

    pip_major_version = int(pip.__version__.split(".", maxsplit=1)[0])
    pip_minor_version = int(pip.__version__.split(".", maxsplit=2)[1])
    pip_major_minor = (pip_major_version, pip_minor_version)
    if (9, 0) <= pip_major_minor < (10, 0):
        return "from pip import main"
    if (10, 0) <= pip_major_minor < (19, 3):
        return "from pip._internal import main"
    if (19, 3) <= pip_major_minor < (20, 0):
        return "from pip._internal.main import main"
    if pip_major_minor >= (20, 0):
        return "from pip._internal.cli.main import main"
    raise RuntimeError(f"Unknown import string for pip version: {str(pip_major_minor)}")
