import unittest
from unittest.mock import patch, MagicMock, Mock, mock_open
import tempfile
from caribou.deployment.common.deploy.deployment_packager import (
    DeploymentPackager,
    pip_import_string,
)
from caribou.deployment.common.config.config import Config
from caribou.deployment.common.deploy.models.workflow import Workflow
import zipfile
import os
import shutil


class TestDeploymentPackager(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    @patch("os.path.exists")
    @patch("tempfile.TemporaryDirectory")
    @patch("zipfile.ZipFile")
    def test_create_deployment_package(self, mock_zipfile, mock_temp_dir, mock_exists):
        mock_exists.return_value = True
        mock_temp_dir.return_value.__enter__.return_value = self.test_dir
        mock_zipfile.return_value.__enter__.return_value = MagicMock()

        # Make requirements.txt
        with open(os.path.join(self.test_dir, "requirements.txt"), "w") as f:
            f.write("requests\n")

        # Make project structure
        os.mkdir(os.path.join(self.test_dir, ".caribou"))

        config = MagicMock()
        config.project_dir = self.test_dir
        config.python_version = "3.8"

        packager = DeploymentPackager(config)
        result = packager._create_deployment_package(config.project_dir, config.python_version)

        self.assertEqual(result, packager._get_package_filename(config.project_dir, config.python_version))

    @patch("os.path.exists")
    def test_create_deployment_package_no_requirements(self, mock_exists):
        mock_exists.return_value = False

        config = MagicMock()
        config.project_dir = self.test_dir
        config.python_version = "3.8"

        packager = DeploymentPackager(config)
        with self.assertRaises(RuntimeError):
            packager._create_deployment_package(config.project_dir, config.python_version)

    @patch("os.walk")
    @patch("zipfile.ZipFile")
    def test_add_py_dependencies(self, mock_zipfile, mock_os_walk):
        mock_os_walk.return_value = [("/deps_dir", [], ["file1.py", "file2.py"])]
        mock_zipfile.return_value.__enter__.return_value = MagicMock()

        config = MagicMock()
        packager = DeploymentPackager(config)
        packager._add_py_dependencies(mock_zipfile, "/deps_dir")

        self.assertEqual(mock_zipfile.write.call_count, 2)

    @patch("caribou.deployment.common.deploy.deployment_packager.pip_execute")
    def test__build_dependencies(self, mock_pip_execute):
        config = MagicMock()
        packager = DeploymentPackager(config)
        tmp_dir = self.test_dir
        # Make requirements.txt
        with open(os.path.join(tmp_dir, "requirements.txt"), "w") as f:
            f.write("requests\n")
        packager._build_dependencies(os.path.join(tmp_dir, "requirements.txt"), tmp_dir)

        mock_pip_execute.assert_called_once()

    def test_pip_import_string(self):
        result = pip_import_string()

        self.assertEqual(result, "from pip._internal.cli.main import main")

    @patch("builtins.open", new_callable=mock_open)
    @patch("os.path.exists")
    @patch("os.walk")
    @patch("zipfile.ZipFile")
    def test__add_application_files(self, mock_zipfile, mock_os_walk, mock_exists, mock_open):
        mock_os_walk.return_value = [("/app_dir", [], ["src/file1.py", "src/file2.py"])]
        mock_zipfile.return_value.__enter__.return_value = MagicMock()
        mock_exists.return_value = True

        config = MagicMock()
        packager = DeploymentPackager(config)
        packager._add_application_files(mock_zipfile, "/app_dir")

        self.assertEqual(mock_zipfile.write.call_count, 2)

    @patch("zipfile.ZipFile")
    def test__add_mutli_x_serverless_dependency(self, mock_zipfile):
        mock_zipfile.return_value.__enter__.return_value = MagicMock()

        config = MagicMock()
        packager = DeploymentPackager(config)
        packager._add_caribou_dependency(mock_zipfile)

        self.assertEqual(mock_zipfile.write.call_count, 22)

    @patch.object(DeploymentPackager, "_download_deployment_package", return_value="test.zip")
    def test_re_build(self, mock_download_deployment_package):
        config = Config({}, self.test_dir)
        workflow = Workflow("test_workflow", "0.0.1", [], [], [], config)
        remote_client = Mock()
        packager = DeploymentPackager(config)

        packager.re_build(workflow, remote_client)

        mock_download_deployment_package.assert_called_once_with(remote_client)
        for deployment_package in workflow.get_deployment_packages():
            self.assertEqual(deployment_package.filename, "test.zip")

    @patch.object(DeploymentPackager, "_create_deployment_package", return_value="test.zip")
    def test_build(self, mock_create_deployment_package):
        config = Config({}, self.test_dir)
        workflow = Workflow("test_workflow", "0.0.1", [], [], [], config)
        packager = DeploymentPackager(config)

        packager.build(config, workflow)

        mock_create_deployment_package.assert_called_once_with(self.test_dir, config.python_version)
        for deployment_package in workflow.get_deployment_packages():
            self.assertEqual(deployment_package.filename, "test.zip")

    @patch("tempfile.mktemp", return_value="test.zip")
    @patch("builtins.open", new_callable=mock_open)
    def test__download_deployment_package(self, mock_open, mock_mktemp):
        config = Config({}, self.test_dir)
        remote_client = Mock()
        remote_client.download_resource.return_value = b"test_content"
        packager = DeploymentPackager(config)

        result = packager._download_deployment_package(remote_client)

        self.assertEqual(result, "test.zip")
        mock_open.assert_called_once_with("test.zip", "wb")
        file_handle = mock_open()
        file_handle.write.assert_called_once_with(b"test_content")

    @patch("tempfile.mktemp", return_value="test.zip")
    @patch("builtins.open", new_callable=mock_open)
    def test__download_deployment_package_with_none_content(self, mock_open, mock_mktemp):
        config = Config({}, self.test_dir)
        remote_client = Mock()
        remote_client.download_resource.return_value = None
        packager = DeploymentPackager(config)

        with self.assertRaises(RuntimeError, msg="Could not download deployment package"):
            packager._download_deployment_package(remote_client)


if __name__ == "__main__":
    unittest.main()
