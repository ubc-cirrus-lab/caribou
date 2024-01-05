import unittest
from unittest.mock import patch, MagicMock
import tempfile
from multi_x_serverless.deployment.client.deploy.deployment_packager import (
    DeploymentPackager,
    pip_import_string,
)
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
        os.mkdir(os.path.join(self.test_dir, ".multi-x-serverless"))

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

    @patch("multi_x_serverless.deployment.client.deploy.deployment_packager.pip_execute")
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

    @patch("os.walk")
    @patch("zipfile.ZipFile")
    def test__add_application_files(self, mock_zipfile, mock_os_walk):
        mock_os_walk.return_value = [("/app_dir", [], ["src/file1.py", "src/file2.py"])]
        mock_zipfile.return_value.__enter__.return_value = MagicMock()

        config = MagicMock()
        packager = DeploymentPackager(config)
        packager._add_application_files(mock_zipfile, "/app_dir")

        self.assertEqual(mock_zipfile.write.call_count, 2)

    @patch("os.path.join")
    @patch("zipfile.ZipFile")
    def test__add_mutli_x_serverless_dependency(self, mock_zipfile, mock_os_path_join):
        tmp_dir = self.test_dir
        mock_os_path_join.return_value = os.path.join(tmp_dir)
        mock_zipfile.return_value.__enter__.return_value = MagicMock()

        config = MagicMock()
        packager = DeploymentPackager(config)
        packager._add_mutli_x_serverless_dependency(mock_zipfile, tmp_dir)

        self.assertEqual(mock_zipfile.write.call_count, 4)


if __name__ == "__main__":
    unittest.main()
