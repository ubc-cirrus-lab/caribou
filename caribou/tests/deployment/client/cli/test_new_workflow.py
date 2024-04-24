import unittest
from unittest.mock import patch, mock_open

from caribou.deployment.client.cli.new_workflow import create_new_workflow_directory


class TestCreateNewWorkflowDirectory(unittest.TestCase):
    @patch("os.path.exists", return_value=False)
    @patch("os.getcwd", return_value="/current/directory")
    @patch("os.path.abspath", return_value="/path/to/your_module.py")
    @patch("os.path.dirname", return_value="/path/to")
    @patch("shutil.copytree")
    @patch("os.walk", return_value=[("/new/workflow/dir", [], ["file1", "file2"])])
    @patch("builtins.open", new_callable=mock_open, read_data="{{ workflow_name }}")
    def test_create_new_workflow_directory(
        self, mock_file, mock_walk, mock_copytree, mock_dirname, mock_abspath, mock_getcwd, mock_exists
    ):
        workflow_name = "test_workflow"
        create_new_workflow_directory(workflow_name)
        mock_exists.assert_called_once_with("/current/directory/test_workflow")
        mock_copytree.assert_called_once_with("/path/to/template", "/current/directory/test_workflow")
        mock_file.assert_any_call("/new/workflow/dir/file1", "r", encoding="utf-8")
        mock_file.assert_any_call("/new/workflow/dir/file1", "w", encoding="utf-8")
        mock_file.assert_any_call("/new/workflow/dir/file2", "r", encoding="utf-8")
        mock_file.assert_any_call("/new/workflow/dir/file2", "w", encoding="utf-8")
        file_handle = mock_file()
        file_handle.write.assert_any_call("test_workflow")

    @patch("os.path.exists", return_value=True)
    @patch("os.getcwd", return_value="/current/directory")
    @patch("os.path.abspath", return_value="/path/to/your_module.py")
    @patch("os.path.dirname", return_value="/path/to")
    def test_create_new_workflow_directory_already_exists(self, mock_dirname, mock_abspath, mock_getcwd, mock_exists):
        workflow_name = "test_workflow"
        with self.assertRaises(RuntimeError):
            create_new_workflow_directory(workflow_name)
        mock_exists.assert_called_once_with("/current/directory/test_workflow")


if __name__ == "__main__":
    unittest.main()
