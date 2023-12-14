import os
from distutils.dir_util import copy_tree
import os
from distutils.dir_util import copy_tree


def create_new_workflow_directory(workflow_name: str) -> None:
    template_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "template")

    template_kwargs = {
        "workflow_name": workflow_name,
    }

    new_workflow_dir = os.path.join(os.getcwd(), workflow_name)

    os.mkdir(new_workflow_dir)

    copy_tree(template_dir, new_workflow_dir)

    for root, _, files in os.walk(new_workflow_dir):
        for file in files:
            file_path = os.path.join(root, file)
            with open(file_path, "r") as f:
                content = f.read()
            for key, value in template_kwargs.items():
                content = content.replace("{{ " + key + " }}", value)
            with open(file_path, "w") as f:
                f.write(content)
