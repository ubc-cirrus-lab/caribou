import os
import shutil


def create_new_workflow_directory(workflow_name: str) -> None:
    template_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "template")

    template_kwargs = {
        "workflow_name": workflow_name,
    }

    new_workflow_dir = os.path.join(os.getcwd(), workflow_name)

    if os.path.exists(new_workflow_dir):
        raise RuntimeError(f"Workflow directory {new_workflow_dir} already exists")

    shutil.copytree(template_dir, new_workflow_dir)

    for root, _, files in os.walk(new_workflow_dir):
        for file in files:
            file_path = os.path.join(root, file)
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()
            for key, value in template_kwargs.items():
                content = content.replace("{{ " + key + " }}", value)
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(content)
