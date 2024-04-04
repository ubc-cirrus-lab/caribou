from typing import Any

from multi_x_serverless.deployment.client import MultiXServerlessWorkflow
import markdown
import base64
import json

workflow = MultiXServerlessWorkflow(name="markdown2html", version="0.0.1")


@workflow.serverless_function(
    name="Markdown2HTML",
    entry_point=True,    
)
def first_function(event: dict[str, Any]) -> dict[str, Any]:
    if isinstance(event, str):
        event = json.loads(event)

    if "markdown" in event:
        text = event["markdown"]
    else:
        raise ValueError("No message provided")

    decoded_text = base64.b64decode(text.encode()).decode()

    html = markdown.markdown(decoded_text)

    return {"html_response": html}
