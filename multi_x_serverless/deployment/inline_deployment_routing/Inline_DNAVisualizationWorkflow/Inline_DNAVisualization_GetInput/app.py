from chalice import Chalice

import base64
import json
import datetime
import logging
import uuid
from random import randint
from sys import getsizeof
import numpy as np
import random

app = Chalice(app_name='Inline_DNAVisualization_GetInput')


@app.route('/')
def index(methods=['PUT']):
    request = app.current_request
    body = request.json_body

    gen_file_name = body['gen_file_name']

    message_json = json.dumps({
        'genFileName': gen_file_name,
    })

    message_bytes = message_json.encode('utf-8')
    msgID = uuid.uuid4().hex







# The view function above will return {"hello": "world"}
# whenever you make an HTTP GET request to '/'.
#
# Here are a few more examples:
#
# @app.route('/hello/{name}')
# def hello_name(name):
#    # '/hello/james' -> {"hello": "james"}
#    return {'hello': name}
#
# @app.route('/users', methods=['POST'])
# def create_user():
#     # This is the JSON body the user sent in their POST request.
#     user_as_json = app.current_request.json_body
#     # We'll echo the json body back to the user in a 'user' key.
#     return {'user': user_as_json}
#
# See the README documentation for more examples.
#
