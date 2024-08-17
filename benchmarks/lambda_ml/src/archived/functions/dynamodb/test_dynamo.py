import time
import boto3
from boto3.dynamodb.conditions import Key

import numpy as np

# lambda setting
grad_tb_name = "ml-grads"
model_tb_name = "ml-models"
model_size = 30


def np2str(arr):
    return str(arr.tolist()).strip('[]')


def str2np(str, shape):
    return np.array([float(x) for x in str.split(',')])


def handler(event, context):
    worker_index = event['rank']
    num_workers = event['num_workers']
    num_epochs = event['num_epochs']
    num_iters = event['num_iters']

    print('number of workers = {}'.format(num_workers))
    print('worker index = {}'.format(worker_index))
    print('num epochs = {}'.format(num_epochs))
    print('num iterations = {}'.format(num_iters))
    print('model size = {}'.format(model_size))

    dynamodb = boto3.resource('dynamodb')
    model_tb = dynamodb.Table(model_tb_name)
    grad_tb = dynamodb.Table(grad_tb_name)

    start_time = time.time()

    for epoch in range(num_epochs):
        for iter in range(num_iters):
            grad_id = "{}_{}_{}".format(worker_index, epoch, iter)
            grad = np.random.rand(1, model_size)
            grad_str = np2str(grad)
            #print("write grad {}".format(grad_id))
            grad_tb.put_item(
                Item={
                    'grad_id': grad_id,
                    'worker_id': worker_index,
                    'epoch_id': epoch,
                    'iter_id': iter,
                    'value': grad_str
                }
            )
            if worker_index == 0:
                merged_grads = np.random.rand(1, model_size)
                # merge grads
                for w_id in range(num_workers):
                    grad_id = "{}_{}_{}".format(w_id, epoch, iter)
                    grad_exist = False
                    while grad_exist is not True:
                        response = grad_tb.query(
                            KeyConditionExpression=Key('grad_id').eq(grad_id)
                        )
                        num_grads = len(response['Items'])
                        if num_grads == 1:
                            grad_item = response['Items'][0]
                            grad_str = grad_item['value']
                            grad_arr = str2np(grad_str, (1, model_size))
                            #print("read grad {}".format(grad_id))
                            merged_grads += grad_arr
                            grad_exist = True
                            response = grad_tb.delete_item(
                                Key={
                                    'grad_id': grad_id
                                }
                            )
                # write merged grads to model table
                model_id = "{}_{}".format(epoch, iter)
                model_str = np2str(merged_grads)
                print("write model {}".format(model_id))
                model_tb.put_item(
                    Item={
                        'model_id': model_id,
                        'epoch_id': epoch,
                        'iter_id': iter,
                        'value': model_str
                    }
                )

                # while num_grads < num_workers:
                #     response = grad_tb.query(
                #         KeyConditionExpression=Key('epoch_id').eq(epoch) & Key('iter_id').eq(iter)
                #     )
                #     num_grads = len(response['Items'])
                #     if num_grads == num_workers:
                #         for grad_item in response['Items']:
                #             grad_str = grad_item['value']
                #             grad_arr = np.fromstring(grad_str)
                #             merged_grads += grad_arr
                #         # write merged grads to model table
                #         model_id = "{}_{}".format(epoch, iter)
                #         model_str = merged_grads.tostring()
                #         model_tb.put_item(
                #             Item={
                #                 'model_id': model_id,
                #                 'epoch_id': epoch,
                #                 'iter_id': iter,
                #                 'value': model_str
                #             }
                #         )
            else:
                # wait for the next model
                exist_model = False
                while exist_model is not True:
                    model_id = "{}_{}".format(epoch, iter)
                    response = model_tb.query(
                        KeyConditionExpression=Key('model_id').eq(model_id)
                    )
                    num_model = len(response['Items'])
                    if num_model == 1:
                        model_item = response['Items'][0]
                        model_str = model_item['value']
                        model_arr = str2np(model_str, (1, model_size))
                        print("read model {}".format(model_id))
                        exist_model = True

    end_time = time.time()
    total_time = end_time - start_time
    avg_sync_time = total_time / (num_epochs * num_iters)
    print("cost {} s, each sync cost {} s".format(total_time, avg_sync_time))


# np_arr = np.random.rand(1, model_size)
# np_str = np2str(np_arr)
# print(np_str)
# np_arr = str2np(np_str, (1, model_size))
# print(np_arr)
