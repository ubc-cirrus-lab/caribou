import time

import torch
from torch.autograd import Variable
from torch.utils.data.sampler import SubsetRandomSampler

from archived.elasticache.Memcached import memcached_init
from archived.s3.get_object import get_object
from archived.s3 import put_object

from archived.old_model import LogisticRegression
from data_loader.libsvm_dataset import DenseDatasetWithLines

# lambda setting
grad_bucket = "s3-grads"
model_bucket = "s3-updates"
local_dir = "/tmp"
w_prefix = "w_"
b_prefix = "b_"
w_grad_prefix = "w_grad_"
b_grad_prefix = "b_grad_"

# algorithm setting
learning_rate = 0.05
num_epochs = 30
validation_ratio = .2
shuffle_dataset = True
random_seed = 42


def handler(event, context):
    startTs = time.time()
    bucket = event['bucket']
    key = event['name']
    num_features = event['num_features']
    num_classes = event['num_classes']
    elasti_location = event['elasticache']
    endpoint = memcached_init(elasti_location)
    #endpoint = elasti_location
    print('bucket = {}'.format(bucket))
    print('key = {}'.format(key))
    key_splits = key.split("_")
    worker_index = int(key_splits[0])
    #num_worker = int(key_splits[1])
    num_worker = event['num_files']
    batch_size = 100000/8
    batch_size = int(np.ceil(batch_size/num_worker))

    # read file(dataset) from s3
    file = get_object(bucket, key).read().decode('utf-8').split("\n")
    print("read data cost {} s".format(time.time() - startTs))
    parse_start = time.time()
    dataset = DenseDatasetWithLines(file, num_features)
    preprocess_start = time.time()
    print("libsvm operation cost {}s".format(parse_start - preprocess_start))

    # Creating data indices for training and validation splits:
    dataset_size = len(dataset)
    print("dataset size = {}".format(dataset_size))
    indices = list(range(dataset_size))
    split = int(np.floor(validation_ratio * dataset_size))
    if shuffle_dataset:
        np.random.seed(random_seed)
        np.random.shuffle(indices)
    train_indices, val_indices = indices[split:], indices[:split]

    # Creating PT data samplers and loaders:
    train_sampler = SubsetRandomSampler(train_indices)
    valid_sampler = SubsetRandomSampler(val_indices)
    train_loader = torch.utils.data.DataLoader(dataset,
                                               batch_size=batch_size,
                                               sampler=train_sampler)
    validation_loader = torch.utils.data.DataLoader(dataset,
                                                    batch_size=batch_size,
                                                    sampler=valid_sampler)
    print("preprocess data cost {} s".format(time.time() - preprocess_start))


    model = LogisticRegression(num_features, num_classes)
    # Loss and Optimizer
    # Softmax is internally computed.
    # Set parameters to be updated.
    criterion = torch.nn.CrossEntropyLoss()
    optimizer = torch.optim.SGD(model.parameters(), lr=learning_rate)

    train_loss = []
    test_loss = []
    test_acc = []
    epoch_time = 0
    # Training the Model
    epoch_start = time.time()
    for epoch in range(num_epochs):
        tmp_train = 0
        batch_start = time.time()
        for batch_index, (items, labels) in enumerate(train_loader):
            print("------worker {} epoch {} batch {}------".format(worker_index, epoch, batch_index))
            items = Variable(items.view(-1, num_features))
            labels = Variable(labels)

            # Forward + Backward + Optimize
            optimizer.zero_grad()
            outputs = model(items)
            loss = criterion(outputs, labels)
            loss.backward()
            tmp_calculation_time = time.time()-batch_start
            print("forward and backward cost {} s".format(tmp_calculation_time))

            #gradients = [param.grad.data.numpy() for param in model.parameters()]
            w_grad = model.linear.weight.grad.data.numpy()
            b_grad = model.linear.bias.grad.data.numpy()
            #synchronization starts from that every worker writes their gradients of this batch and epoch
            #print("model size = {}".format((sys.getsizeof(w_grad.tobytes())+sys.getsizeof(b_grad.tobytes()))/1024/1024))

            sync_start = time.time()
            hset_object(endpoint, grad_bucket, w_grad_prefix + str(worker_index), w_grad.tobytes())
            hset_object(endpoint, grad_bucket, b_grad_prefix + str(worker_index), b_grad.tobytes())
            tmp_write_local_epoch_time = time.time()-sync_start
            #print("write local gradient cost = {}".format(tmp_write_local_epoch_time))

            #merge gradients among files
            file_postfix = "{}_{}".format(epoch, batch_index)
            if worker_index == 0:
                w_grad_merge, b_grad_merge = \
                    merge_w_b_grads(endpoint,
                                    grad_bucket, num_worker, w_grad.dtype,
                                    w_grad.shape, b_grad.shape,
                                    w_grad_prefix, b_grad_prefix)
                #possible rewrite the file before being accessed. wait until anyone finishes accessing.
                #while sync_counter(endpoint, model_bucket, num_worker):
                #    time.sleep(0.01)
                put_merged_w_b_grads(endpoint,model_bucket,
                                    w_grad_merge, b_grad_merge, file_postfix,
                                    w_grad_prefix, b_grad_prefix)
                #delete_expired_w_b(endpoint,
                #                   model_bucket, epoch, batch_index, w_grad_prefix, b_grad_prefix,end)
                model.linear.weight.grad = Variable(torch.from_numpy(w_grad_merge))
                model.linear.bias.grad = Variable(torch.from_numpy(b_grad_merge))
            else:
                w_grad_merge, b_grad_merge = get_merged_w_b_grads(endpoint,model_bucket, file_postfix,
                                                                    w_grad.dtype, w_grad.shape, b_grad.shape,
                                                                    w_grad_prefix, b_grad_prefix)
                #hcounter(endpoint, model_bucket, "counter")#flag it if it's accessed.
                #print("number of access at this time = {}".format(int(hget_object(endpoint, model_bucket, "counter"))))
                model.linear.weight.grad = Variable(torch.from_numpy(w_grad_merge))
                model.linear.bias.grad = Variable(torch.from_numpy(b_grad_merge))
            tmp_sync_time = time.time() - sync_start
            #print("synchronization cost {} s".format(tmp_sync_time))

            optimizer.step()
            batch_cost = time.time()-batch_start
            print("batch cost = {}".format(batch_cost))
            tmp_train = tmp_train+loss.item()
            batch_start = time.time()
            
        epoch_time = epoch_time + time.time()-epoch_start
        print("epoch_time = {},train_loss = {}".format(epoch_time,tmp_train/(batch_index+1)))
        train_loss.append(tmp_train/(batch_index+1))
        # Test the Model every epoch
        correct = 0
        total = 0
        tmp_test = 0
        count = 0
        for items, labels in validation_loader:
            items = Variable(items.view(-1, num_features))
            labels = Variable(labels)
            outputs = model(items)
            loss = criterion(outputs, labels)
            _, predicted = torch.max(outputs.data, 1)
            total += labels.size(0)
            correct += (predicted == labels).sum()
            tmp_test = tmp_test + loss.item()
            count = count+1
        print('Accuracy of the model on the %d test samples: %d %%' % (len(val_indices), 100 * correct / total))
        test_loss.append(tmp_test/count)
        test_acc.append(100 * correct / total)
        epoch_start = time.time()

    endTs = time.time()
    print("elapsed time = {} s".format(endTs - startTs))
    loss_record = [test_loss,test_acc,train_loss,epoch_time]
    put_object("grad-average-loss","grad-loss{}".format(worker_index),pickle.dumps(loss_record))
