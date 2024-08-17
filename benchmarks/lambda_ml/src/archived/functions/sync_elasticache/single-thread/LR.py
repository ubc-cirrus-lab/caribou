import time
import numpy as np
import pickle

import torch
from torch.autograd import Variable
from torch.utils.data.sampler import SubsetRandomSampler


from archived.old_model import LogisticRegression

# lambda setting


# algorithm setting

learning_rate = [0.1]#np.arange(0.0001,0.0009,0.0001)
batch_size = 100000
num_epochs = 10
validation_ratio = .2
shuffle_dataset = True
random_seed = 42





def handler():

    num_features = 30
    num_classes = 2
    file = "/home/ubuntu/code/data/s3.pkl"
    #file = "./ec2/s3.pkl"
    batch_size = 100000
    # read file(dataset) from s3
    parse_start = time.time()
    f = open(file,"rb")
    dataset = pickle.load(f)
    print("processing time = {}".format(time.time()-parse_start))

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

    #print("preprocess data cost {} s".format(time.time() - preprocess_start))




    # Loss and Optimizer
    # Softmax is internally computed.
    # Set parameters to be updated.
    testset = [[inputs,targets] for inputs,targets in validation_loader]
    length_test = len(testset)
    test_loss = []
    train_loss = []
    acc = []
    for lr in learning_rate:

        model = LogisticRegression(num_features, num_classes)
        criterion = torch.nn.CrossEntropyLoss()
        optimizer = torch.optim.SGD(model.parameters(), lr=lr)
    # Training the Model

        total_time = 0;
        for epoch in range(num_epochs):
            temp_test = []
            temp_train = []
            temp_acc = []
            for batch_index, (items, labels) in enumerate(train_loader):
                #print("------worker {} epoch {} batch {}------".format(worker_index, epoch, batch_index))
                batch_start = time.time()
                items = Variable(items.view(-1, num_features))
                labels = Variable(labels)

                # Forward + Backward + Optimize
                optimizer.zero_grad()#eliminate the acumulated gradients.
                outputs = model(items)
                loss = criterion(outputs, labels)
                loss.backward()#calculating the gradients.
                optimizer.step()#only after this step will the model be updated.

                #print("weights are of the scale = {}".format(model.linear.weight.data.numpy()))
                #print("gradients are of the scale = {}".format(model.linear.weight.grad.numpy()))
                #print("batch cost {} s".format(time.time() - batch_start))
                total_time += time.time()-batch_start;
                if (batch_index + 1) % 1 == 0:
                    print('Epoch: [%d/%d], Step: [%d/%d], Loss: %.4f, learning rate: %.4f'
                          % (epoch + 1, num_epochs, batch_index + 1, len(train_indices) / batch_size, loss.data, lr))
                #if (batch_index + 1) % 11 == 0:
            #        #a,_ = test(model,train_loader,criterion)
                #    train_loss.append(loss.data)
                     a,b = test(model,testset[batch_index%length_test],criterion)
                    test_loss.append(a)
                    acc.append(b)
                #print("operation time = {}".format(time.time()-startTs))




        #train_loss.append(temp_train)
        #test_loss.append(temp_test)
        #acc.append(temp_acc)

    print("total time = {}".format(total_time))
    loss = np.array([test_loss,train_loss,acc])
    f = open("/home/ubuntu/code/lambda/loss.pkl","wb")
    pickle.dump(loss,f)
    f.close()
    #f = open("/home/ubuntu/code/lambda/loss_argmin.pkl","wb")
    #pickle.dump(learning_rate[np.argmin(loss,axis=1)],f)
    #f.close()
    #print(learning_rate[np.argmin(loss,axis=1)])


def test(model,testloader,criterion):
    # Test the Model
    correct = 0
    total = 0
    total_loss = 0
    items, labels = testloader[0],testloader[1]
    with torch.no_grad():

        outputs = model(items)
        _, predicted = torch.max(outputs.data, 1)
        total += labels.size(0)
        correct += (predicted == labels).sum()
        loss = criterion(outputs,labels)
        total_loss+=loss.data
    return total_loss, float(correct)/float(total)
