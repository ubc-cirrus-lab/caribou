import time
import urllib.parse
import numpy as np
import pickle

from data_loader.libsvm_dataset import DenseDatasetWithFile

if __name__=="__main__":
    startTs = time.time()
    num_features = 30
    num_classes = 2
    num_file = 10
    parse_start = time.time()

    for index in range(num_file):

        file = "/home/ubuntu/code/data/higgs_{}.pkl".format(index)
        print(file)
        f = open(file,"rb")
        tmp = pickle.load(f)
        f.close()
        if index == 0:
            result = tmp;
        else:
            result.ins_list = result.ins_list+tmp.ins_list
            #result.ins_list_np = result.ins_list_np+tmp.ins_list_np
        #print(tmp.ins_np)
            result.label_list = result.label_list+tmp.label_list


    #result.ins_np = np.array(result.ins_list_np)
    #result.label_np = np.array(result.label_list).reshape(len(result.label_list), 1)
    print(len(result))
    r = open("/home/ubuntu/code/data/s3.pkl","wb")
    pickle.dump(result,r)
    r.close()
    print("parse time = {} s".format(time.time()-parse_start))
