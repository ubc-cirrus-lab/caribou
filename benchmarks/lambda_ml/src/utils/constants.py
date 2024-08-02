

# prefix
class Prefix:
    w_prefix = "w_"
    b_prefix = "b_"
    tmp_w_prefix = "tmp_w_"
    tmp_b_prefix = "tmp_b_"

    w_b_prefix = "w_b_"
    tmp_w_b_prefix = "tmp_w_b_"

    w_grad_prefix = "w_g_"
    b_grad_prefix = "b_g_"
    tmp_w_grad_prefix = "tmp_w_g_"
    tmp_b_grad_prefix = "tmp_b_g_"

    w_b_grad_prefix = "w_b_g_"
    tmp_w_b_grad_prefix = "tmp_w_b_g_"

    KMeans_Init_Cent = "init_cent_"
    KMeans_Cent = "cent_"


class MLModel:
    Linear_Models = ["lr", "svm"]
    Sparse_Linear_Models = ["sparse_lr", "sparse_svm"]
    Cluster_Models = ["kmeans", "sparse_kmeans"]
    Deep_Models = ["resnet", "mobilenet"]


class Optimization:
    Grad_Avg = "grad_avg"
    Model_Avg = "model_avg"
    ADMM = "admm"
    All = [Grad_Avg, Model_Avg, ADMM]


class Synchronization:
    Async = "async"
    Reduce = "reduce"
    Reduce_Scatter = "reduce_scatter"
    All = [Async, Reduce, Reduce_Scatter]
