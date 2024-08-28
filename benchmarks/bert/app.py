from typing import Any

import json
import boto3
from tempfile import TemporaryDirectory
from transformers import BertTokenizer
from pytorch_SUT import get_pytorch_sut
import logging
from create_squad_data import read_squad_examples, convert_examples_to_features
from accuracy_squad import load_loadgen_log, write_predictions
import mlperf_loadgen as lg
import argparse
import re
import numpy as np

from caribou.deployment.client import CaribouWorkflow

s3_bucket_name = "dn-caribou-bert"
s3_bucket_region_name = "us-east-1"

workflow = CaribouWorkflow(name="bert", version="0.0.1")

logger = logging.getLogger()
logger.setLevel(logging.INFO)

@workflow.serverless_function(
    name="prepare_input",
    entry_point=True,
)
def prepare_input(event: dict[str, Any]) -> dict[str, Any]:
    
    if isinstance(event, str):
        event = json.loads(event)
    
    for key, value in event.items():
        if key == "vocab_file":
            vocab_file_name = value
        
        if key == "dataset_file":
            dataset_file_name = value

    s3 = boto3.client("s3", region_name=s3_bucket_region_name)

    with TemporaryDirectory() as tmp_dir:
        vocab_file = f"{tmp_dir}/vocab.txt"
        s3.download_file(s3_bucket_name, vocab_file_name, vocab_file)

        dataset_file = f"{tmp_dir}/dataset.json"
        s3.download_file(s3_bucket_name, dataset_file_name, dataset_file)

    with open(vocab_file, "r") as f:
        vocab = f.read()

    tokenizer = BertTokenizer.from_pretrained(vocab)
    eval_examples = read_squad_examples(input_file=dataset_file, is_training=False, version_2_with_negative=False)

    eval_features = []
    def append_feature(feature):
        eval_features.append(feature)

    convert_examples_to_features(
        examples=eval_examples,
        tokenizer=tokenizer,
        max_seq_length=384,
        doc_stride=128,
        max_query_length=64,
        is_training=False,
        output_fn=append_feature,
        verbose_logging=False
    )

    workflow.invoke_serverless_function(run_inference, {"eval_features": eval_features})

    return {"status": 200}


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--backend", choices=["tf", "pytorch", "onnxruntime", "tf_estimator"], default="pytorch", help="Backend")
    parser.add_argument("--scenario", choices=["SingleStream", "Offline",
                        "Server", "MultiStream"], default="Offline", help="Scenario")
    parser.add_argument("--accuracy", action="store_true",
                        help="enable accuracy pass")
    parser.add_argument("--quantized", action="store_true",
                        help="use quantized model (only valid for onnxruntime backend)")
    parser.add_argument("--profile", action="store_true",
                        help="enable profiling (only valid for onnxruntime backend)")
    parser.add_argument(
        "--mlperf_conf", default="build/mlperf.conf", help="mlperf rules config")
    parser.add_argument("--user_conf", default="user.conf",
                        help="user config for user LoadGen settings such as target QPS")
    parser.add_argument("--max_examples", type=int,
                        help="Maximum number of examples to consider (not limited by default)")
    parser.add_argument("-a", "--addr", dest="addr", default="0.0.0.0", help="IP address")
    parser.add_argument("-p", "--port", dest="port", default="50051", help="serve port")
    args = parser.parse_args()
    return args

scenario_map = {
    "SingleStream": lg.TestScenario.SingleStream,
    "Offline": lg.TestScenario.Offline,
    "Server": lg.TestScenario.Server,
    "MultiStream": lg.TestScenario.MultiStream
}

def parse_summary_file(summary_file):
    keys = ["Min latency (ns)", "Max latency (ns)", "Mean latency (ns)"]
    res_dic = {}
    with open(summary_file) as f:
        try:
            text = f.read()
            for key in keys:
                val = extract_text_between_strings(text,key,"\n")

                res_dic[key] = int(val.split(': ')[-1])
        except Exception as e:
            # print(e)
            return None
    return res_dic
        

def extract_text_between_strings(text, str1, str2):
    try:
        pattern = re.escape(str1) + r'(.*?)' + re.escape(str2)
        match = re.search(pattern, text, re.DOTALL)
        if match:
            extracted_text = match.group(1)
            return extracted_text
        else:
            raise Exception('Pattern not found in the text for start: %s'%(str1))
    except IOError:
        raise Exception('Failed to read the file')


@workflow.serverless_function(name="run_inference")
def run_inference(event: dict[str, Any]) -> dict[str, Any]:

    # Load eval_features from previous step
    eval_features = json.load(event["eval_features"])
    
    s3 = boto3.client("s3", region_name=s3_bucket_region_name)
    with TemporaryDirectory() as tmp_dir:
        # Use pytorch model for now 
        model_path = f"{tmp_dir}/model.pytorch"
        s3.download_file(s3_bucket_name, "model.pytorch", model_path)

    with TemporaryDirectory() as tmp_dir:

        args = get_args()

        sut = get_pytorch_sut(args, model_path, eval_features)

        settings = lg.TestSettings()
        settings.scenario = scenario_map[args.scenario]
        settings.FromConfig(args.mlperf_conf, "bert", args.scenario)
        settings.FromConfig(args.user_conf, "bert", args.scenario)

        if args.accuracy:
            settings.mode = lg.TestMode.AccuracyOnly
        else:
            settings.mode = lg.TestMode.PerformanceOnly

        log_output_settings = lg.LogOutputSettings()
        log_output_settings.outdir = f"{tmp_dir}/logs"
        log_output_settings.copy_summary_to_stdout = True
        log_settings = lg.LogSettings()
        log_settings.log_output = log_output_settings
        log_settings.enable_trace = True

        lg.StartTestWithLogSettings(sut.sut, sut.qsl.qsl, settings, log_settings)

        lg.DestroySUT(sut.sut)

        lg.DestroyQSL(sut.qsl.qsl)

        latency_dict = parse_summary_file(f"{tmp_dir}/logs/mlperf_log_summary.txt")

        workflow.invoke_serverless_function(postprocess, 
            {
                "inference_results": json.dumps(latency_dict),
            }
        )

        return {"status": 200}
    

@workflow.serverless_function(name="postprocess")
def postprocess(event: dict[str, Any]) -> dict[str, Any]:
    inference_results = event["inference_results"]
    eval_features = event["eval_features"]

    with TemporaryDirectory() as tmp_dir:
        with open(f"{tmp_dir}/inference_results.json", "w") as f:
            f.write(inference_results)

        inference_results = json.loads(inference_results)
        eval_features = json.loads(eval_features)

        s3 = boto3.client("s3", region_name=s3_bucket_region_name)
        with TemporaryDirectory() as tmp_dir:
            # dataset 
            dataset_file = f"{tmp_dir}/dataset.json"
            s3.download_file(s3_bucket_name, "bert_dataset.json", dataset_file)
        
        eval_examples = read_squad_examples(input_file=dataset_file, is_training=False)

        results = load_loadgen_log(f"{tmp_dir}/inference_results.json", eval_features, dtype=np.float32, output_transposed=False)

        write_predictions(
            all_examples=eval_examples,
            all_features=eval_features,
            all_results=results,
            n_best_size=20,
            max_answer_length=30,
            do_lower_case=True,
            output_prediction_file=f"{tmp_dir}/predictions.json",
        )

        s3.upload_file(f"{tmp_dir}/predictions.json", s3_bucket_name, "predictions.json")
        
        return {"status": 200}