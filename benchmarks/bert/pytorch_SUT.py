import array
import json
import os
import sys
import mlperf_loadgen as lg
import numpy as np
import torch
import transformers
from transformers import BertConfig, BertForQuestionAnswering
from squad_QSL import get_squad_QSL

class BERT_PyTorch_SUT():
    def __init__(self, args, model_file, query):
        print("Loading BERT configs...")
        with open("bert_config.json") as f:
            config_json = json.load(f)

        config = BertConfig(
            attention_probs_dropout_prob=config_json["attention_probs_dropout_prob"],
            hidden_act=config_json["hidden_act"],
            hidden_dropout_prob=config_json["hidden_dropout_prob"],
            hidden_size=config_json["hidden_size"],
            initializer_range=config_json["initializer_range"],
            intermediate_size=config_json["intermediate_size"],
            max_position_embeddings=config_json["max_position_embeddings"],
            num_attention_heads=config_json["num_attention_heads"],
            num_hidden_layers=config_json["num_hidden_layers"],
            type_vocab_size=config_json["type_vocab_size"],
            vocab_size=config_json["vocab_size"])

        self.dev = torch.device("cuda:0") if torch.cuda.is_available() else torch.device("cpu")
        print("Using: %s"%self.dev)
        self.version = transformers.__version__

        print("Loading PyTorch model...")
        self.model = BertForQuestionAnswering(config)
        self.model.to(self.dev)
        self.model.eval()
        self.model.load_state_dict(torch.load(model_file), strict=False)

        print("Constructing SUT...")
        self.sut = lg.ConstructSUT(self.issue_queries, query)
        print("Finished constructing SUT.")

        self.qsl = get_squad_QSL(args.max_examples)

    def issue_queries(self, query_samples):
        with torch.no_grad():
            print("query_samples number: %d"%len(query_samples))
            for i in range(len(query_samples)):
                eval_features = self.qsl.get_features(query_samples[i].index)
                model_output = self.model.forward(input_ids=torch.LongTensor(eval_features.input_ids).unsqueeze(0).to(self.dev),
                    attention_mask=torch.LongTensor(eval_features.input_mask).unsqueeze(0).to(self.dev),
                    token_type_ids=torch.LongTensor(eval_features.segment_ids).unsqueeze(0).to(self.dev))
                if self.version >= '4.0.0':
                    start_scores = model_output.start_logits
                    end_scores = model_output.end_logits
                else:
                    start_scores, end_scores = model_output
                output = torch.stack([start_scores, end_scores], axis=-1).squeeze(0).cpu().numpy()

                response_array = array.array("B", output.tobytes())
                bi = response_array.buffer_info()
                response = lg.QuerySampleResponse(query_samples[i].id, bi[0], bi[1])
                lg.QuerySamplesComplete([response])

    def __del__(self):
        print("Finished destroying SUT.")

def get_pytorch_sut(args, model_file, query):
    return BERT_PyTorch_SUT(args, model_file, query)