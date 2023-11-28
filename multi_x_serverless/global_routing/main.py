import argparse

import yaml


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=str, default="./multi_x_serverless/config/global_routing/config.yaml")

    args = parser.parse_args()
    config_path = args.config

    with open(config_path, "r", encoding="utf-8") as file:
        config = yaml.safe_load(file)

    print(config)


if __name__ == "__main__":
    main()
