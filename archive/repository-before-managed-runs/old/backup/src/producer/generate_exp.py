import yaml
from itertools import product

def generate_experiment_yaml(
        base_config_file="pipeline-configmap.yaml",
        output_file="pipeline-configmap-experiments.yaml",
        target_rates=[500, 1000, 2000],
        batch_sizes=[16384, 32768, 65536]
):
    with open(base_config_file, 'r') as f:
        base_yaml = yaml.safe_load(f)

    experiments = []

    for rate, batch in product(target_rates, batch_sizes):
        config_copy = yaml.safe_load(yaml.dump(base_yaml))  # Deep copy
        config_copy['metadata']['name'] = f"pipeline-configmap-tr{rate}-bs{batch}"
        config_copy['data']['TARGET_RATE'] = str(rate)
        config_copy['data']['BATCH_SIZE'] = str(batch)

        experiments.append(config_copy)

    with open(output_file, 'w') as f:
        yaml.dump_all(experiments, f)

    print(f"✅ Generated {len(experiments)} experimental configs in '{output_file}'.")

if __name__ == "__main__":
    generate_experiment_yaml(
        base_config_file="pipeline-configmap.yaml",
        output_file="pipeline-configmap-experiments.yaml",
        target_rates=[500, 1000, 2000],
        batch_sizes=[16384, 32768, 65536]
    )

