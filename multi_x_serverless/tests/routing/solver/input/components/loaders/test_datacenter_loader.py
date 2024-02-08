datacenter_data = {
        "aws:region1": {
                "execution_cost": {
                        "invocation_cost": {"arm64": 2.3e-7, "x86_64": 2.3e-7, "free_tier_invocations": 1000000},
                        "compute_cost": {"arm64": 1.56138e-5, "x86_64": 1.95172e-5, "free_tier_compute_gb_s": 400000},
                        "unit": "USD",
                },
                "transmission_cost": {"global_data_transfer": 0.09, "provider_data_transfer": 0.02, "unit": "USD/GB"},
                "pue": 1.15,
                "cfe": 0.9,
                "average_memory_power": 3.92e-6,
                "average_cpu_power": 0.00212,
                "available_architectures": ["arm64", "x86_64"],
        }
}
