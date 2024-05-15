from caribou.deployment.common.config.config import Config
from caribou.deployment.common.deploy.deployer import Deployer
from caribou.deployment.common.factories.deployer_factory import DeployerFactory
from caribou.common.models.remote_client.integration_test_remote_client import IntegrationTestRemoteClient


def test_deploy(workflow_dir: str):
    # This test deploys the application to the integration test mock provider (an sqlite database)
    print("Test deploying the application.")

    deployer_factory = DeployerFactory(workflow_dir)
    config: Config = deployer_factory.create_config_obj()
    deployer: Deployer = deployer_factory.create_deployer(config=config)

    deployer.deploy([config.home_region])

    remote_client = IntegrationTestRemoteClient()

    # Check that the functions are deployed
    deployed_functions = remote_client.select_all_from_table("functions")

    assert len(deployed_functions) == 7

    expected_function_names = [
        'integration_test_workflow-0_0_1-First_Function_IntegrationTestProvider-rivendell',
        'integration_test_workflow-0_0_1-Second_Function_IntegrationTestProvider-rivendell',
        'integration_test_workflow-0_0_1-Third_Function_IntegrationTestProvider-rivendell',
        'integration_test_workflow-0_0_1-Fourth_Function_IntegrationTestProvider-rivendell',
        'integration_test_workflow-0_0_1-Fifth_Function_IntegrationTestProvider-rivendell',
        'integration_test_workflow-0_0_1-Sixth_Function_IntegrationTestProvider-rivendell',
        'integration_test_workflow-0_0_1-Seventh_Function_IntegrationTestProvider-rivendell'
    ]

    for function in deployed_functions:
        assert function[0] in expected_function_names
    
    added_resources = remote_client.select_all_from_table("resources")

    assert len(added_resources) == 1

    assert added_resources[0][0] == "deployment_package_integration_test_workflow-0.0.1"

    deployed_roles = remote_client.select_all_from_table("roles")

    assert len(deployed_roles) == 7

    expected_role_names = [
        'integration_test_workflow-0_0_1-First_Function_IntegrationTestProvider-rivendell-role',
        'integration_test_workflow-0_0_1-Second_Function_IntegrationTestProvider-rivendell-role',
        'integration_test_workflow-0_0_1-Third_Function_IntegrationTestProvider-rivendell-role',
        'integration_test_workflow-0_0_1-Fourth_Function_IntegrationTestProvider-rivendell-role',
        'integration_test_workflow-0_0_1-Fifth_Function_IntegrationTestProvider-rivendell-role',
        'integration_test_workflow-0_0_1-Sixth_Function_IntegrationTestProvider-rivendell-role',
        'integration_test_workflow-0_0_1-Seventh_Function_IntegrationTestProvider-rivendell-role'
    ]

    for role in deployed_roles:
        assert role[0] in expected_role_names

    deployed_messaging_topics = remote_client.select_all_from_table("messaging_topics")

    expected_topic_names = [
        'integration_test_workflow-0_0_1-First_Function_IntegrationTestProvider-rivendell',
        'integration_test_workflow-0_0_1-Second_Function_IntegrationTestProvider-rivendell',
        'integration_test_workflow-0_0_1-Third_Function_IntegrationTestProvider-rivendell',
        'integration_test_workflow-0_0_1-Fourth_Function_IntegrationTestProvider-rivendell',
        'integration_test_workflow-0_0_1-Fifth_Function_IntegrationTestProvider-rivendell',
        'integration_test_workflow-0_0_1-Sixth_Function_IntegrationTestProvider-rivendell',
        'integration_test_workflow-0_0_1-Seventh_Function_IntegrationTestProvider-rivendell'
    ]

    for topic in deployed_messaging_topics:
        assert topic[0] in expected_topic_names

    deployed_messaging_subscriptions = remote_client.select_all_from_table("messaging_subscriptions")

    for subscription in deployed_messaging_subscriptions:
        assert subscription[1] in expected_topic_names
        assert subscription[2] in expected_function_names

    print("Application deployed successfully.")
