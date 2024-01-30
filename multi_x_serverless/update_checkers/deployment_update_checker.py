from multi_x_serverless.update_checkers.update_checker import UpdateChecker

class DeploymentUpdateChecker(UpdateChecker):
    def __init__(self, name):
        super().__init__(name)

    def check(self):
        print(f"Checking for updates for {self.name} deployment")
        return True