from multi_x_serverless.update_checkers.update_checker import UpdateChecker


class SolverUpdateChecker(UpdateChecker):
    # TODO (#110): Implement SolverUpdateChecker

    def check(self) -> None:
        print(f"Checking for updates for {self.name} solver")
