from multi_x_serverless.update_checkers.solver_update_checker import SolverUpdateChecker

def test_solve_and_redeploy():
    # This test runs a data collection process of the data that we have on a region level (without a deployed workflow)
    print("Test solve and redeploy.")

    solver_update_checker = SolverUpdateChecker()