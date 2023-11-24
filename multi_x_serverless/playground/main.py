from multi_x_serverless.shared.remote_logging import get_logger

with get_logger(__name__) as logger:

    logger.info("Test info message")

    logger.error("Test error message")

    logger.debug("Test debug message")

    logger.warning("Test warning message")

    def test_function(logger):
        logger.info("Test info message")

        logger.error("Test error message")

        logger.debug("Test debug message")

        logger.warning("Test warning message")

        raise Exception("Test exception")

    test_function(logger)