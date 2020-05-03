def run(server):

    import logging, os

    # Create a custom logger
    logger = logging.getLogger('pyql-cluster')

    # Create handlers
    i_handler = logging.StreamHandler()
    c_handler = logging.StreamHandler()
    f_handler = logging.FileHandler('pyql-cluster.log')
    d_handler = logging.StreamHandler()
    i_handler.setLevel(logging.INFO)
    c_handler.setLevel(logging.WARNING)
    f_handler.setLevel(logging.ERROR)
    d_handler.setLevel(logging.DEBUG)

    # Create formatters and add it to handlers
    c_format = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
    f_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    i_handler.setFormatter(f_format)
    c_handler.setFormatter(f_format)
    f_handler.setFormatter(f_format)

    # Add handlers to the logger
    logger.addHandler(i_handler)
    logger.addHandler(c_handler)
    logger.addHandler(f_handler)
    if server.PYQL_DEBUG == True:    
        logger.addHandler(d_handler)
        logger.setLevel(logging.DEBUG)
    
    def log_and_return(log_func):
        def log(message):
            log_func(message)
            return message
        return log
    
    class log_return:
        def __init__(self, loggr):
            self.debug = log_and_return(loggr.debug)
            self.error = log_and_return(loggr.error)
            self.warning = log_and_return(loggr.warning)
            self.info = log_and_return(loggr.info)
            self.exception = log_and_return(loggr.exception)
    server.log = log_return(logger)