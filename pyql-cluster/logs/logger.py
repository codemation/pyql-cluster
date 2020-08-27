def run(server):

    import logging, os

    # Create a custom logger
    logger = logging.getLogger('pyql-cluster')
    logger.propagate = False
    if logger.hasHandlers()):
        logger.handlers.clear()
        
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
    
    class LogReturn:
        def __init__(self):
            self.debug = log_and_return(logger.debug)
            self.error = log_and_return(logger.error)
            self.warning = log_and_return(logger.warning)
            self.info = log_and_return(logger.info)
            self.exception = log_and_return(logger.exception)
    server.log = LogReturn()