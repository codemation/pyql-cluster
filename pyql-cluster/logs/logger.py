def run(server):

    import logging

    # Create a custom logger
    logger = logging.getLogger('pyql-cluster')

    # Create handlers
    i_handler = logging.StreamHandler()
    c_handler = logging.StreamHandler()
    f_handler = logging.FileHandler('pyql-cluster.log')
    i_handler.setLevel(logging.INFO)
    c_handler.setLevel(logging.WARNING)
    f_handler.setLevel(logging.ERROR)

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
    server.log = logger