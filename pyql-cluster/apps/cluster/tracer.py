import time, uuid

class Tracer:
    def __init__(self, name, root=None, logger=None):
        self.logger = logger
        self.name = name
        self.root = root
        self.op_id = None
        if self.root == None:
            self.op_id = str(uuid.uuid1())
        self.start = time.time()
    def get_root_operation(self):
        if not self.root == None:
            return self.root.get_root_operation()
        return self.op_id
    def get_callers(self, path=''):
        if self.root == None:
            return f"{self.op_id} {self.name}{path}"
        else:
            path = f" --> {self.name}{path}"
            return self.root.get_callers(path)
    def get_root_caller_duration(self):
        if self.root == None:
            return time.time() - self.start
        else:
            return self.root.get_root_caller_duration()
    def log(self, message):
        root_duration = self.get_root_caller_duration()
        local_duration = time.time() - self.start
        return f"{self.get_callers()} - {root_duration:.3f} - {local_duration:.3f}s - {message}"

    def debug(self, message):
        self.logger.debug(self.log(message))
        return message
    def error(self, message):
        self.logger.error(self.log(message))
        return message
    def warning(self, message):
        self.logger.warning(self.log(message))
        return message
    def info(self, message):
        self.logger.info(self.log(message))
        return message
    def exception(self, message):
        self.logger.exception(self.log(message))
        return message
    def __call__(self, message): 
        self.logger.warning(self.log(message))
        return message

def trace(func):
    def traced(*args, **kwargs):
        func_name = f'{func}'.split(' ')[1].split('.')[-1]
        if not 'trace' in kwargs:
            kwargs['trace'] = Tracer(func_name)
        else:
            kwargs['trace'] = Tracer(func_name, kwargs['trace'])
        return func(*args, **kwargs)
    return traced

def get_tracer(logger):
    def trace(func):
        def traced(*args, **kwargs):
            func_name = f'{func}'.split(' ')[1].split('.')[-1]
            if not 'trace' in kwargs:
                kwargs['trace'] = Tracer(func_name, logger=logger)
            else:
                kwargs['trace'] = Tracer(func_name, kwargs['trace'], logger=logger)
            return func(*args, **kwargs)
        traced.__name__ = func.__name__
        return traced
    return trace