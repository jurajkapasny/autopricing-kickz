import logging
import logging.handlers as handlers

BASE_LEVEL = logging.INFO
LOG_FORMAT = '%(asctime)s - %(name)s - %(funcName)s - %(levelname)s - %(message)s'
PRINT_FORMAT = '%(asctime)s - %(name)s - %(funcName)s - %(levelname)s - %(message)s'
DATEFMT = '%Y-%m-%d %H:%M:%S'
MAX_BYTES = 10000000 # 10Mb
BACKUP_COUNT = 5

class Logger:
    def __init__(self,logger=''):
        self.logger = logging.getLogger(logger)
        self.logger.setLevel(BASE_LEVEL)
    
    def _setup_filehandler(self, filename, level, format, maxBytes, backupCount):
        fh = handlers.RotatingFileHandler(filename, maxBytes=maxBytes, backupCount=backupCount)
        fh.setLevel(level)
        fh.setFormatter(logging.Formatter(format, DATEFMT))
        self.logger.addHandler(fh)
    
    def _setup_printer(self, level, format):
        pt = logging.StreamHandler()
        pt.setLevel(level)
        pt.setFormatter(logging.Formatter(format,DATEFMT))
        self.logger.addHandler(pt)
    
    def get_full_logger(self, filename,
                        log_level=BASE_LEVEL,
                        log_format=LOG_FORMAT,
                        print_level=BASE_LEVEL,
                        print_format=LOG_FORMAT,
                        maxBytes=MAX_BYTES, 
                        backupCount=BACKUP_COUNT):
        self._setup_filehandler(filename, log_level, log_format, maxBytes, backupCount)
        self._setup_printer(print_level, print_format)

        return self.logger
    
    def get_file_logger(self,filename, 
                        level=BASE_LEVEL, 
                        format=LOG_FORMAT,
                        maxBytes=MAX_BYTES, 
                        backupCount=BACKUP_COUNT):
        self._setup_filehandler(filename, level, format, maxBytes, backupCount)

        return self.logger  
    
    def get_print_logger(self, level=BASE_LEVEL, format=PRINT_FORMAT):
        self._setup_printer(level, format)
        
        return self.logger