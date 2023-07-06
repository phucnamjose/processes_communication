from threading import Thread
from multiprocessing.managers import BaseManager as bm
from time import sleep


# this class is the base to share data between processes
class QueueManager(bm):
    pass

class JobPool(Thread):
    """
        This class create a queue server that hosts a job pool and the corresponding result pool for jobs that have been processed
    """
    def __init__(self, **args):
        self.__job_pool = {}
        self.__result_pool = {}
        self.__connection_info = {"ip": args.get("ip", ""),
                                  "port": args.get("port", 50000),
                                  "authkey": args.get("authkey", '')}
        if not isinstance(self.__connection_info["authkey"], bytes):
            self.__connection_info["authkey"] = str(self.__connection_info["authkey"]).encode("ascii")
        Thread.__init__(self)

    def run(self):
        QueueManager.register("job_queue_dict", callable=lambda:self.__job_pool)
        QueueManager.register("result_queue_dict", callable=lambda:self.__result_pool)
        manager = QueueManager(address=(self.__connection_info["ip"],
                                        self.__connection_info["port"]),
                               authkey=self.__connection_info["authkey"])
        s = manager.get_server()
        #manager.start()
        print("Started host process for exchange data")
        s.serve_forever()

class JobClient():
    def __init__(self, **args):
        self.__client_name = args.get("name", self.generateName())
        self.__connection_info = {"ip": args.get("ip", ""),
                                  "port": args.get("port", 50000),
                                  "authkey": args.get("authkey", '')}
        self.__job_callback = args.get("callback", None)
        QueueManager.register("job")
        self.__manager = QueueManager(address=(self.__connection_info["ip"],
                                               self.__connection_info["port"]),
                                      authkey=self.__connection_info["authkey"])
        self.__manager.connect()

    def addJob(self, str_data=None, key=None, value=None):
        if key and value:
            self.__manager.job_queue_dict().update({key: value})
            if str_data is not None:
                print("  Warning: Key value provided, ignore str_data")
            return
        if str_data is not None:
            self.__manager.job_queue_dict().update({self.__client_name: str_data})

    def getNextJob(self):
        ret = None
        try:
            ret = self.__manager.job_queue_dict().popitem()
        except KeyError:
            if ret is not None:
                return ret
        return ret

    def pushResult(self, result_data):
        if not isinstance(result_data, dict):
            return
        self.__manager.result_queue_dict().update({self.__client_name: result_data})

    def setCallback(self, callback):
        if callable(callback):
            self.__job_callback = callback

    def waitForJob(self):
        if self.__job_callback is None:
            print("No callback is set, no job will be processed")
            return

        print(f"Client {self.__client_name} is waiting for jobs...")
        while True:
            job = self.getNextJob()
            if job is not None and self.__job_callback is not None:
                self.__job_callback(job)
            sleep(0.05)

    @staticmethod
    def generateName():
        from uuid import uuid4
        return f"{uuid4()}"
