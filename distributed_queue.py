from dask.distributed import Queue , Client

class MessageQue_:
    
    def __init__(self,queue_name):
        self.client = Client(address="dask-scheduler:8786")
        self.queue=Queue(queue_name)
    def append(self,data):
        future = self.client.scatter(data)
        self.queue.put(future)

    def get(self):
        try:
            return self.queue.get().result()
        except IndexError:
            return False
