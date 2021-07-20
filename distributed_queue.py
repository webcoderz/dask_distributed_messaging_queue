from dask.distributed import Queue , Client


class MessageQue_:
    
    from dask.distributed import Queue , Client
    
    def __init__(self,name,pubsub=False):
        self.client = Client(address="dask-scheduler:8786")
        self.queue=Queue(name)
        self.name=name
        if pubsub:
            self.pub=Pub(name)
            self.sub=Sub(name)
        
    def append(self,data):
        future = self.client.scatter(data)
        self.queue.put(future)
        
    def get(self):
        try:
            return self.queue.get().result()
        except Exception as e:
            return False
              
    def publish(self,data):
        return self.pub.put(data)
                
    def subscribe(self):
        try:
            return self.sub.get()
        except Exception as e:
            return False
    
    def submit_function(self,data,func):
        future = self.client.submit(func, data)
        if self.pub:
            self.pub.put(future)
        else:
            self.queue.put(future)
            
    def get_q_size(self):
        return self.client.scheduler.queue_qsize(name=self.name)
        