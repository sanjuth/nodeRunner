import threading
from fastapi import FastAPI, Request
import uvicorn
from time import time
from node import Node
import argparse
import random


app = FastAPI()

class TimerThread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        random_num = random.randint(5, 15)
        self.interval = random_num
        self.timer = None

    def run(self):
        global last_request_time
        last_request_time = time()
        self.timer = threading.Timer(self.interval, self.run)
        self.timer.start()
        while True:
            # print(time() - last_request_time)
            if time() - last_request_time > self.interval:
                print("Timed Out Asking for Votes ------------------------")
                try:
                    node.request_vote([0,1,2,3,4])    
                except:
                    print("req failed")
                self.timer.cancel()
                self.timer = threading.Timer(self.interval, self.run)
                self.timer.start()
                last_request_time = time()


def run_server():
    @app.post("/askVote")
    async def askVote(req:Request):
        global last_request_time
        last_request_time = time()
        body=await req.json()
        print(body)
        res=node.send_vote(body["term"],body["id"],body["last_log_index"],body["last_log_term"])
        print(res)
        return res

    
    uvicorn.run(app, host="0.0.0.0", port=port)



if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--id", type=int, help="id no. of the node on this node")
    parser.add_argument("--port", type=int, help="Port number to run the server on")
    args = parser.parse_args()

    global node
    node = Node(args.id)
    global port
    port = args.port or 8000

    server_thread = threading.Thread(target=run_server)
    server_thread.start()

    global last_request_time
    last_request_time = time()

    timer_thread = TimerThread()
    timer_thread.start()

    # while True:
    #     do_something_else()
