from utils import QueueManager as QM, JobClient

QM.register("job_queue_dict")

HOST_IP = "0.0.0.0"
HOST_PORT = 55550
KEY = b"conga"

def dataCallbackFunction(data):
    print(f"recieving '{data}'")

if __name__ == "__main__":
    print(f"Connecting to server '{HOST_IP}:{HOST_PORT}'")
    try:
        j = JobClient(ip=HOST_IP, port=HOST_PORT, authkey=KEY)
    except Exception as e:
        print("Error connecting to queue server: '{}'".format(e.__str__()))
        exit(0)

    j.setCallback(dataCallbackFunction)
    j.waitForJob()