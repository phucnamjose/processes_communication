from utils import QueueManager as QM, JobClient

QM.register("job_queue_dict")


HOST_IP = "0.0.0.0"
HOST_PORT = 55550
KEY = b"conga"

if __name__ == "__main__":
    print(f"Connecting to server '{HOST_IP}:{HOST_PORT}'")
    try:
        j = JobClient(ip=HOST_IP, port=HOST_PORT, authkey=KEY)
    except Exception as e:
        print("Error connecting to queue server: '{}'".format(e.__str__()))
        exit(0)
    while True:
        in_data = input("Please enter any string to send: ")
        print(f"Sending '{in_data}' to other process")

        in_data_list = in_data.split("|")
        if len(in_data_list) > 1:
            j.addJob(key=in_data_list[0], value=in_data_list[1:])
            continue
        j.addJob(in_data)
