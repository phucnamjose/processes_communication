from utils import JobPool

S = JobPool(ip="0.0.0.0", port=55550, authkey="conga")
if __name__ == "__main__":
    S.start()