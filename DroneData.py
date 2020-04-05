import time


class DroneData():
    def __init__(self):
        self.uitvoering_id = 1
        self.drone_id = 1
        self.timestamp = int(round(time.time() * 1000))
        self.drone_lat = 0.000000
        self.drone_long = 0.000000
        self.batterij_duur = 100

    def new_update(self):
        self.timestamp = self.timestamp + 1
