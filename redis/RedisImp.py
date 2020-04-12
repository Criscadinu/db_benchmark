import redis
from datetime import datetime

now = datetime.now()
conn = redis.Redis()

i = 0

data = {"Coa": str(i), "Naam": "Test"+str(i),
        "commandantnaam": "Pieter", "starttijd": str(now)}

for i in range(0, 10):
    conn.hmset("coa"+str(i), data)
