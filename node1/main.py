from fastapi import FastAPI
import httpx
import asyncio
import random
from contextlib import asynccontextmanager

import time 
@asynccontextmanager
async def lifespan(app: FastAPI):

    election_task = asyncio.create_task(election_monitor())

    yield

    election_task.cancel()


app=FastAPI(title="Node 1", lifespan=lifespan)

data_store={}

# raft log 
log=[]
commit_index=0
election_timeout = random.uniform(5, 8)

# leader election 
current_term=1 # current election number 
current_leader=None
state="follower" # follower / candidate/ leader 
voted_for=None 

last_heartbeat = time.monotonic()

node2_url = "http://localhost:8002"
node3_url ="http://localhost:8003"

@app.get("/")
def root():
    return {
        "node": "node-1",
        "message": "Hello from Node 1"
    }
next_index = {
    "node2": 1,
    "node3": 1
}
# this node is a follower it does not accepts the write
@app.post('/replicate/{key}')
async def set_data(key:str, value:str):
    data_store[key]=value
    return{
        "node":"node 1",
        "status":"replicated",
        "key":key,
        "value":value
    }

async def replicate_to_node3(client):
    global next_index

    while next_index["node3"] <= len(log):
        index = next_index["node3"]
        entry = log[index - 1]

        if index == 1:
            prev_index = 0
            prev_term = 0
        else:
            prev_index = index - 1
            prev_term = log[index - 2]["term"]

        try:
            response = await client.post(
                f"{node3_url}/append-entry",
                params={
                    "term": current_term,
                    "index": index,
                    "prev_index": prev_index,
                    "prev_term": prev_term,
                    "key": entry["key"],
                    "value": entry["value"],
                    "leader_commit": commit_index,
                    "leader": "node1"
                },
                timeout=2.0
            )

            result = response.json()

            if result["success"]:
                print(f"Node 3 accepted log entry {index}")
                next_index["node3"] += 1
            else:
                # A higher term means Node 1 must step down.
                if result.get("reason") == "old_term":
                    print("Node 3 has a higher term. Stop replication.")
                    break

                print(
                    f"Node 3 rejected entry {index}: "
                    f"{result['reason']}"
                )

                next_index["node3"] = max(
                    1,
                    next_index["node3"] - 1
                )

        except httpx.RequestError:
            print("Node 3 unavailable")
            break

@app.get("/data/{key}")
def get_data(key: str):

    return {
        "node": "node-1",
        "key": key,
        "value": data_store.get(key)
    }




@app.post("/heartbeat")
def heartbeat(term:int, leader:str):
     global current_term
     global current_leader
     global state
     global voted_for
     global last_heartbeat

     if term < current_term:
        return {
            "success": False,
            "reason": "old_term",
            "current_term": current_term
        }
    
     if term > current_term:
        current_term = term
        current_leader = leader
     state="follower"
     voted_for=None
     last_heartbeat=time.monotonic()
      
     return {
        "success": True,
        "node": "node1",
        "state": state,
        "leader": current_leader,
        "term": current_term
    }



@app.post("/request-vote")
async def request_votes(term : int , candidate_id : str , last_log_index:int , last_log_term : int):
    global state
    global current_term
    global voted_for
    my_last_log_index=len(log)
    if log:
        my_last_log_term = log[-1]["term"]
    else:
         my_last_log_term =0

    candidate_log_is_up_to_date=(last_log_term > my_last_log_term or last_log_term == my_last_log_term
        and last_log_index >= my_last_log_index)

    if not candidate_log_is_up_to_date:
        return {
            "vote_granted": False,
            "reason": "candidate log is not up to date",
            "current_term": current_term
        }


    # reject candidate with older term
    if term < current_term:
        return {
            "vote_granted": False,
            "reason":"old term",
            "current_term":current_term
        }
    # if candidate has newer term so update our term and clear the previous vote 
    if term > current_term:
        current_term=term
        voted_for=None
        state="follower"

    # for now allow voting for once per time 
    if voted_for is None or voted_for==candidate_id:
        voted_for=candidate_id
        return {
            "voted_granted":True,
            "current_term":current_term
        }
    return {
        "vote_granted": False,
        "reason": "already voted",
        "current_term": current_term
    }



# background election monitor 
async def election_monitor():

    while True:
        await asyncio.sleep(1)
        now = time.monotonic()
        elapsed=now-last_heartbeat
        if elapsed > election_timeout:

            print("Election timeout reached")
            await start_election()
            





# sending heartbeat to the followers 
async def send_heartbeats():
    global state
    while True:
            if state =="leader":
            
                async with httpx.AsyncClient() as client:
                    for node_url in [node3_url, node2_url]:
                        try:
                            url=f"{node_url}/heartbeat"
                            params={
                                "leader":"node-1",
                                "term":current_term
                            }
                            response=await client.post(
                                url=url,
                                params=params,
                                timeout=1.0
                            )
                            print(
                            f"Heartbeat sent to {node_url}: "
                            f"{response.status_code}"
                        )
                        except httpx.RequestError:
                            print(f"Node unavailable: {node_url}")
                await asyncio.sleep(1)



@app.post('/append-entry')
def append_entry(term:int,index:int,prev_index:int,prev_term:int, key:str, value:str,leader_commit: int,leader:str):
    global current_term
    global log
    global commit_index
    global state
    global current_leader
    global voted_for
    if term < current_term:
        return{
            "success":False,
            "reason":"old term",
            "current_term":current_term
        }
    if term>current_term:
        current_term=term
        voted_for=None
    state="follower"
    current_leader=leader
        
   
    # check the previous log entry 
    if prev_index>0:
        # then the prev index log does not exists 
        if len(log) < prev_index:
            return {
                "success": False,
                "reason": "missing_previous_entry"
            }
        prev_entry=log[prev_index-1]
        # prev entry exists but term does not match 
        if prev_entry["term"] != prev_term:
            # Remove conflicting suffix
            log = log[:prev_index - 1]
            return {
                "success": False,
                "reason": "log_conflict",
                "conflict_index": prev_index
            }
        if len(log) >=index:
            existing_entry = log[index - 1]
            if existing_entry['term']!=term:
             # Remove conflicting entry and everything after it
                log = log[:index - 1]



    # appending the new entry 
    entry = {
        "term": term,
        "index": index,
        "prev_index": prev_index,
        "prev_term": prev_term,
        "key": key,
        "value": value
    }
    log.append(entry)
    if leader_commit>commit_index:
        commit_index = min(
            leader_commit,
            len(log)
        )
        apply_committed_entries()
    return {
        "success": True,
        "node": "node-1",
        "entry": entry,
        "commit_index": commit_index
    }


def apply_committed_entries():
    global commit_index

    for entry in log:
        if entry["index"] <= commit_index:
            data_store[entry["key"]] = entry["value"]


@app.post("/data/{key}")
async def set_data(key: str, value: str):

    global log
    global commit_index

    # Create new log entry
    if len(log) > 0:
        prev_term = log[-1]["term"]
    else:
        prev_term = 0

    entry = {
        "term": current_term,
        "index": len(log) + 1,
        "prev_index": len(log),
        "prev_term": prev_term,
        "key": key,
        "value": value
    }

    # Add entry to leader's log
    log.append(entry)

    successful_replicas = 1

    async with httpx.AsyncClient() as client:

        # Replicate to Node 2
        try:
            response = await client.post(
                f"{node2_url}/append-entry",
                params={
                    "term": entry["term"],
                    "index": entry["index"],
                    "prev_index": entry["prev_index"],
                    "prev_term": entry["prev_term"],
                    "key": entry["key"],
                    "value": entry["value"],
                    "leader_commit": commit_index
                },
                timeout=2.0
            )

            if response.status_code == 200:
                result = response.json()

                if result["success"]:
                    successful_replicas += 1

        except httpx.RequestError:
            print("Node 2 unavailable")


        # Replicate to Node 3
        old_next_index = next_index["node3"]

        await replicate_to_node3(client)

        if next_index["node3"] > old_next_index:
            successful_replicas += 1


    # Check majority
    if successful_replicas >= 2:

        commit_index = entry["index"]

        data_store[key] = value

        return {
            "status": "committed",
            "leader": "node-1",
            "term": current_term,
            "entry": entry,
            "successful_replicas": successful_replicas,
            "commit_index": commit_index
        }

    return {
        "status": "uncommitted",
        "leader": "node-1",
        "entry": entry,
        "successful_replicas": successful_replicas,
        "commit_index": commit_index
    }



async def start_election():
    global current_term 
    global state 
    global voted_for 
    global current_leader

    # become candidate 
    state="candidate"
    current_term+=1
    voted_for='node1'
    current_leader=None 

    votes=1

    print(f"Node 1 starting election for term {current_term}")

    async with httpx.AsyncClient() as client:
        for node_url in [node2_url, node3_url]:
            try:
                response=await client.post(
                    f"{node_url}/request-vote",
                    params={
                        "term":current_term,
                        "candidate_id":"node1",
                        "last_log_index":len(log),
                        "last_log_term": log[-1]["term"] if log else 0
                    },
                    timeout=2.0
                )
                result=response.json()

                # a higher term means we are outdated 
                if result["current_term"]> current_term:
                    current_term= result["current_term"]
                    state="follower"
                    voted_for=None
                    return

                if result["vote_granted"]:
                    votes+=1
                    print(f"Node1 receive the vote {votes}")

            except httpx.RequestError:
                print(f"Node 1 unable to reach to node {node_url}")


        # election result 
        if votes>=2:
            state="leader"
            current_leader="node1"
            print("node 1 is now the leader")

        else:
            state="follower"
            print("Node1 election failed ")
