from fastapi import FastAPI
import time 
import asyncio
import random 
from contextlib import asynccontextmanager
import httpx

data_store={}
log=[]
# leader election 
current_term=1 # current election number 
current_leader = None
state="follower" # follower / candidate/ leader 
voted_for=None 
commit_index = 0
election_timeout = random.uniform(3, 5)
last_heartbeat = time.monotonic()

node1_url="http://localhost:8001"
node3_url="http://localhost:8003"
@asynccontextmanager
async def lifespan(app: FastAPI):

    election_task = asyncio.create_task(election_monitor())

    yield

    election_task.cancel()

app = FastAPI(title="Node 2", lifespan=lifespan)
@app.get("/")
def root():
    return {
        "node": "node-2",
        "message": "Hello from Node 2"
    }

# this is follower 
@app.post("/replicate/{key}")
def replicate_data(key: str, value: str):

    data_store[key] = value

    return {
        "node": "node-2",
        "status": "replicated",
        "key": key,
        "value": value
    }

@app.get('/data/{key}')
def get_data(key:str):
    value=data_store.get(key)
    return{
        "node":"node2",
        "key":key,
        "value":value
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
            "sucess":False,
            "term":"Old term",
            "current_term":current_term
        }
     if term >current_term:
        current_term = term
        current_leader = leader
     current_leader=leader
     state="follower"
     last_heartbeat = time.monotonic()
    
     return{
        "node":"node2",
        "status": "heartbeat_received",
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
            "vote_granted":True,
            "current_term":current_term
        }
    return {
        "vote_granted": False,
        "reason": "already voted",
        "current_term": current_term
    }



def apply_committed_entries():
    global commit_index

    for entry in log:
        if entry["index"] <= commit_index:
            data_store[entry["key"]] = entry["value"]



@app.post('/append-entry')
def append_entry(term:int,index:int,prev_index:int,prev_term:int, key:str, value:str, leader_commit:int,leader:str):
    global current_term
    global log
    global commit_index
    global state
    global current_leader
    global voted_for
    if term < current_term:
        return{
            "success":False,
            "reason":"old term"
        }
    if term > current_term:
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
        "node": "node-2",
        "entry": entry
    }




async def start_election():
    global current_term 
    global state 
    global voted_for 
    global current_leader

    # become candidate 
    state="candidate"
    current_term+=1
    voted_for='node2'
    current_leader=None 

    votes=1

    print(f"Node 2 starting election for term {current_term}")

    async with httpx.AsyncClient() as client:
        for node_url in [node1_url, node3_url]:
            try:
                response=await client.post(
                    f"{node_url}/request-vote",
                    params={
                        "term":current_term,
                        "candidate_id":"node2",
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
                    print(f"Node2 receive the vote {votes}")

            except httpx.RequestError:
                print(f"Node 2 unable to reach to node {node_url}")


        # election result 
        if votes>=2:
            state="leader"
            current_leader="node2"
            print("node 2 is now the leader")

        else:
            state="follower"
            print("Node2 election failed ")


# background election monitor 
async def election_monitor():
    global election_timeout
    global state
    global last_heartbeat

    while True:
        await asyncio.sleep(1)
        now = time.monotonic()
        elapsed=now-last_heartbeat
        if elapsed > election_timeout:
            print("election timeout reached")
            await start_election()
            


# sending heartbeat to the followers 
async def send_heartbeats():
    global state
    while True:
            if state =="leader":
            
                async with httpx.AsyncClient() as client:
                    for node_url in [node1_url, node3_url]:
                        try:
                            url=f"{node_url}/heartbeat"
                            params={
                                "leader":"node-2",
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

