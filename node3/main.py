from fastapi import FastAPI
import httpx
import asyncio
import time
import random
from contextlib import asynccontextmanager


last_heartbeat=time.monotonic()
election_timeout=random.uniform(3,5)
@asynccontextmanager
async def lifespan(app: FastAPI):

    heartbeat_task = asyncio.create_task(
        send_heartbeats()
    )
    task = asyncio.create_task(election_monitor())
    yield
    heartbeat_task.cancel()
    task.cancel()


app=FastAPI(title='node3',lifespan=lifespan)
log=[]
commit_index = 0
prev_term=0
node1_url="http://localhost:8001"
node2_url="http://localhost:8002"
N=3
W=2
R=2

# raft variables 
# leader election 
current_term=1
# for testing node 3 becomes leader for the first time 
state="leader" # follower / candidate/ leader 
current_leader=None
voted_for=None

next_index = {
    "node1": 1,
    "node2": 1
}


data_store={}
@app.post("/data/{key}")
async def set_data(key:str, value:str):
    global log 
    global commit_index
    global next_index
    # creating the log entry 
    
    if len(log)>0:
        prev_term=log[-1]["term"]
    else:
         prev_term=0
    entry={
         "term":current_term,
         "index":len(log)+1,
         "prev_index": len(log),
         "key":key,
         "value":value,
         "prev_term":prev_term,
    }
    log.append(entry)
    
    successful_replicas=1
   
    async with httpx.AsyncClient() as client:
        # replicate to node 1 
        old_next_index=next_index["node1"]
        await replicate_to_node(client, node1_url,"node1" )
        if next_index['node1']>old_next_index:
             successful_replicas+=1


        # replicate to node 2 
        old_next_index=next_index["node2"]
        await replicate_to_node(client, node2_url, "node2")

        if next_index["node2"]>old_next_index:
             successful_replicas+=1
        
    # write quoram 
    if successful_replicas>=W:
          commit_index = entry["index"]
          apply_committed_entries()
          return {
            "status": "committed",
            "entry": entry,
            "successful_replicas": successful_replicas,
            "commit_index": commit_index
        }

    return {
        "status": "uncommitted",
        "entry": entry,
        "successful_replicas": successful_replicas,
        "commit_index": commit_index
    }


@app.get('/data/{key}')
async def get_data(key:str):
    
    values=[]
    async with httpx.AsyncClient() as client:
            # read from node 1
            values.append(
                  {
                        "node":"node1",
                        "value":data_store.get(key)
                  }
            )

            # Read from Node 1
            try:
                response2 = await client.get(
                    f"{node1_url}/data/{key}",
                    timeout=2.0
                )

                if response2.status_code == 200:
                    values.append(response2.json())

            except httpx.RequestError:
                pass

            # Read from Node 3
            try:
                response3 = await client.get(
                    f"{node2_url}/data/{key}",
                    timeout=2.0
                )

                if response3.status_code == 200:
                    values.append(response3.json())

            except httpx.RequestError:
                pass
    if len(values) < R:
        return {
            "status": "rejected",
            "reason": "Read quorum not reached",
            "responses": len(values),
            "required": R
        }
    value_counts = {}
    for item in values:
        value = item["value"]
        value_counts[value] = value_counts.get(value, 0) + 1

    latest_value = max(
        value_counts,
        key=value_counts.get
    )

    return {
        "status": "success",
        "value": latest_value,
        "responses": len(values),
        "read_quorum": R
    }



# sending heartbeat to the followers 
async def send_heartbeats():
    global state
    while True:
            if state =="leader":
            
                async with httpx.AsyncClient() as client:
                    for node_url in [node1_url, node2_url]:
                        try:
                            url=f"{node_url}/heartbeat"
                            params={
                                "leader":"node-3",
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

# fastapi lifespan is used to automatically start the function 
# next index - leader needs to remember which log entry should i send to each follower
async def replicate_to_node(
    client,
    node_url,
    node_name
):
    global next_index

    while next_index[node_name] <= len(log):

        index = next_index[node_name]

        entry = log[index - 1]

        if index == 1:
            prev_index = 0
            prev_term = 0
        else:
            prev_index = index - 1
            prev_term = log[index - 2]["term"]

        try:

            response = await client.post(
                f"{node_url}/append-entry",
                params={
                    "term": entry["term"],
                    "index": entry["index"],
                    "prev_index": prev_index,
                    "prev_term": prev_term,
                    "key": entry["key"],
                    "value": entry["value"],
                    "leader_commit": commit_index,
                    "leader":"node3"
                },
                timeout=2.0
            )

            result = response.json()

            if result["success"]:

                print(
                    f"{node_name} accepted "
                    f"log entry {index}"
                )

                next_index[node_name] += 1

            else:

                print(
                    f"{node_name} rejected "
                    f"log entry {index}: "
                    f"{result['reason']}"
                )

                next_index[node_name] -= 1

                if next_index[node_name] < 1:
                    next_index[node_name] = 1

                print(
                    f"Retrying {node_name} "
                    f"from index {next_index[node_name]}"
                )

        except httpx.RequestError:

            print(f"{node_name} unavailable")
            break


def apply_committed_entries():
    global commit_index

    for entry in log:
        if entry["index"] <= commit_index:

            key = entry["key"]
            value = entry["value"]

            data_store[key] = value



# adding the heartbeat endpoint in node 3 as well beacuase if this node becomes follower then it should listen the heatbeat 
@app.post('/heartbeat')
def receive_heartbeat(term :int, leader:str):
    global current_term
    global current_leader
    global state
    global voted_for
    global last_heartbeat

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
    last_heartbeat = time.monotonic()
    return {
        "success": True,
        "node": "node3",
        "state": state,
        "leader": current_leader,
        "term": current_term
    }



@app.post('/append-entry')
def append_entry(term:int,index:int,prev_index:int,prev_term:int, key:str, value:str,leader_commit: int, leader:str):
    global current_term
    global log
    global commit_index
    global state
    global current_leader
    global voted_for 

    # reject request from the older term 

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
                "reason": "missing_previous_entry",
                "current_term":current_term
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
      # Append only if this entry is not already present
    if len(log) < index:
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
        "node": "node3",
        "state": state,
        "term": current_term,
        "leader": current_leader,
        "log_length": len(log),
        "commit_index": commit_index
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



async def start_election():
    global current_term 
    global state 
    global voted_for 
    global current_leader

    # become candidate 
    state="candidate"
    current_term+=1
    voted_for='node3'
    current_leader=None 

    votes=1

    print(f"Node 3 starting election for term {current_term}")

    async with httpx.AsyncClient() as client:
        for node_url in [node2_url, node1_url]:
            try:
                response=await client.post(
                    f"{node_url}/request-vote",
                    params={
                        "term":current_term,
                        "candidate_id":"node3",
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
                    print(f"Node3receive the vote {votes}")

            except httpx.RequestError:
                print(f"Node 3 unable to reach to node {node_url}")


        # election result 
        if votes>=2:
            state="leader"
            current_leader="node1"
            print("node 2 is now the leader")

        else:
            state="follower"
            print("Node2 election failed ")


async def election_monitor():
    global state
    global last_heartbeat
    global election_timeout

    while True :
        await asyncio.sleep(0.5)
        if state=="follower":
            elapsed=time.monotonic()-last_heartbeat

            if elapsed >= election_timeout:
                print("Election Timeout reached")
                await start_election()
                last_heartbeat = time.monotonic()
                election_timeout = random.uniform(3, 5)

