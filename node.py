import requests
import json


class Node:
    def __init__(self,id):
        # Persistent state
        self.id=id
        self.currentTerm = 0
        self.votedFor = None
        self.log = []
        self.state="follower"
        # Volatile state
        self.commitIndex = 0
        self.lastApplied = 0

        # Volatile state on leaders
        self.nextIndex = {0:0,1:0,2:0,3:0,4:0}
        self.matchIndex = {0:0,1:0,2:0,3:0,4:0}



    def receive_append_entries(self, term, leader_id, prev_log_index, prev_log_term, entries, leader_commit):
        if term < self.currentTerm:
            return {"term":self.currentTerm,"success":False}

        if self.votedFor!=leader_id:
            return {"term":self.currentTerm,"success":False}
        
        if log[prev_log_index]['term'] != prev_log_term:
            return {"term":self.currentTerm,"success":False}

        i = prev_log_index + 1
        j = 0
        while i <= len(self.logs) and j < len(entries):
            if log[i]['term'] != entries[j]['term']:
                log = log[:i]
                break
            i += 1
            j += 1

        if j < len(entries):
            log.extend(entries[j:])

        if leader_commit > commit_index:
            commit_index = min(leader_commit, len(self.logs))

        return {"term":self.currentTerm,"success":True}
    

    def send_append_entries(self, server_id):
        if len(self.logs) == 0:
            prev_log_index = -1
            prev_log_term = -1
            entries = []
        else:
            prev_log_index = self.nextIndex[server_id] - 1
            prev_log_term = self.logs[prev_log_index]['term']
            entries = self.logs[self.nextIndex[server_id]:]

        success = False
        while not success:
            message = {
                'term': self.currentTerm,
                'leader_id': self.id,
                'prev_log_index': prev_log_index,
                'prev_log_term': prev_log_term,
                'entries': entries,
                'leader_commit': self.commitIndex,
            }
            response = self.send_rpc_append_entries(server_id,message)
            if response['term'] > self.currentTerm:
                self.currentTerm = response['term']
                self.state = 'follower'
                return
            success = response['success']
            if success:
                self.nextIndex[server_id] = prev_log_index + len(entries) + 1
                self.matchIndex[server_id] = prev_log_index + len(entries)
                for i in range(self.commitIndex+1, len(self.logs)):
                    if self.logs[i]['term'] == self.currentTerm and sum(1 for j in self.matchIndex.values() if j >= i) > len(self.matchIndex)//2:
                        self.commitIndex = i
            else:
                self.nextIndex[server_id] -= 1
                if self.nextIndex[server_id] < 0:
                    self.nextIndex[server_id] = 0
                    break

    
    def send_rpc_append_entries(id,message):
        headers = {"Content-Type": "application/json"}
        response = requests.post(endpoint, data=json.dumps(message), headers=headers)
        response.raise_for_status()
        return response.json()
    

    def send_vote(self, candidate_term, candidate_id, last_log_index, last_log_term):
        if candidate_term < self.currentTerm:
            return self.currentTerm, False
        
        if self.votedFor is not None and self.votedFor != candidate_id:
            return self.currentTerm, False
        if len(self.log)>=1:
            if self.logs[-1]['term'] > last_log_term:
                return self.currentTerm, False
            
            if self.logs[-1]['term'] == last_log_term and len(self.logs) > last_log_index:
                return self.currentTerm, False

        self.votedFor = candidate_id
        self.currentTerm = candidate_term
        
        return self.currentTerm, True
    



    def request_vote(self,nodes=[0,1,2,3,4]):
        self.currentTerm += 1
        self.state = 'candidate'
        vote_count = 1

        for node_id in nodes:
            if node_id == self.id:
                continue
            message={
                "term":self.currentTerm,
                "id": self.id,
                "last_log_index": len(self.log) - 1,
                "last_log_term": 0 if len(self.log)==0 else self.log[-1]['term']
            }
            rpc_result = self.send_rpc_request_vote(node_id,message)

            if rpc_result['term'] > self.currentTerm:
                self.currentTerm = rpc_result['term']
                self.state = 'follower'
                return

            if rpc_result['vote_granted']:
                vote_count += 1

        if vote_count > len(self.nodes) // 2:
            print("BECAME LEADER ")
            self.state = 'leader'
            self.leader_id = self.node_id
            self.nextIndex = [len(self.logs)] * len(self.nodes)
            self.matchIndex = [0] * len(self.nodes)
            self.send_rpc_append_entries()


    def send_rpc_request_vote(self,id,message):
        headers = {"Content-Type": "application/json"}
        response = requests.post(f"http://localhost:800{id}/askVote", data=json.dumps(message), headers=headers)
        response.raise_for_status()
        return response.json()  



        

    




    

    



    