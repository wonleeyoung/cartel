import socket
import threading
import time
import random
import json

class RaftNode:
    def __init__(self, id, peers):
        self.id = id  # 노드 ID
        self.peers = peers  # 다른 노드들의 주소 목록 (IP, 포트)
        self.state = "follower"  # 초기 상태는 팔로워
        self.term = 0  # 현재 임기
        self.voted_for = None  # 해당 임기 동안 투표한 후보
        self.log = []  # 로그 목록  [(term, data), ...] 이런식으로 저장
        self.commit_index = 0  # 커밋된 로그의 인덱스
        self.vote_count = 0  # 받은 투표 수

        # 임의의 시간으로 설정된 선거 타이머
        self.election_timeout = random.uniform(3, 6)

    # 리더가 되었을 때 실행
    def become_leader(self):
        self.state = "leader"
        self.vote_count = 0  # 리더가 되면 투표 카운트 초기화
        print(f"Node {self.id} became leader in term {self.term}")
        # 리더가 되면 주기적으로 하트비트 메시지를 보냄
        threading.Thread(target=self.send_heartbeats).start()

    # 팔로워로 돌아갈 때 실행
    def become_follower(self, term):
        self.state = "follower"
        self.term = term
        self.voted_for = None
        self.vote_count = 0
        print(f"Node {self.id} became follower in term {self.term}")

    # 후보자가 되었을 때 실행
    def become_candidate(self):
        self.state = "candidate"
        self.term += 1  # 새로운 임기로 전환
        self.voted_for = self.id  # 자신에게 투표
        self.vote_count = 1  # 자신에게 투표한 것으로 설정
        print(f"Node {self.id} became candidate in term {self.term}")
        self.start_election()  # 선거 시작

    # 선거 시작 - 다른 노드에 투표 요청
    def start_election(self):
        for peer in self.peers:
            threading.Thread(target=self.request_vote, args=(peer,)).start()

    # 다른 노드에 투표 요청 메시지 전송
    def request_vote(self, peer):
        message = {
            "type": "RequestVote",
            "term": self.term,
            "candidate_id": self.id,
            "last_log_index": len(self.log) - 1,
            "last_log_term": self.log[-1][0] if self.log else 0,
        }
        self.send_message(peer, message)

    # 하트비트를 주기적으로 보냄 (리더일 때만)
    def send_heartbeats(self):
        while self.state == "leader":
            for peer in self.peers:
                message = {
                    "type": "AppendEntries",
                    "term": self.term,
                    "leader_id": self.id,
                    "log": self.log,
                }
                self.send_message(peer, message)
            time.sleep(1)  # 1초마다 하트비트 전송

    # 소켓을 통해 메시지를 전송
    def send_message(self, peer, message):
        peer_host, peer_port = peer
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((peer_host, peer_port))
                s.sendall(json.dumps(message).encode())
                print(f"Node {self.id} sent message to {peer_host}:{peer_port} -> {message['type']}")
        except Exception as e:
            print(f"Node {self.id} failed to connect to {peer_host}:{peer_port} -> {str(e)}")

    # 소켓 서버에서 받은 메시지 처리
    def handle_message(self, conn, addr):
        data = conn.recv(1024).decode()
        message = json.loads(data)

        if message['type'] == "RequestVote":
            self.handle_request_vote(message, addr)
        elif message['type'] == "AppendEntries":
            self.handle_append_entries(message)
        elif message['type'] == "VoteResponse":  # 투표 응답 처리
            self.handle_vote_response(message)

    # RequestVote 메시지를 처리 (투표 요청)
    def handle_request_vote(self, message, addr):
        my_last_log_index = len(self.log) - 1
        my_last_log_term = self.log[-1][0] if self.log else 0
        
        # 투표 요청 수락할 때
        if message['term'] > self.term or (message['term'] == self.term and (message['last_log_term'] > my_last_log_term or message['last_log_index'] >= my_last_log_index)):
            self.become_follower(message['term'])
            self.voted_for = message['candidate_id']
            response = {"term": self.term, "vote_granted": True}
        # 투표 요청 거절할 때
        else:
            response = {"term": self.term, "vote_granted": False}
        self.send_message(addr, response)

    # AppendEntries 메시지를 처리 (하트비트 또는 로그 복제)
    def handle_append_entries(self, message):
        if message['term'] >= self.term:
            self.become_follower(message['term'])
            print(f"Node {self.id} received heartbeat from leader {message['leader_id']}")
        else:
            print(f"Node {self.id} rejected heartbeat from leader due to outdated term")

    # 투표 요청에 대한 응답을 처리하는 함수
    def handle_vote_response(self, message):
        if message['term'] == self.term and message['vote_granted']:
            self.vote_count += 1
            # 과반수 이상의 투표를 얻으면 리더로 전환
            if self.vote_count > len(self.peers) // 2:
                self.become_leader()

# 서버로서 소켓을 열어 다른 노드들의 연결을 대기
def start_server(raft_node, host="localhost", port=5000):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((host, port + raft_node.id))
    server_socket.listen(5)
    print(f"Node {raft_node.id} listening on {host}:{port + raft_node.id}")

    while True:
        conn, addr = server_socket.accept()
        threading.Thread(target=raft_node.handle_message, args=(conn, addr)).start()

# 선거 타임아웃을 설정하고, 리더가 없으면 선거 시작
def election_timeout(raft_node):
    while True:
        time.sleep(raft_node.election_timeout)
        if raft_node.state == "follower":
            raft_node.become_candidate()

# 노드를 실행하는 함수
def run_node(raft_node):
    # 서버 역할을 하는 스레드 실행
    server_thread = threading.Thread(target=start_server, args=(raft_node,))
    server_thread.start()

    # 선거 타임아웃 스레드 실행
    election_thread = threading.Thread(target=election_timeout, args=(raft_node,))
    election_thread.start()

# 메인 실행 함수
if __name__ == '__main__':
    # 각 노드의 주소 (호스트, 포트)
    peers = [("localhost", 5001), ("localhost", 5002), ("localhost", 5003)]

    # 노드 생성
    raft_node = RaftNode(id=1, peers=peers)  # 노드 1 생성
    run_node(raft_node)  # 노드 실행
