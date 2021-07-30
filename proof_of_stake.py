import time
import json
import threading
from hashlib import sha256
from datetime import datetime
from random import choice
from queue import Queue, Empty

from socketserver import BaseRequestHandler, ThreadingTCPServer

block_chain = []
temp_blocks = []
candidate_blocks = Queue()
announcements = Queue()
validators = {}

lock = threading.Lock()


def create_block(oldblock, bpm, address):
    newblock = {
        "Index": oldblock["Index"] + 1,
        "BPM": bpm,
        "Timestamp": str(datetime.now()),
        "PrevHash": oldblock["Hash"],
        "Validator": address
    }

    newblock["Hash"] = calculate_hash(newblock)
    return newblock


def calculate_hash(block):
    record = "".join([
        str(block["Index"]),
        str(block["BPM"]),
        block["Timestamp"],
        block["PrevHash"]
    ])

    return sha256(record.encode()).hexdigest()


def is_block_valid(newblock, oldblock):
    if oldblock["Index"] + 1 != newblock["Index"]:
        return False

    if oldblock["Hash"] != newblock["PrevHash"]:
        return False

    if calculate_hash(newblock) != newblock["Hash"]:
        return False

    return True


def pick_winner(announcements):
    """
    :param announcements:
    :return:
    """
    time.sleep(10)

    while True:
        with lock:
            temp = temp_blocks

        lottery_pool = []  #

        if temp:
            for block in temp:
                if block["Validator"] not in lottery_pool:
                    set_validators = validators
                    k = set_validators.get(block["Validator"])
                    if k:
                        for i in range(k):
                            lottery_pool.append(block["Validator"])

            lottery_winner = choice(lottery_pool)
            print(lottery_winner)
            # add block of winner to blockchain and let all the other nodes known
            for block in temp:
                if block["Validator"] == lottery_winner:
                    with lock:
                        block_chain.append(block)

                    # write message in queue.
                    msg = "\n{0} won the billing rights\n".format(lottery_winner)
                    announcements.put(msg)

                    break

        with lock:
            temp_blocks.clear()


class HandleConn(BaseRequestHandler):
    def handle(self):
        print("Got connection from", self.client_address)

        # validator address
        self.request.send(b"Enter token balance:")
        balance = self.request.recv(8192)
        try:
            balance = int(balance)
        except Exception as e:
            print(e)

        t = str(datetime.now())
        address = sha256(t.encode()).hexdigest()
        validators[address] = balance
        print("Validator = " + validators)

        while True:
            announce_winner_t = threading.Thread(target=annouce_winner, args=(announcements, self.request,),
                                                 daemon=True)
            announce_winner_t.start()

            bpm = self.request.recv(8192)
            try:
                bpm = int(bpm)
            except Exception as e:
                print(e)
                del validators[address]
                break

            # with lock:
            last_block = block_chain[-1]

            new_block = create_block(last_block, bpm, address)

            if is_block_valid(new_block, last_block):
                print("new block is valid!")
                candidate_blocks.put(new_block)

            self.request.send(b"\nEnter a new BPM:\n")

            annouce_blockchain_t = threading.Thread(target=annouce_blockchain, args=(self.request,), daemon=True)
            annouce_blockchain_t.start()


def annouce_winner(announcements, request):
    while True:
        try:
            msg = announcements.get(block=False)
            request.send(msg.encode())
            request.send(b'\n')
        except Empty:
            time.sleep(3)
            continue


def annouce_blockchain(request):
    while True:
        time.sleep(30)
        with lock:
            output = json.dumps(block_chain)
        try:
            request.send(output.encode())
            request.send(b'\n')
        except OSError:
            pass


def candidate(candidate_blocks):
    while True:
        try:
            candi = candidate_blocks.get(block=False)
        except Empty:
            time.sleep(5)
            continue
        temp_blocks.append(candi)


def run():
    t = str(datetime.now())
    first_block = {
        "Index": 0,
        "Timestamp": t,
        "BPM": 0,
        "PrevHash": "",
        "Validator": ""
    }

    first_block["Hash"] = calculate_hash(first_block)
    print(first_block)
    block_chain.append(first_block)

    thread_canditate = threading.Thread(target=candidate, args=(candidate_blocks,), daemon=True)
    thread_pick = threading.Thread(target=pick_winner, args=(announcements,), daemon=True)

    thread_canditate.start()
    thread_pick.start()

    # start a tcp server
    serv = ThreadingTCPServer(('', 9090), HandleConn, bind_and_activate=True)
    serv.serve_forever()


if __name__ == '__main__':
    run()