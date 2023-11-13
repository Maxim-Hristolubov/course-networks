from typing import List, Set
import threading
import socket
import time
import sys
from collections import OrderedDict

SPLIT_SIZE = 32767

def int_to_bytes(n: int, size = 4) -> bytes:
    return n.to_bytes(size, byteorder='little', signed=False)

def bytes_to_int(b: bytes) -> int:
    return int.from_bytes(b, byteorder='little', signed=False)

class UDPBasedProtocol:
    def __init__(self, *, local_addr, remote_addr):
        self.udp_socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
        self.udp_socket.setblocking(0)
        self.remote_addr = remote_addr
        self.udp_socket.bind(local_addr)

    def sendto(self, data):
        return self.udp_socket.sendto(data, self.remote_addr)

    def recvfrom(self, n):
        msg, addr = self.udp_socket.recvfrom(n)
        return msg

class Packet:
    def __init__(self, data: bytes, id: int, ack: int, split = 0):
        self.data = data
        self.id = id
        self.ack = ack
        self.split = split

    def __eq__(self, other):
        return self.id == other.packet_id

    def __hash__(self):
        return hash(('id', self.id))

    def serialize(self) -> bytes:
        return int_to_bytes(self.id) + \
               int_to_bytes(self.ack) + \
               int_to_bytes(self.split, 1) + \
               self.data

    def is_empty(self) -> bool:
        return len(self.data) == 0

    @classmethod
    def load(cls, data: bytes):
        id = bytes_to_int(data[:4])
        ack = bytes_to_int(data[4:8])
        split = bytes_to_int(data[8:9])
        return cls(data[9:], id, ack, split)


class MyTCPProtocol(UDPBasedProtocol):
    def __init__(self, name = None, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.initialize_protocol_parameters(name)

        self.transmit_queue: List[Packet] = []
        self.receive_queue: List[Packet] = []

        self.transmitted_list: List[Packet] = []
        self.received_set: Set[int] = set()

        self.id = 1
        self.ack = 0
        self.last_ack = None

        self.TICK = 1 / 100_000_000

        self.time = 0
        self.last_time = 0

        self.hide_flag = False
        self.log_mode = False

        self.thread = threading.Thread(target=self.working_thread)
        self.thread.start()

    def initialize_protocol_parameters(self, name):
        self.buffer_size = 4096
        self.name = name

    def log(self, *values: object):
        if self.log_mode:
            print(self.name, *values)

    def __del__(self):
        self.hide_flag = True

    def recv(self, n: int):
        while len(self.receive_queue) == 0 or self.receive_queue[-1].split == 1:
            time.sleep(self.TICK)

        data = self.combine_split_sequence()
        return data

    def combine_split_sequence(self):
        if self.receive_queue[-1].split == 0:
            packet = self.receive_queue.pop()
            return packet.data

        data = b''
        while len(self.receive_queue) > 0 and self.receive_queue[-1].split != 0:
            packet = self.receive_queue.pop()
            data = packet.data + data

        return data

    def send(self, data: bytes):
        splits = len(data) // SPLIT_SIZE + 1

        for i in range(splits):
            packet_split = {1: 0, i + 1: 2}.get(splits, 1)
            data_slice = data[i * SPLIT_SIZE: (i + 1) * SPLIT_SIZE]
            packet = Packet(data_slice, self.id, self.ack, packet_split)
            self.log("Sending packet, id=", self.id, "len=", len(data_slice), "Split=", packet_split)
            self.transmit_queue.insert(0, packet)
            self.id += 1

        return len(data) + 9

    def process_received_packet(self) -> Packet:
        data = self.recvfrom(32767 + 9)
        packet = Packet.load(data)

        if packet.id not in self.received_set:
            self.handle_new_packet(packet)
        else:
            self.log("Got duplicate packet! id=", packet.id, packet.ack)

        self.last_ack = packet.ack
        return packet

    def handle_new_packet(self, packet: Packet):
        if packet.id - self.ack != 1:
            self.handle_packet_loss(packet)
        else:
            if packet.is_empty():
                self.handle_empty_packet(packet)
            else:
                self.handle_ordered_packet(packet)

    def handle_packet_loss(self, packet: Packet):
        self.log("Packet loss detected id=", packet.id, "ack=", self.ack)
        self.send_acknowledgement(loss=True)

    def handle_empty_packet(self, packet: Packet):
        self.log("Empty packet ack =", packet.ack, " split=", packet.split)
        if packet.split == 5:
            self.send_missing_packets(packet.ack)
        else:
            self.delete_acknowledged_packets(packet.ack)

    def handle_ordered_packet(self, packet: Packet):
        self.log("Packet in order: p.id=", packet.id, "p.ack=", packet.ack)
        self.received_set.add(packet.id)
        self.receive_queue.append(packet)
        self.ack = packet.id

    def working_thread(self):
        while not self.hide_flag:
            try:
                self.process_received_packet()
            except Exception as exc:
                self.log("Exception: ", exc)

            self.process_transmit_queue()
            self.update_time()

            time.sleep(self.TICK)

    def process_transmit_queue(self):
        if len(self.transmit_queue) > 0:
            self.send_next_transmitted_packet()

            if self.is_idle_for_long_time():
                self.send_acknowledgement()

    def send_next_transmitted_packet(self):
        packet = self.transmit_queue.pop()
        packet.ack = self.ack
        self.sendto(packet.serialize())
        self.transmitted_list.append(packet)
        self.last_time = self.time

    def is_idle_for_long_time(self):
        return self.time - self.last_time >= 100 * self.TICK

    def update_time(self):
        self.time += self.TICK

    def send_missing_packets(self, ack: int):
        id_to_send = sys.maxsize if not self.transmit_queue else self.transmit_queue[0].id

        missing_packets = self.find_missing_packets(ack, id_to_send)
        self.update_missing_packets_ack(missing_packets)
        self.add_missing_packets_to_queue(missing_packets)
        self.remove_duplicates_from_queue()

        self.log("Queue after: ", [p.id for p in self.transmit_queue])

    def find_missing_packets(self, ack: int, id_to_send: int):
        return list(filter(
            lambda packet: ack < packet.id < id_to_send and not packet.is_empty(),
            self.transmitted_list
        ))

    def update_missing_packets_ack(self, missing_packets):
        for p in missing_packets:
            p.ack = self.ack

    def add_missing_packets_to_queue(self, missing_packets):
        self.transmit_queue += missing_packets

    def remove_duplicates_from_queue(self):
        self.transmit_queue = list(OrderedDict.fromkeys(self.transmit_queue))

    def delete_acknowledged_packets(self, ack: int):
        self.transmit_queue = list(
            filter(lambda packet: packet.id > ack and not packet.is_empty(),
                   self.transmit_queue))

    def send_acknowledgement(self, loss=False):
        self.log("Sending empty packet ack=", self.ack, "id=", self.id)
        packet = Packet(b'', self.id, self.ack)
        packet.split = 5 if loss else 0
        self.transmit_queue.append(packet)