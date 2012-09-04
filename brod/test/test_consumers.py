import logging
import time
import unittest
from random import random
from brod.zk import ZKConsumer, ZKProducer

class TestTwoConsumers(unittest.TestCase):
    def setUp(self):
        # create a new topic with two partitions
        # create a producer which will fill the partitions with dummy messages
        self.zk_conn_str = 'localhost:2181'
        self.topic = 'test_multi_consumer2'
        #self.producer = ZKProducer(self.zk_conn_str, self.topic)
        #messages = map(hex,range(9))
        #self.producer.send(messages)
        
        # create one consumer and connect it to the topic
        name = 'both_same_name' #repr(random())[2:]
        self.consumerBill = ZKConsumer(self.zk_conn_str, name, self.topic, max_size=20*300*1024)
        
        for msg_set in self.consumerBill.poll(poll_interval=1):
            for offset, msg in msg_set:
                print msg
        
    def test_connect_second_consumer(self):
        # create a second consumer and connect it to the topic
        # assert that it got a partition
        # trigger a rebalance
        # after 60 seconds assert that both consumers are still online.
        pass
        
 

if __name__ == '__main__':
    logging.basicConfig(
        format='%(asctime)s,%(msecs)03d %(levelname)-5.5s [%(name)s] %(filename)s:%(lineno)s %(message)s',
        level=logging.DEBUG
    )
    unittest.main()
