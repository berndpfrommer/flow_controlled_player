#!/usr/bin/env python
#
# play rosbag in a flow controlled way
#
# example:
# ./player.py --ack_subject /vio/odom --ack_type nav_msgs.msg.Odometry --buffer_time 0.5 foo.bag

import rosbag, rospy
import argparse
import importlib
import threading
from rosgraph_msgs.msg import Clock
from std_msgs.msg import Header
import time

class Player():
    def __init__(self, args):
        self.args = args
        self.bag  = rosbag.Bag(args.bagfile, 'r')
        self.ack_time = None
        self.publishers = {}
        info = self.bag.get_type_and_topic_info()
        s = self.args.ack_type.rsplit('.',1)
        module = importlib.import_module(s[0])
        cl = getattr(module, s[1])
        self.sub = rospy.Subscriber(args.ack_subject, cl, self.callback)
        self.cv = threading.Condition()
        self.clock_pub = rospy.Publisher("/clock", Clock, queue_size=1)
        self.buffer_time = rospy.Duration(float(args.buffer_time))
        self.wait_timeout = float(args.timeout)
        self.last_ack_wall_time = time.time()

    def callback(self, msg):
        if hasattr(msg, 'header') and type(msg.header) == Header:
            with self.cv: # takes lock,
                self.ack_time = msg.header.stamp
                self.last_ack_wall_time = time.time()
                #print 'got ack: ', self.ack_time
                self.cv.notifyAll()

    def make_pub(self, topic, msg):
        return (rospy.Publisher(topic, msg, queue_size=args.out_queue_size))

    def get_pub(self, topic, msg):
        if not msg._type in self.publishers:
            self.publishers[msg._type] = {}
        if not topic in self.publishers[msg._type]:
            self.publishers[msg._type][topic] = self.make_pub(topic, type(msg))
        return self.publishers[msg._type][topic]

    def wait_for_ack(self, msg):
        if not hasattr(msg, 'header') or not hasattr(msg.header, 'stamp'):
            return
        if self.ack_time == None:
            self.ack_time = msg.header.stamp
            return
        t = msg.header.stamp
        with self.cv: # 'with' takes the lock
            while self.ack_time < t - self.buffer_time:
#                print 'ack wait: ', (t - self.ack_time).to_sec()
                self.cv.wait(self.wait_timeout)
                t_now = time.time()
                if t_now - self.last_ack_wall_time > self.wait_timeout:
                    print 'WARN: timeout expired at time ', t, t_now - self.last_ack_wall_time, self.wait_timeout
                    self.last_ack_wall_time = t_now
                    self.ack_time = t
                    break
                if rospy.is_shutdown():
                    break

    def publish_clock(self, t):
        clock = Clock()
        clock.clock = t
        self.clock_pub.publish(clock)

    def start(self):
        self.th = threading.Thread(target=self.play)
        self.th.start()

    def print_stats(self, t_start, t_end, t0, next_print_time, num_prints):
        total_time = (t_end - t_start).to_sec()
        time_played = (next_print_time - t_start).to_sec()
        wall_time = time.time() - t0
        print("completed: %5.2f%% speedup: %6.2f%%, remaining: %5.2fs"\
              % (time_played / total_time * 100,
                 time_played / wall_time * 100,
                 (t_end-next_print_time).to_sec() * wall_time / time_played))
        return next_print_time + rospy.Duration(total_time / num_prints)
    
    def play(self):
        t_start = rospy.Time(self.bag.get_start_time())
        t_end   = rospy.Time(self.bag.get_end_time())
        total_time = (t_end - t_start).to_sec()
        num_prints = 20
        next_print_time = t_start + rospy.Duration(total_time / num_prints)

        t0 = time.time()
        iterator = self.bag.read_messages(
            start_time=rospy.Time(self.args.start),
            end_time=rospy.Time(self.args.end))
        self.last_ack_wall_time = time.time()
        for (topic, msg, t) in iterator:
            self.publish_clock(t)
            pub = self.get_pub(topic, msg)
            if pub.get_num_connections() > 0:
                pub.publish(msg)
            self.wait_for_ack(msg)
            if t > next_print_time:
                next_print_time = self.print_stats(t_start, t_end, t0,
                                                   next_print_time, num_prints)
            if rospy.is_shutdown():
                break

        print "playback complete!"
        

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='play bag with flow control.')
    parser.add_argument('--name', '-n', action='store', default="player",
                        help= 'name of ros node')
    parser.add_argument('--start', '-s', action='store', default=0.0,
                        type=float, help= 'start time (large number!)')
    parser.add_argument('--end', '-e', action='store', default=1e60,
                        type=float, help='end time (large number!)')
    parser.add_argument('--ack_subject', '-a', action='store',
                        required=True, help='subject with ack.')
    parser.add_argument('--ack_type',  action='store',
                        required=True, help='type of ack.')
    parser.add_argument('--buffer_time',  action='store',
                        default=0.1, help='time[sec] to buffer.')
    parser.add_argument('--timeout',  action='store',
                        default=2.0, help='timeout [sec] wait for ack.')
    parser.add_argument('--out_queue_size', '-q', action='store',
                        default=100000, help='outgoing queue size.')
    parser.add_argument('bagfile')

    args = parser.parse_args()

    rospy.init_node(args.name)
    player = Player(args)
    player.start()
    rospy.spin()
