# rosbag player with flow control

Play rosbag with flow control. The following example will play all
topics from 'foo.bag', but will wait if received messages on the 'ack'
topic have fallen behind by a ros time of more than 0.5sec (buffer
time). If no ack is received for more than 2.0s, the buffer time is
reset, and playback is continued.

    ./player.py --ack_subject /vio/odom --ack_type nav_msgs.msg.Odometry --buffer_time 0.5 --timeout 2.0 foo.bag


