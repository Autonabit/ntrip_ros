#!/usr/bin/env python

import rospy
import asyncio

from rtcm_msgs.msg import Message as RTCM_Message
from ntripstreams.ntripstreams import NtripStream

class NtripClient:
    def __init__(self):
        rospy.init_node('ntripclient', anonymous=True)

        self.rtcm_topic = rospy.get_param('~rtcm_topic', 'rtcm')
        self.ntrip_server = rospy.get_param('~ntrip_server')
        self.ntrip_user = rospy.get_param('~ntrip_user')
        self.ntrip_pass = rospy.get_param('~ntrip_pass')
        self.ntrip_stream = rospy.get_param('~ntrip_stream')


        if not self.ntrip_server.startswith("http"):
            self.ntrip_server = "http://"+self.ntrip_server

        self.pub = rospy.Publisher(self.rtcm_topic, RTCM_Message, queue_size=10)

    async def run(self):

        ntripstream = NtripStream()
        rtcm_msg = RTCM_Message()

        try:
            await ntripstream.requestNtripStream(
                self.ntrip_server,
                self.ntrip_stream,
                self.ntrip_user,
                self.ntrip_pass)

        except OSError as error:
            rospy.logerr(error)
            return
        while not rospy.is_shutdown():
            try:
                rtcmFrame, timeStamp = await asyncio.wait_for(ntripstream.getRtcmFrame(), timeout=10)
                fail = 0
            except (ConnectionError, IOError, asyncio.exceptions.TimeoutError):

                fail += 1
                sleepTime = 5 * fail
                if sleepTime > 300:
                    sleepTime = 300
                rospy.logerr(
                    f"{self.ntrip_stream}:{fail} failed attempts to reconnect. "
                    f"Will retry in {sleepTime} seconds!"
                )
                await asyncio.sleep(sleepTime)
                await ntripstream.requestNtripStream(
                        self.ntrip_server,
                        self.ntrip_stream,
                        self.ntrip_user,
                        self.ntrip_pass)
 
            else:
                rtcm_msg.message = rtcmFrame.bytes
                rtcm_msg.header.seq += 1
                rtcm_msg.header.stamp = rospy.get_rostime()
                self.pub.publish(rtcm_msg)


if __name__ == '__main__':
    c = NtripClient()
    asyncio.run(c.run())

