from unittest import TestCase

import streamsx.jms as jms

from streamsx.topology.topology import Topology
from streamsx.topology.tester import Tester
from streamsx.topology.schema import CommonSchema, StreamSchema
import streamsx.spl.op as op
import streamsx.spl.toolkit
import streamsx.rest as sr

import datetime
import os
import json

##
## Test assumptions
##
## Streaming analytics service running
##

class JMSBuildOnlyTest(TestCase):
    
    def _build_only(self, name, topo):
        result = streamsx.topology.context.submit("TOOLKIT", topo.graph) # creates tk* directory
        print(name + ' (TOOLKIT):' + str(result))
        assert(result.return_code == 0)
        result = streamsx.topology.context.submit("BUNDLE", topo.graph)  # creates sab file
        print(name + ' (BUNDLE):' + str(result))
        assert(result.return_code == 0)



    def test_buildonly_consume(self):
        txtMsgSchema = StreamSchema('tuple<rstring msg>')
        topo = Topology('test_buildonly_consume')
        jmsTxtMsgStream = jms.consume(topo, schemas=txtMsgSchema, connection="localActiveMQ", access="accessToTextMessages", connectionDocument="./connectionDocument.xml", name="JMS_Consumer")
        jmsTxtMsgStream.print()
        self._build_only('test_buildonly_consume', topo)



    def test_buildonly_produce(self):
        txtMsgSchema = StreamSchema('tuple<rstring msg>')
        topo = Topology('test_buildonly_produce')
        txtMsgStream = op.Source(topo, 'spl.utility::Beacon', txtMsgSchema, params = {'period':0.3})
        txtMsgStream.msg = txtMsgStream.output("Message #" + 'IterationCount()')
        jms.produce(txtMsgStream, schema=None, connection="localActiveMQ", access="accessToTextMessages", connectionDocument="./connectionDocument.xml", name="JMS_Producer")
        self._build_only('test_buildonly_produce', topo)

