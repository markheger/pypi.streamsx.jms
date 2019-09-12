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
        txtmsg_schema = StreamSchema('tuple<rstring msg>')
        path_to_connection_doc = os.getcwd() + "/streamsx/jms/tests/connectionDocument.xml"     # tests are supposed to be run from the package directory
        topo = Topology('test_buildonly_consume')
        txtmsg_stream = jms.consume(topo, schemas=txtmsg_schema, connection="localActiveMQ", access="accessToTextMessages", connection_document=path_to_connection_doc, name="JMS_Consumer")
        #txtmsg_stream.print()
        self._build_only('test_buildonly_consume', topo)



    def test_buildonly_produce(self):
        txtmsg_schema = StreamSchema('tuple<rstring msg>')
        errmsg_schema = StreamSchema('tuple<rstring errorMessage>')
        path_to_connection_doc = os.getcwd() + "/streamsx/jms/tests/connectionDocument.xml"     # tests are supposed to be run from the package directory
        topo = Topology('test_buildonly_produce')
        txtmsg_stream = op.Source(topo, 'spl.utility::Beacon', txtmsg_schema, params = {'period':0.3})
        txtmsg_stream.msg = txtmsg_stream.output('"Message #" + (rstring)IterationCount()')
#        jms.produce(stream=txtmsg_stream, schema=None, connection="localActiveMQ", access="accessToTextMessages", connection_document=path_to_connection_doc, name="JMS_Producer")
        self._build_only('test_buildonly_produce', topo)
