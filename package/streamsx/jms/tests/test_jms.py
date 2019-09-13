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
        errmsg_schema = StreamSchema('tuple<rstring errorMessage>')
        path_to_connection_doc = os.getcwd() + "/streamsx/jms/tests/connectionDocument.xml"     # tests are supposed to be run from the package directory
        topo = Topology('test_buildonly_consume')
        outputs = jms.consume(topo, schemas=[txtmsg_schema,errmsg_schema], connection="localActiveMQ", access="accessToTextMessages", connection_document=path_to_connection_doc, name="JMS_Consumer")
        txtmsg_stream = outputs[0]
        txtmsg_stream.print()
        self._build_only('test_buildonly_consume', topo)



    def test_buildonly_produce(self):
        txtmsg_schema = StreamSchema('tuple<rstring msg>')
        errmsg_schema = StreamSchema('tuple<rstring errorMessage>')
        path_to_connection_doc = os.getcwd() + "/streamsx/jms/tests/connectionDocument.xml"     # tests are supposed to be run from the package directory
        topo = Topology('test_buildonly_produce')
        txtmsg_source = op.Source(topo, 'spl.utility::Beacon', txtmsg_schema, params = {'period':0.3}, name="DataGenerator")
        txtmsg_source.msg = txtmsg_source.output('"Message #" + (rstring)IterationCount()')
        txtmsg_stream = txtmsg_source.stream
        txtmsg_stream.print()
        errmsg_stream = jms.produce(stream=txtmsg_stream, schema=errmsg_schema, connection="localActiveMQ", access="accessToTextMessages", connection_document=path_to_connection_doc, name="JMS_Producer")
        errmsg_stream.print()
        self._build_only('test_buildonly_produce', topo)
