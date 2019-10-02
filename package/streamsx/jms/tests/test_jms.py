import filecmp
import os

import streamsx.jms as jms
import streamsx.spl.toolkit as toolkit

from streamsx.spl.op import Source, Sink
from streamsx.topology.context import submit, ConfigParams
from streamsx.topology.schema import CommonSchema, StreamSchema
from streamsx.topology.tester import Tester
from streamsx.topology.topology import Topology

from unittest import TestCase




##
## Test assumptions
##
## Streaming analytics service running
##

class JmsBuildOnlyTest(TestCase):

    def _build_only(self, name, topo):
        result = submit("TOOLKIT", topo.graph) # creates tk* directory
        print(name + ' (TOOLKIT):' + str(result))
        assert(result.return_code == 0)
        result = submit("BUNDLE", topo.graph)  # creates sab file
        print(name + ' (BUNDLE):' + str(result))
        assert(result.return_code == 0)



    def test_buildonly_consume(self):
        txtmsg_schema = StreamSchema('tuple<rstring msg>')
        errmsg_schema = StreamSchema('tuple<rstring errorMessage>')

        java_class_lib_paths = []
        java_class_lib_paths.append("./streamsx/jms/tests/libs/activemq/lib")
        java_class_lib_paths.append("./streamsx/jms/tests/libs/activemq/lib/optional")

        path_to_connection_doc = "./streamsx/jms/tests/connectionDocument.xml"     # tests are supposed to be run from the package directory

        topo = Topology('buildonly_consume')
        toolkit.add_toolkit(topo, "../../streamsx.jms/com.ibm.streamsx.jms")
        outputs = jms.consume(topo, schemas=[txtmsg_schema,errmsg_schema], java_class_libs=java_class_lib_paths, connection="localActiveMQ", access="accessToTextMessages", connection_document=path_to_connection_doc, name="JMS_Consumer")
        txtmsg_stream = outputs[0]
        txtmsg_stream.print()
        self._build_only('test_buildonly_consume', topo)



    def test_buildonly_produce(self):
        txtmsg_schema = StreamSchema('tuple<rstring msg>')
        errmsg_schema = StreamSchema('tuple<rstring errorMessage>')

        java_class_lib_paths = []
        java_class_lib_paths.append("./streamsx/jms/tests/libs/activemq/lib")
        java_class_lib_paths.append("./streamsx/jms/tests/libs/activemq/lib/optional")

        path_to_connection_doc = "./streamsx/jms/tests/connectionDocument.xml"     # tests are supposed to be run from the package directory
        
        topo = Topology('buildonly_produce')
        toolkit.add_toolkit(topo, "../../streamsx.jms/com.ibm.streamsx.jms")
        txtmsg_source = Source(topo, 'spl.utility::Beacon', txtmsg_schema, params = {'period':0.3}, name="DataGenerator")
        txtmsg_source.msg = txtmsg_source.output('"Message #" + (rstring)IterationCount()')
        txtmsg_stream = txtmsg_source.stream
        txtmsg_stream.print()
        errmsg_stream = jms.produce(stream=txtmsg_stream, schema=errmsg_schema, java_class_libs=java_class_lib_paths, connection="localActiveMQ", access="accessToTextMessages", connection_document=path_to_connection_doc, name="JMS_Producer")
        errmsg_stream.print()
        self._build_only('test_buildonly_produce', topo)



class TopologyProvider(object):

    @staticmethod
    def text_message_class_topology():
        txtmsg_schema = StreamSchema('tuple<rstring sent_msg>')
        received_txtmsg_schema = StreamSchema('tuple<rstring received_msg>')
        errmsg_schema = StreamSchema('tuple<rstring errorMessage>')


        java_class_lib_paths = []
        java_class_lib_paths.append("./streamsx/jms/tests/libs/activemq/lib")
        java_class_lib_paths.append("./streamsx/jms/tests/libs/activemq/lib/optional")

        path_to_connection_doc = "./streamsx/jms/tests/connectionDocument.xml"

        topo = Topology('testtopo_text_message_class')

        toolkit.add_toolkit(topo, "../../streamsx.jms/com.ibm.streamsx.jms")

        txtmsg_source = Source(topo, 'spl.utility::Beacon', txtmsg_schema, params={'period':0.3, 'iterations':15}, name='DataGenerator')
        txtmsg_source.sent_msg = txtmsg_source.output('"My message #" + (rstring)IterationCount()')
        txtmsg_stream = txtmsg_source.stream
        #txtmsg_stream.print()
        #Sink('spl.adapter::FileSink', txtmsg_stream, params={'file':'expected.txt', 'format':'txt', 'flush':1}, name='TestDataDumper')
        Sink('spl.adapter::FileSink', txtmsg_stream, params={'file':'/tmp/expected.txt'}, name='TestDataDumper')

        errmsg_stream = jms.produce(stream=txtmsg_stream,
                                    schema=errmsg_schema,
                                    java_class_libs=java_class_lib_paths,
                                    connection="localActiveMQ",
                                    access="accessToSentTextMessages",
                                    connection_document=path_to_connection_doc,
                                    name="JMS_Producer")
        #errmsg_stream.print()
        Sink('spl.adapter::FileSink', errmsg_stream, params={'file':'/tmp/errors.txt'}, name='ErrorMsgDumper')

        outputs = jms.consume(topo, schemas=[received_txtmsg_schema,errmsg_schema],
                                    java_class_libs=java_class_lib_paths,
                                    connection="localActiveMQ",
                                    access="accessToReceivedTextMessages",
                                    connection_document=path_to_connection_doc,
                                    name="JMS_Consumer")
        received_txtmsg_stream = outputs[0]
        #received_txtmsg_stream.print()
        Sink('spl.adapter::FileSink', received_txtmsg_stream, params={'file':'/tmp/actual.txt'}, name='ReceivedDataDumper')

        return topo


    @staticmethod
    def map_message_class_topology():
        mapmsg_schema = StreamSchema('tuple<uint64 seqID, rstring msg>')
        errmsg_schema = StreamSchema('tuple<rstring errorMessage>')


        java_class_lib_paths = []
        java_class_lib_paths.append("./streamsx/jms/tests/libs/activemq/lib")
        java_class_lib_paths.append("./streamsx/jms/tests/libs/activemq/lib/optional")

        path_to_connection_doc = "./streamsx/jms/tests/connectionDocument.xml"

        topo = Topology('testtopo_map_message_class')

        toolkit.add_toolkit(topo, "../../streamsx.jms/com.ibm.streamsx.jms")

        mapmsg_source = Source(topo, 'spl.utility::Beacon', mapmsg_schema, params={'period':0.3, 'iterations':15}, name='DataGenerator')
        mapmsg_source.seqID = mapmsg_source.output('IterationCount()')
        mapmsg_source.msg = mapmsg_source.output('"My message #" + (rstring)IterationCount()')
        mapmsg_stream = mapmsg_source.stream
        #mapmsg_stream.print()
        #Sink('spl.adapter::FileSink', mapmsg_stream, params={'file':'expected.txt', 'format':'txt', 'flush':1}, name='TestDataDumper')
        Sink('spl.adapter::FileSink', mapmsg_stream, params={'file':'/tmp/expected.txt'}, name='TestDataDumper')

        errmsg_stream = jms.produce(stream=mapmsg_stream,
                                    schema=errmsg_schema,
                                    java_class_libs=java_class_lib_paths,
                                    connection="localActiveMQ",
                                    access="accessToMapMessages",
                                    connection_document=path_to_connection_doc,
                                    name="JMS_Producer")
        #errmsg_stream.print()
        Sink('spl.adapter::FileSink', errmsg_stream, params={'file':'/tmp/errors.txt'}, name='ErrorMsgDumper')

        outputs = jms.consume(topo, schemas=[mapmsg_schema,errmsg_schema],
                                    java_class_libs=java_class_lib_paths,
                                    connection="localActiveMQ",
                                    access="accessToMapMessages",
                                    connection_document=path_to_connection_doc,
                                    name="JMS_Consumer")
        received_mapmsg_stream = outputs[0]
        #received_mapmsg_stream.print()
        Sink('spl.adapter::FileSink', received_mapmsg_stream, params={'file':'/tmp/actual.txt'}, name='ReceivedDataDumper')

        return topo



class JmsTestDefinitions(TestCase):

    def test_text_message_class(self):
        topology = TopologyProvider.text_message_class_topology()
        self.tester = Tester(topology)
        self.tester.run_for(60)

        # Add the local check
        self.tester.local_check = self.local_checks

        # Run the test
        self.tester.test(self.test_ctxtype, self.test_config)

        self.assertTrue(filecmp.cmp('/tmp/expected.txt', '/tmp/actual.txt'))


    def test_map_message_class(self):
        topology = TopologyProvider.map_message_class_topology()
        self.tester = Tester(topology)
        self.tester.run_for(60)

        # Add the local check
        self.tester.local_check = self.local_checks

        # Run the test
        self.tester.test(self.test_ctxtype, self.test_config)

        self.assertTrue(filecmp.cmp('/tmp/expected.txt', '/tmp/actual.txt'))


    def local_checks(self):
        job = self.tester.submission_result.job
        self.assertEqual('healthy', job.health)



class JmsStandaloneTest(JmsTestDefinitions):

    def setUp(self):
        Tester.setup_standalone(self)



class JmsDistributedTest(JmsTestDefinitions):

    def setUp(self):
        Tester.setup_distributed(self)
        print("Disabling SSL verification.")
        self.test_config = {ConfigParams.SSL_VERIFY : False}
        print("For DISTRIBUTED context: remember to set STREAMS_USERNAME and STREAMS_PASSWORD environment variables.")




class JmsStreamingAnalyticsTest(JmsTestDefinitions):

    def setUp(self):
        Tester.setup_streaming_analytics(self)
