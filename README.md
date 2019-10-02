# Python streamsx.jms package

This exposes SPL operators in the `com.ibm.streamsx.jms` toolkit as Python methods.

Package is organized using standard packaging to upload to PyPi.

The package is uploaded to PyPi in the standard way:
```
cd package
python setup.py sdist bdist_wheel upload -r pypi
```
Note: This is done using the `ibmstreams` account at pypi.org and requires `.pypirc` file containing the credentials in your home directory.

Package details: https://pypi.python.org/pypi/streamsx.jms

Documentation is using Sphinx and can be built locally using:
```
cd package/docs
make html
```

or

    ant doc

and viewed using
```
firefox package/docs/build/html/index.html
```

The documentation is also setup at `readthedocs.io`.

Documentation links:
* http://streamsxjms.readthedocs.io

## Version update

To change the version information of the Python package, edit following files:

- ./package/docs/source/conf.py
- ./package/streamsx/jms/\_\_init\_\_.py

When the development status changes, edit the *classifiers* in

- ./package/setup.py

When the documented sample must be changed, change it here:

- ./package/streamsx/jms/\_\_init\_\_.py
- ./package/DESC.txt


## Test

When using local build (e.g. not forcing remote build), then you need to specifiy the toolkit location, for example:

    export STREAMSX_JMS_TOOLKIT=<PATH_TO_JMS_TOOLKIT>/com.ibm.streamsx.jms


### Build only test


```
cd package
python3 -u -m unittest streamsx.jms.tests.test_jms.JMSBuildOnlyTest
```



### Standalone test

Make sure that the streams environment is set and the environment variable:
STREAMS_INSTALL is setup.

Run the test with:

    ant test-standalone

or

```
cd package
python3 -u -m unittest streamsx.jms.tests.test_jms.JmsStandaloneTest
```



### Distributed test

Make sure that the streams environment is set and the environment variables:
STREAMS_INSTALL, STREAMS_DOMAIN_ID, and STREAMS_INSTANCE_ID are setup.

Run the test with:

    ant test

or

```
cd package
python3 -u -m unittest streamsx.jms.tests.test_jms.JmsDistributedTest
```



### Streaming Analytics service

Package can be tested with TopologyTester using the [Streaming Analytics](https://www.ibm.com/cloud/streaming-analytics) service.

Run the test with:

    ant test-sas

or

```
cd package
python3 -u -m unittest streamsx.jms.tests.test_jms.JmsStreamingAnalyticsTest
```

