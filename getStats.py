#!/usr/bin/env python
#RUN COMMAND: thrift -r --gen py bookkeeper.thrift
import sys
sys.path.append('./gen-py')
import xml.etree.ElementTree as ET
import os

from bookkeeper import BookKeeperService
from bookkeeper.ttypes import *
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol


conf_file = "/usr/lib/hadoop2/etc/hadoop/core-site.xml"

name_value = {}
tree = ET.parse(conf_file)
root = tree.getroot()
properties = root.findall("property")
name_value = {}
for p in properties:
   name = p[0].text
   value = p[1].text
   name_value[name] = value
port = name_value.get('hadoop.cache.data.bookkeeper.port', 8899)
try:

	transport = TSocket.TSocket('localhost', port)

	transport = TTransport.TBufferedTransport(transport)

	protocol = TBinaryProtocol.TBinaryProtocol(transport)

	client = BookKeeperService.Client(protocol)
	
	transport.open()
	#stats is Dictionary data structure
	#keys are : 'Cache Miss Rate', 'Cache Hit Rate', 'Cache Reads', 'Remote Reads', 'Non-Local Reads'
	stats = client.getCacheStats()
  	print stats

	# Close!
	transport.close()

except Thrift.TException, tx:
	print '%s' % (tx.message)

