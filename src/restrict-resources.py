from apim_types import *
from apim_clients import *
from fedora_services import *
from urlparse import urlparse
import datetime
import base64
import sys
import os, os.path
import codecs
import shutil
import json
import getopt

def loadTemplate(fname):
	t = codecs.open(os.path.join('.', fname), 'rU','utf-8')
	r = t.read()
	t.close()
	return r
def testUri(test):
  return str(test).startswith('info:fedora/') or urlparse(test)[0] in ['http','https','file']

FMODEL = 'info:fedora/fedora-system:def/model#'
DC_TEMPLATE = U"""<?xml version="1.0" encoding="UTF-8"?><oai_dc:dc xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/" \
xmlns:dc="http://purl.org/dc/elements/1.1/" \
xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" \
xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/oai_dc/ http://www.openarchives.org/OAI/2.0/oai_dc.xsd">{0[dc]}</oai_dc:dc>"""

if __name__ == '__main__':
  opts, args = getopt.getopt(sys.argv[1:],'i:o:u:p:c:s:')
  infile=False
  user=False
  password=False
  useSSL = False
  collections = []
  for o,a in opts:
    if o == '-i':
      infile = open(a, 'r')
    if o == '-o':
      sys.stdout = open(a, 'w')
    if o == '-u':
      user = a
    if o == '-p':
      password = a
    if o == '-s':
      useSSl = a
    if o == '-c':
      for c in a.split(';'):
        collections.append(c)

  if not infile:
    print 'usage: python ' + sys.argv[0] + ' -i INFILE [-o OUTFILE]'
    exit()

  myApim = FedoraServices(debug=False)
  if (user):
    myApim.setBasicAuth(username=user,password=password)
  if (useSSL):
    myApim.setSSL(useSSL)
# load batch file
  batch = infile.readlines()
  infile.close()
  predicate = 'info:fedora/fedora-system:def/model#hasModel'
  cmodel='info:fedora/ldpd:RestrictedResource'
  for obj in batch:
    myApim.addRelationship(obj.strip(),predicate,cmodel)
  sys.stdout.flush()
