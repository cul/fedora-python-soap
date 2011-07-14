from mime_types import *
from apim_types import *
from apim_clients import *
from fedora_services import *
from urlparse import urlparse
import base64
import codecs
import datetime
import getopt
import json
import md5
import os, os.path
import shutil
import sys

def testUri(test):
  return str(test).startswith('info:fedora/') or urlparse(test)[0] in ['http','https','file']

def usage():
  print 'usage: python ' + sys.argv[0] + ' -i INFILE -u USERNAME -w PASSWORD [-s USESSL] [-o OUTFILE]'

def toPidAndUri(pidOrUri):
  pidOrUri = pidOrUri.strip()
  if pidOrUri.startswith("info:fedora/"):
    return (pidOrUri.split('/')[1], pidOrUri)
  else:
    return (pidOrUri, "info:fedora/" + pidOrUri)

def getDescriptum(relationships):
  for triple in relationships:
    if triple.predicate == "http://purl.oclc.org/NET/CUL/metadataFor":
      return triple.object
  return None

def contentUri(host,port,pid):
  return "https://%s:%s/fedora/objects/%s/datastreams/CONTENT/content" % (host,port,pid)

def compareContent(fedora, dsUri1, dsUri2):
  parts1 = dsUri1.split('/')
  parts2 = dsUri2.split('/')
  content1 = fedora.getDatastreamContent(parts1[1],parts1[2])
  content2 = fedora.getDatastreamContent(parts2[1],parts2[2])
  result = (len(content1) == len(content2))
  if result:
    m1 = md5.new(content1).digest()
    m2 = md5.new(content2).digest()
    return (m1 == m2)
  else:
    return False
## MAIN ##

if __name__ == '__main__':
  opts, args = getopt.getopt(sys.argv[1:],'i:o:u:p:w:c:h:s')
  infile=False
  user=False
  password=False
  objuri=False
  useSSL = True
  host = 'localhost'
  for o,a in opts:
    if o == '-i':
      inpath = a
    if o == '-o':
      sys.stdout = open(a, 'w')
    if o == '-u':
      user = a
    if o == '-h':
      host = a
    if o == '-p':
      password = a

  if not inpath or not os.access(inpath, os.R_OK):
    usage()
    exit()
  else:
    fileuri = 'file://' + inpath
  myApim = FedoraServices(hostname=host,debug=False, port=8443)
  myApim.setSSL(True,True)
  if (user):
    myApim.setBasicAuth(username=user,password=password)
  # for each pid in file
  with open(inpath) as infile:
    for line in infile: 
      (pid,objuri) = toPidAndUri(line)
    # get rels for metadata object
      response = myApim.getRelationships(objuri,None)
    # verify that it is a MODSMetadata object and get descriptum
      descriptum = getDescriptum(response.relationships)
      if descriptum != None:
        dsProps = {'pid' : descriptum,
                   'dsID' : 'descMetadata',
                   'dsLabel' : 'Descriptive Metadata',
                   'MIMEType' : 'text/xml',
                   'checksumType' : 'SHA-1',
                   'dsLocation' : contentUri(host,8443,pid)
                  }
        dsProps['versionable'] = True
        dsProps['controlGroup'] = 'M'
        dsProps['dsState'] = 'A'
    # addDatastream handles both new and modifed datastreams
        print "would have added ds to %s" % descriptum
        # myApim.addDatastream(descriptum, dsProps)
    # compare source and destination datastreams
        if not compareContent(myApim, descriptum + '/descMetadata', objuri + '/CONTENT'):
          print ("ERROR: %s/descMetadata and %s/CONTENT do not appear identical as expected." % (descriptum, objuri))
  sys.stdout.flush()
