from mime_types import *
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

def testUri(test):
  return str(test).startswith('info:fedora/') or urlparse(test)[0] in ['http','https','file']

def usage():
  print 'usage: python ' + sys.argv[0] + ' -i INFILE -u USERNAME -w PASSWORD -p PID [-s USESSL] [-o OUTFILE]'

## MAIN ##

if __name__ == '__main__':
  opts, args = getopt.getopt(sys.argv[1:],'i:o:u:p:w:c:s:')
  infile=False
  user=False
  password=False
  pid=False
  objuri=False
  useSSL = True
  for o,a in opts:
    if o == '-i':
      infile = a
    if o == '-o':
      sys.stdout = open(a, 'w')
    if o == '-u':
      user = a
    if o == '-w':
      password = a
    if o == '-p':
      pid = a
      objuri = 'info:fedora/' + a
    if o == '-s':
      useSSl = (a.lower == 'true')

  if not infile or not os.access(infile, os.R_OK):
    usage()
    exit()
  else:
    fileuri = 'file://' + infile

  myApim = FedoraServices(debug=False)
  if (user):
    myApim.setBasicAuth(username=user,password=password)
  if (useSSL):
    myApim.setSSL(useSSL)
  properties = {'dsID':'CONTENT'}
  properties['dsLocation'] = fileuri
  properties['MIMEType'] = mime_from_path(infile)
  properties['controlGroup'] = 'M'
  properties['dsState'] = 'A'
  properties['formatURI'] = ''
  myApim.addDatastream(pid.strip(),properties)
  if properties['MIMEType'].startswith('image'):
    props = image.triples(infile)
    for triple in props['triples']:
      # search for <objuri> <triple.predicate>, and remove
      oldtriples = myApim.getRelationships(subject=objuri,relationship=triple.predicate)
      for oldtriple in oldtriples:
        myApim.purgeRelationship(subject=objuri,relationship-oldtriple.predicate, object=oldtriple.object, isLiteral = testUri(oldtriple.object))
      # add triples
      myApim.addRelationship(subject=objuri, relationship=triple.predicate, object=triple.object, isLiteral=isUri(triple.object))
  sys.stdout.flush()
