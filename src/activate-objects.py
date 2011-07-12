from apim_types import *
from apim_clients import *
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

STUB_TEMPLATE = loadTemplate(sys.path[0] + '/object-stub.xml')
FMODEL = 'info:fedora/fedora-system:def/model#'
DC_TEMPLATE = U"""<?xml version="1.0" encoding="UTF-8"?><oai_dc:dc xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/" \
xmlns:dc="http://purl.org/dc/elements/1.1/" \
xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" \
xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/oai_dc/ http://www.openarchives.org/OAI/2.0/oai_dc.xsd">{0[dc]}</oai_dc:dc>"""

class FedoraServices(object):
  def __init__(self,debug=False):
    self.map = dict()
    self.locator = ServiceLocator()
    if debug:
      self.apim = self.locator.getDebug()
      self.apia = self.locator.getAPIA()
    else:
      self.apim = self.locator.getAPIM(host='localhost',port='8443',SSL=True,debug=True)
      self.apia = self.locator.getAPIA(debug=True)
  def setBasicAuth(self, username, password):
    self.apim.binding.setAuth(username=username, password=password)
    self.apia.binding.setAuth(username=username, password=password)
  def setSSL(self, useSSL):
    self.apim.binding.setSSL(useSSL)
  def nextPid(self, pidNamespace='ldpd'):
    request = getNextPIDRequest(numPIDs=1, pidNamespace=pidNamespace)
    response = self.apim.getNextPID(request)
    return response.pid[0]

  def createStubObject(self,dateCreated):
    pid = self.nextPid()
    src = STUB_TEMPLATE.format({'pid':pid,'dateCreated':dateCreated})
    request = ingestRequest(objectXml=src)
    response = self.apim.ingest(request)
    pid = response.objectPID
    if not pid.startswith('info:fedora/'):
      pid = 'info:fedora/' + pid
    return pid
    
  def searchByDC(self, searchField, value, resultFields, verify):
    uris = []
    if not 'pid' in resultFields: resultFields.append('pid')
    request = findObjectsRequest(resultFields=resultFields, maxResults=2, query=[condition(searchField,"has",value)])
    response = self.apia.findObjects(request)
    for result in response.resultList:
      if verify and searchField in resultFields:
        rValues = result.__getattr__(searchField)
        if not(value in rValues):
          print value + " not in " + repr(rValues)
          raise Exception("false positive on dc:" + searchField + "value match: " + rValues[0] + " for " + value)
      objuri = 'info:fedora/' + result.pid[0]
      print 'found object ' + objuri + ' for ' + searchField + ' ' + value
      if objuri not in uris:
        uris.append(objuri)
    return uris
# do RI query (or admin query)
# if none returned, create stub object and return new pid
# else return found pid
  def getObjectForId(self, values, dateCreated, field='identifier'):
    uris = []
    for value in values:
      value = value.strip()
      if (value in self.map):
        uris.append(self.map[value])
      else:
        resultList = self.searchByDC(searchField=field, value=value,resultFields=["pid","identifier"],verify=True)
        for result in resultList:
          self.map[value] = result
          print 'found object ' + self.map[value] + ' for ' + field + ' ' + value
          if self.map[value] not in uris:
            uris.append(self.map[value])
    if len(uris) > 1:
      print 'ERROR: ' + repr(uris)
    if len(uris) == 0:
      uris.append(self.createStubObject(dateCreated))
      print uris[0] + ' stub object created'
      for value in values:
        self.map[value] = uris[0]
        print uris[0] + ' mapped to ' + value
    if len(uris) == 1:
      for value in values:
        if not (value in self.map):
          self.map[value] = uris[0]
          print uris[0] + ' mapped to ' + value
    return self.map[values[0]]

  def getUriForId(self,identifiers):
    return self.map[identifiers[0].strip()]

  def addRelationship(self,objuri, predicate, object, object_type=None):
    object = object.strip()
    if (object in self.map):
      object = self.map[object]
    if (testUri(object) and object_type == None):
      isLiteral = False
    else:
      isLiteral = True
    split = objuri.split('/')
    pid = split[-1]
    request = addRelationshipRequest(pid=pid, relationship=predicate, object=object, datatype=object_type, isLiteral=isLiteral)
    response = self.apim.addRelationship(request)
    if (response.added):
      print 'SUCCESS: added ' + objuri + ' ' + predicate + ' ' + object
    else:
      if (response.status is 200):
        print 'NOOP: existing triple at ' + objuri + ' ' + predicate + ' ' + object
      else:
        print 'FAILURE: could not add ' + objuri + ' ' + predicate + ' ' + object + ' status ' + str(response.status)
  def addDatastream(self, objuri, properties):
    if not("formatURI" in properties):
      properties["formatURI"] = None
    split = objuri.split('/')
    pid = split[-1]
    request = listDatastreamsRequest(pid=pid)
    response = self.apia.listDatastreams(request)
    found = False
    for datastream in response.datastreamDef:
      if datastream.ID == properties["dsID"]:
        found = True
    if not found:
      rclass = addDatastreamRequest
      op = self.apim.addDatastream
    else:
      rclass = modifyDatastreamByReferenceRequest
      op = self.apim.modifyDatastreamByReference
    if properties["controlGroup"] is not 'M':
      oldDsLocation = properties["dsLocation"]
      newDsLocation = oldDsLocation.replace('/diskonly/','/')
      newPath = newDsLocation.replace('file://','')
      oldPath = oldDsLocation.replace('file://','')
      os.makedirs(os.path.dirname(newPath))
      try:
        shutil.copyfile(oldPath, newPath)
      except Exception as e:
        print 'ERROR: copying ' + oldPath + ' to ' + newPath + ': ' + repr(e)
      properties["dsLocation"] = newDsLocation


    request = rclass(pid=pid, dsID=properties["dsID"],
                                     dsLocation=properties["dsLocation"],controlGroup=properties["controlGroup"],
                                     formatURI=properties["formatURI"])
    try:
      response = op(request)
      if (response.status == 200):
        print 'SUCCESS: ' + str(op.__class__) +'  ' + objuri + '/' + properties["dsID"] + '=' + properties["dsLocation"]
      else:
        print 'ERROR: ' + str(op.__class__) +'  ' + objuri + '/' + properties["dsID"] + '=' + properties["dsLocation"]
    except Exception as ex:
      print 'ERROR: ' + repr(ex)
      print 'ERROR: ' + str(op.__class__) +'  ' + objuri + ' ' + repr(request)
  def addDcProps(self, objuri, properties):
    if (len(properties) == 0):
      return
    value = ''
    for property in properties:
      vals = properties[property]
      ot = '<dc:' + property + '>'
      ct = '</dc:' + property + '>'
      for val in vals:
        value += (ot + val + ct)
        print objuri + ' dc:' + property + ' ' + val
    dsContent = DC_TEMPLATE.format({"dc":value})
    print dsContent
    split = objuri.split('/')
    pid = split[-1]
    request = modifyDatastreamByValueRequest(pid=pid, dsID='DC',
                                             formatURI='http://www.openarchives.org/OAI/2.0/oai_dc/',
                                             dsContent=dsContent, dsLabel='Dublin Core Record for this object',
                                             MIMEType='text/xml')
    response = self.apim.modifyDatastreamByValue(request)
    print objuri + ' modified DC datastream by value at ' + str(response.modifiedDate)
  def modifyObject(self, objuri, **kw):
    split = objuri.split('/')
    pid = split[-1]
    request = modifyObjectRequest(pid=pid, **kw)
    response = self.apim.modifyObject(request)
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
# load json
  batch = infile.readlines()
  infile.close()

  for obj in batch:
    myApim.modifyObject(obj.strip(),state='A')
  sys.stdout.flush()
