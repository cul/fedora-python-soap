import urlparse, types, codecs, datetime, base64, httplib
from apim_types import *

class ServiceLocator:
  APIM_PATH = '/fedora/services/management'
  APIA_PATH = '/fedora/services/access'
  default_path = "/fedora/services/management"
  default_host = "localhost"
  def getFedora_API_MAddress(self):
      return Fedora_API_M_ServiceLocator.Fedora_API_M_address
  def getAPIA(self, host=None, path=APIA_PATH, **kw):
      return APIAClient(host or ServiceLocator.default_host, path=path, **kw)
  def getAPIM(self, host=None, path=APIM_PATH, **kw):
      return APIMClient(host=ServiceLocator.default_host, path=path, **kw)
  def getDebug(self, host=None, path=None, **kw):
      return DebugProxy(host=ServiceLocator.default_host, path=path, **kw)

class Binding:
  def __init__(self,host='localhost',port=8080,path='/fedora/services/management', **kw):
    self.debug = 2
    self.host = host
    self.port = port
    self.path = path
    if ("Credentials" in kw):
      self.creds = base64.b64encode(creds)
    if ("SSL" in kw):
      self.useSSL = kw["SSL"]
    else:
      self.useSSL = False
    if ("debug" in kw):
      self.debug = kw["debug"]
    else:
      self.debug = False
  def setSSL(self,value):
    self.useSSL=value
  def setAuth(self,username,password):
    self.creds = base64.b64encode(username + ':' + password)
  def send(self,request):
    """Handles making the SOAP request"""
    if (self.useSSL):
      connection = httplib.HTTPSConnection(self.host,self.port)
    else:
      connection = httplib.HTTPConnection(self.host,self.port)

    input = request.serialize()
    myheaders={
    	'Host':self.host,
    	'Content-Type':"text/xml; charset=\"UTF-8\"",
    	'Content-Length':str(len(input)),
    	'SOAPAction':'"%s"' % request.soapaction,
    }
    creds = getattr(self,'creds',None)
    if (creds):
      myheaders['Authorization'] = 'Basic ' + creds
    if self.debug:
      try:
        print input.encode('utf-8')
      except:
        pass
    connection.request ('POST', self.path, body=input,headers=myheaders)
    response = connection.getresponse()
    self.responsedata = response.read()
    self.status = response.status
    if response.status!=200:
    	if self.debug: print self.responsedata
        url = self.host + ':' + str(self.port) + self.path
    	raise ValueError('Error connecting to %s: %s, %s' % (url,response.status, response.reason))
  def receive(self,response):
    if self.debug:
      print self.responsedata
    response.parse(self.responsedata)
    response.status = self.status
    return response

class Client:
  def __init__(self, host, path, **kw):
    self.nextPID = 0
    self.binding = Binding(host=host, path=path, **kw)

class APIAClient(Client):
    def __init__(self, host, path, **kw):
      Client.__init__(self, host, path, **kw)
    def findObjects(self, request):
      if isinstance(request, findObjectsRequest) is False:
        raise TypeError, "%s incorrect request type" % (request.__class__)
      response = findObjectsResponse()
      self.binding.send(request)
      self.binding.receive(response)
      return response
    def listDatastreams(self, request):
      if isinstance(request, listDatastreamsRequest) is False:
        raise TypeError, "%s incorrect request type" % (request.__class__)
      response = listDatastreamsResponse()
      self.binding.send(request)
      self.binding.receive(response)
      return response
    def getObjectProfile(self, request):
      if isinstance(request, getObjectProfileRequest) is False:
        raise TypeError, "%s incorrect request type" % (request.__class__)
      response = getObjectProfileResponse()
      self.binding.send(request)
      self.binding.receive(response)
      return response

class APIMClient(Client):
    def __init__(self, host, path, **kw):
      Client.__init__(self, host, path, **kw)

    # op: ingest
    def ingest(self, request):
        if isinstance(request, ingestRequest) is False:
            raise TypeError, "%s incorrect request type" % (request.__class__)
        kw = {}
        # no input wsaction
        response = ingestResponse()
        self.binding.send(request)
        self.binding.receive(response)
        return response

    # op: modifyObject
    def modifyObject(self, request):
        if isinstance(request, modifyObjectRequest) is False:
            raise TypeError, "%s incorrect request type" % (request.__class__)
        kw = {}
        # no input wsaction
        # no output wsaction
        response = modifyObjectResponse()
        self.binding.send(request)
        self.binding.receive(response)
        return response

    # op: getObjectXML
    def getObjectXML(self, request):
        if isinstance(request, getObjectXMLRequest) is False:
            raise TypeError, "%s incorrect request type" % (request.__class__)
        response = getObjectXMLResponse()
        self.binding.send(request)
        self.binding.receive(response)
        return response

    # op: export
    def export(self, request):
        if isinstance(request, exportRequest) is False:
            raise TypeError, "%s incorrect request type" % (request.__class__)
        response = exportResponse()
        self.binding.send(request)
        self.binding.receive(response)
        return response

    # op: purgeObject
    def purgeObject(self, request):
        if isinstance(request, purgeObjectRequest) is False:
            raise TypeError, "%s incorrect request type" % (request.__class__)
        response = purgeObjectResponse()
        self.binding.send(request)
        self.binding.receive(response)
        return response

    # op: addDatastream
    def addDatastream(self, request):
        if isinstance(request, addDatastreamRequest) is False:
            raise TypeError, "%s incorrect request type" % (request.__class__)
        response = addDatastreamResponse()
        self.binding.send(request)
        self.binding.receive(response)
        return response

    # op: modifyDatastreamByReference
    def modifyDatastreamByReference(self, request):
        if isinstance(request, modifyDatastreamByReferenceRequest) is False:
            raise TypeError, "%s incorrect request type" % (request.__class__)
        response = modifyDatastreamByReferenceResponse()
        self.binding.send(request)
        self.binding.receive(response)
        return response

    # op: modifyDatastreamByValue
    def modifyDatastreamByValue(self, request):
        if isinstance(request, modifyDatastreamByValueRequest) is False:
            raise TypeError, "%s incorrect request type" % (request.__class__)
        response = modifyDatastreamByValueResponse()
        self.binding.send(request)
        self.binding.receive(response)
        return response

    # op: setDatastreamState
    def setDatastreamState(self, request):
        if isinstance(request, setDatastreamStateRequest) is False:
            raise TypeError, "%s incorrect request type" % (request.__class__)
        response = setDatastreamStateResponse()
        self.binding.send(request)
        self.binding.receive(response)
        return response

    # op: setDatastreamVersionable
    def setDatastreamVersionable(self, request):
        if isinstance(request, setDatastreamVersionableRequest) is False:
            raise TypeError, "%s incorrect request type" % (request.__class__)
        response = setDatastreamVersionableResponse()
        self.binding.send(request)
        self.binding.receive(response)
        return response

    # op: compareDatastreamChecksum
    def compareDatastreamChecksum(self, request):
        if isinstance(request, compareDatastreamChecksumRequest) is False:
            raise TypeError, "%s incorrect request type" % (request.__class__)
        response = compareDatastreamChecksumResponse()
        self.binding.send(request)
        self.binding.receive(response)
        return response

    # op: getDatastream
    def getDatastream(self, request):
        if isinstance(request, getDatastreamRequest) is False:
            raise TypeError, "%s incorrect request type" % (request.__class__)
        response = getDatastreamResponse()
        self.binding.send(request)
        self.binding.receive(response)
        return response

    # op: getDatastreams
    def getDatastreams(self, request):
        if isinstance(request, getDatastreamsRequest) is False:
            raise TypeError, "%s incorrect request type" % (request.__class__)
        response = getDatastreamsResponse()
        self.binding.send(request)
        self.binding.receive(response)
        return response

    # op: getDatastreamHistory
    def getDatastreamHistory(self, request):
        if isinstance(request, getDatastreamHistoryRequest) is False:
            raise TypeError, "%s incorrect request type" % (request.__class__)
        response = getDatastreamHistoryHistoryResponse()
        self.binding.send(request)
        self.binding.receive(response)
        return response

    # op: purgeDatastream
    def purgeDatastream(self, request):
        if isinstance(request, purgeDatastreamRequest) is False:
            raise TypeError, "%s incorrect request type" % (request.__class__)
        response = purgeDatastreamResponse()
        self.binding.send(request)
        self.binding.receive(response)
        return response

    # op: getNextPID
    def getNextPID(self, request):
        if isinstance(request, getNextPIDRequest) is False:
            raise TypeError, "%s incorrect request type" % (request.__class__)
        kw = {}
        response = getNextPIDResponse()
        self.binding.send(request)
        self.binding.receive(response)
        return response

    # op: getRelationships
    def getRelationships(self, request):
        if isinstance(request, getRelationshipsRequest) is False:
            raise TypeError, "%s incorrect request type" % (request.__class__)
        response = getRelationshipsResponse()
        self.binding.send(request)
        return self.binding.receive(response)

    # op: addRelationship
    def addRelationship(self, request):
        if isinstance(request, addRelationshipRequest) is False:
            raise TypeError, "%s incorrect request type" % (request.__class__)
        response = addRelationshipResponse()
        self.binding.send(request)
        self.binding.receive(response)
        return response

    # op: purgeRelationship
    def purgeRelationship(self, request):
        if isinstance(request, purgeRelationshipRequest) is False:
            raise TypeError, "%s incorrect request type" % (request.__class__)
        response = purgeRelationshipResponse()
        self.binding.send(request)
        self.binding.receive(response)
        return response

class DebugProxy:
    def __init__(self, host, path, **kw):
      self.nextPID = 0
      self.binding = Binding(host=host, path=path, **kw)

    # op: ingest
    def ingest(self, request):
        if isinstance(request, ingestRequest) is False:
            raise TypeError, "%s incorrect request type" % (request.__class__)
        kw = {}
        # no input wsaction
        response = ingestResponse()
        response.objectPID = "info:fedora/changeme:" + str(self.nextPID)
        response.status = 200
        return response

    # op: modifyObject
    def modifyObject(self, request):
        if isinstance(request, modifyObjectRequest) is False:
            raise TypeError, "%s incorrect request type" % (request.__class__)
        kw = {}
        # no input wsaction
        # no output wsaction
        response = modifyObjectResponse()
        response.status = 200
        return response

    # op: getObjectXML
    def getObjectXML(self, request):
        if isinstance(request, getObjectXMLRequest) is False:
            raise TypeError, "%s incorrect request type" % (request.__class__)
        response = getObjectXMLResponse()
        response.status = 200
        return response

    # op: export
    def export(self, request):
        if isinstance(request, exportRequest) is False:
            raise TypeError, "%s incorrect request type" % (request.__class__)
        response = exportResponse()
        response.status = 200
        return response

    # op: purgeObject
    def purgeObject(self, request):
        if isinstance(request, purgeObjectRequest) is False:
            raise TypeError, "%s incorrect request type" % (request.__class__)
        response = purgeObjectResponse()
        response.status = 200
        return response

    # op: addDatastream
    def addDatastream(self, request):
        if isinstance(request, addDatastreamRequest) is False:
            raise TypeError, "%s incorrect request type" % (request.__class__)
        response = addDatastreamResponse()
        response.datastreamID = request.dsID
        response.status = 200
        return response

    # op: modifyDatastreamByReference
    def modifyDatastreamByReference(self, request):
        if isinstance(request, modifyDatastreamByReferenceRequest) is False:
            raise TypeError, "%s incorrect request type" % (request.__class__)
        response = modifyDatastreamByReferenceResponse()
        response.modifiedDate = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000Z")
        response.status = 200
        return response

    # op: modifyDatastreamByValue
    def modifyDatastreamByValue(self, request):
        if isinstance(request, modifyDatastreamByValueRequest) is False:
            raise TypeError, "%s incorrect request type" % (request.__class__)
        response = modifyDatastreamByValueResponse()
        response.modifiedDate = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000Z")
        response.status = 200
        return response

    # op: setDatastreamState
    def setDatastreamState(self, request):
        if isinstance(request, setDatastreamStateRequest) is False:
            raise TypeError, "%s incorrect request type" % (request.__class__)
        response = setDatastreamStateResponse()
        response.status = 200
        return response

    # op: setDatastreamVersionable
    def setDatastreamVersionable(self, request):
        if isinstance(request, setDatastreamVersionableRequest) is False:
            raise TypeError, "%s incorrect request type" % (request.__class__)
        response = setDatastreamVersionableResponse()
        response.status = 200
        return response

    # op: compareDatastreamChecksum
    def compareDatastreamChecksum(self, request):
        if isinstance(request, compareDatastreamChecksumRequest) is False:
            raise TypeError, "%s incorrect request type" % (request.__class__)
        response = compareDatastreamChecksumResponse()
        response.status = 200
        return response

    # op: getDatastream
    def getDatastream(self, request):
        if isinstance(request, getDatastreamRequest) is False:
            raise TypeError, "%s incorrect request type" % (request.__class__)
        response = getDatastreamResponse()
        response.status = 200
        return response

    # op: getDatastreams
    def getDatastreams(self, request):
        if isinstance(request, getDatastreamsRequest) is False:
            raise TypeError, "%s incorrect request type" % (request.__class__)
        response = getDatastreamsResponse()
        response.status = 200
        return response

    # op: getDatastreamHistory
    def getDatastreamHistory(self, request):
        if isinstance(request, getDatastreamHistoryRequest) is False:
            raise TypeError, "%s incorrect request type" % (request.__class__)
        response = getDatastreamHistoryHistoryResponse()
        response.status = 200
        return response

    # op: purgeDatastream
    def purgeDatastream(self, request):
        if isinstance(request, purgeDatastreamRequest) is False:
            raise TypeError, "%s incorrect request type" % (request.__class__)
        response = purgeDatastreamResponse()
        response.status = 200
        return response

    # op: getNextPID
    def getNextPID(self, request):
        if isinstance(request, getNextPIDRequest) is False:
            raise TypeError, "%s incorrect request type" % (request.__class__)
        kw = {}
        response = getNextPIDResponse()
        self.nextPID += 1
        response.pid.append(request.pidNamespace + ':' + str(self.nextPID))
        response.status = 200
        return response

    # op: getRelationships
    def getRelationships(self, request):
        if isinstance(request, getRelationshipsRequest) is False:
            raise TypeError, "%s incorrect request type" % (request.__class__)
        response = getRelationshipsResponse()
        response.status = 200
        return response

    # op: addRelationship
    def addRelationship(self, request):
        if isinstance(request, addRelationshipRequest) is False:
            raise TypeError, "%s incorrect request type" % (request.__class__)
        response = addRelationshipResponse()
        response.added = True
        response.status = 200
        return response

    # op: purgeRelationship
    def purgeRelationship(self, request):
        if isinstance(request, purgeRelationshipRequest) is False:
            raise TypeError, "%s incorrect request type" % (request.__class__)
        response = purgeRelationshipResponse()
        response.status = 200
        return response
