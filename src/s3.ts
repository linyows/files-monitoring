/**
 * Cloud Storage Files Monitoring
 */

/**
 * AWS S3 Client
 */
export type Config = {
  accessKeyId: string
  secretAccessKey: string
  region?: string
}

export type ExecuteOpts = {
  sessionToken?: string
  logRequests?: string
  echoRequestToUrl?: string
}

export type Credentials = {
  accessKeyId: string
  secretAccessKey: string
  sessionToken: string | undefined
}

export default class S3 {
  accessKeyId: string
  secretAccessKey: string
  httpMethod: string
  contentType: string
  content: string
  bucket: string
  objectName: string
  headers: object
  date: Date
  serviceName: string
  region: string
  expiresHeader: string
  extQueryString: string
  lastExchangeLog: string

  constructor(c: Config) {
    this.accessKeyId = c.accessKeyId
    this.secretAccessKey = c.secretAccessKey
    this.region = c.region || 'ap-northeast-1'

    this.httpMethod = "GET"
    this.contentType = ""
    this.content = ""
    this.bucket = ""
    this.objectName = ""
    this.headers = {}
  
    this.date = new Date()
    this.serviceName = 's3'
    this.expiresHeader = 'presigned-expires'
    this.extQueryString = ''
  }

  getObject (bucket: string, objectName: string, opts?: object) {
    const options = opts || {}
    
    this.setHttpMethod('GET')
    this.setBucket(bucket)
    this.setObjectName(objectName)

    try {
      return this.execute(options).getBlob()
    } catch (e) {
      if (e.name == 'AwsError' && e.code == 'NoSuchKey') {
        return null
      } else {
        throw e
      }
    }
  }
  
  getLastExchangeLog () {
    return this.lastExchangeLog 
  }
  
  logExchange (request, response) {
    var logContent = ''
    logContent += "\n-- REQUEST --\n"
    for (const i in request) {
      if (typeof request[i] == 'string' && request[i].length > 1000) {
        request[i] = request[i].slice(0, 1000) + " ... [TRUNCATED]"
      }
      logContent += Utilities.formatString("\t%s: %s\n", i, request[i])
    }
      
    logContent += "-- RESPONSE --\n"
    logContent += "HTTP Status Code: " + response.getResponseCode() + "\n"
    logContent += "Headers:\n"
    
    var headers = response.getHeaders()
    for (const i in headers) {
      logContent += Utilities.formatString("\t%s: %s\n", i, headers[i])
    }
    logContent += "Body:\n" + response.getContentText()
    this.lastExchangeLog = logContent
  }

  setContentType (c: string) {
    this.contentType = c
    return this
  }
  
  getContentType (): string {
    if (this.contentType) {
      return this.contentType
    } else if (this.httpMethod == "PUT" || this.httpMethod == "POST") {
      return 'application/x-www-form-urlencoded'
    }
    return ''
  }
  
  setContent (c: string): this {
    this.content = c
    return this
  }
  
  setHttpMethod (m: string): this {
    this.httpMethod = m
    return this
  }
  
  setBucket (b: string): this {
    this.bucket = b
    return this
  }

  setObjectName (o: string): this {
    this.objectName = o
    return this
  }
  
  addHeader (name: string, value: string): this {
    this.headers[name] = encodeURIComponent(value)
    return this
  }
  
  _getUrl (): string {
    return "https://s3." + this.region + ".amazonaws.com/" + this.bucket.toLowerCase() + this.objectName
  }

  getUrl (): string {
    return this._getUrl() + this.extQueryString
  }

  execute (opts?: ExecuteOpts) {
    const options = opts || {}
    for (var key in options) {
      var lowerKey = key.toLowerCase()
      if (lowerKey.indexOf('x-amz-') === 0) {
        this.addHeader(key, options[key])
      }
    }
  
    delete this.headers['Authorization']
    delete this.headers['Date']
    delete this.headers['X-Amz-Date']
    this.headers['X-Amz-Content-Sha256'] = this.hexEncodedBodyHash()
    this.headers['Host'] = this._getUrl().replace(/https?:\/\/(.+amazonaws\.com).*/, '$1')
  
    const credentials = {
      accessKeyId: this.accessKeyId,
      secretAccessKey: this.secretAccessKey,
      sessionToken: options.sessionToken
    }
  
    this.addAuthorization(credentials, this.date)
    // To avoid conflict with UrlFetchApp#fetch. UrlFetchApp#fetch adds a Host header.
    delete this.headers['Host']
  
    const params = {
      method: this.httpMethod,
      payload: this.content,
      headers: this.headers,
      muteHttpExceptions: true //get error content in the response
    } as GoogleAppsScript.URL_Fetch.URLFetchRequestOptions
  
    if (this.getContentType()) {
      params.contentType = this.getContentType()
    }
  
    const res = UrlFetchApp.fetch(this.getUrl(), params)
  
    const req = UrlFetchApp.getRequest(this.getUrl(), params)
    this.logExchange(req, res)
    if (options.logRequests) {
      Logger.log(this.getLastExchangeLog())
    }
  
    //used in case you want to peak at the actual raw HTTP request coming out of Google's UrlFetchApp infrastructure
    if (options.echoRequestToUrl) {
      UrlFetchApp.fetch(options.echoRequestToUrl, params)
    }
  
    if (res.getResponseCode() > 299) {
      const error = {
        name: 'AwsError',
        message: '',
        httpRequestLog: '',
      }

      try {
        const el = XmlService.parse(res.getContentText()).getRootElement().getChildren()
        for (const i in el) {
          var name = el[i].getName()
          name = name.charAt(0).toLowerCase() + name.slice(1)
          error[name] = el[i].getText()
        }
        error.toString = function () { return "AWS Error - "+this.code+": "+this.message }
        error.httpRequestLog = this.getLastExchangeLog()
      } catch (e) {
        error.message = "AWS returned HTTP code " + res.getResponseCode() + ", but error content could not be parsed."
        error.toString = function () { return this.message }
        error.httpRequestLog = this.getLastExchangeLog()
      }

      throw error
    }
  
    return res
  }
  
  addAuthorization (credentials: Credentials, date: Date) {
    const datetime = date.toISOString().replace(/[:\-]|\.\d{3}/g, '')
    if (this.isPresigned()) {
      this.updateForPresigned(credentials, datetime)
    } else {
      this.addHeaders(credentials, datetime)
    }
    this.headers['Authorization'] = this.authorization(credentials, datetime)
  }
  
  addHeaders (credentials: Credentials, datetime: string) {
    this.headers['X-Amz-Date'] = datetime
    if (credentials.sessionToken) {
      this.headers['x-amz-security-token'] = credentials.sessionToken
    }
  }
  
  updateForPresigned (credentials: Credentials, datetime: string) {
    const credString = this.credentialString(datetime)
    const qs = {
      'X-Amz-Date': datetime,
      'X-Amz-Algorithm': 'AWS4-HMAC-SHA256',
      'X-Amz-Credential': credentials.accessKeyId + '/' + credString,
      'X-Amz-Expires': this.headers[this.expiresHeader],
      'X-Amz-SignedHeaders': this.signedHeaders()
    }
  
    if (credentials.sessionToken) {
      qs['X-Amz-Security-Token'] = credentials.sessionToken
    }
  
    if (this.headers['Content-Type']) {
      qs['Content-Type'] = this.headers['Content-Type']
    }
    if (this.headers['Content-MD5']) {
      qs['Content-MD5'] = this.headers['Content-MD5']
    }
    if (this.headers['Cache-Control']) {
      qs['Cache-Control'] = this.headers['Cache-Control']
    }
  
    for (const key in this.headers) {
      if (key === this.expiresHeader) continue
      if (this.isSignableHeader(key)) {
        const lowerKey = key.toLowerCase()
        // Metadata should be normalized
        if (lowerKey.indexOf('x-amz-meta-') === 0) {
          qs[lowerKey] = this.headers[key]
        } else if (lowerKey.indexOf('x-amz-') === 0) {
          qs[key] = this.headers[key]
        }
      }
    }
  
    const sep = this._getUrl().indexOf('?') >= 0 ? '&' : '?'
    const queryParamsToString = function(params) {
      const items = []
      for (const key in params) {
        const value = params[key]
        const ename = encodeURIComponent(key)
        if (Array.isArray(value)) {
          var vals = []
          for(var i in value) { vals.push(encodeURIComponent(value[i])) }
          items.push(ename + '=' + vals.sort().join('&' + ename + '='))
        } else {
          items.push(ename + '=' + encodeURIComponent(value))
        }
      }
      return items.sort().join('&')
    }
    this.extQueryString += sep + queryParamsToString(qs)
  }
  
  authorization (credentials: Credentials, datetime: string) {
    const parts = []
    const credString = this.credentialString(datetime)
    parts.push('AWS4-HMAC-SHA256 Credential=' + credentials.accessKeyId + '/' + credString)
    parts.push('SignedHeaders=' + this.signedHeaders())
    parts.push('Signature=' + this.signature(credentials, datetime))
    return parts.join(', ')
  }
  
  signature (credentials: Credentials, datetime: string) {
    const sigingKey = this.getSignatureKey(
      credentials.secretAccessKey,
      datetime.substr(0, 8),
      this.region,
      this.serviceName
    )

    const signature = Utilities.computeHmacSha256Signature(
      Utilities.newBlob(this.stringToSign(datetime)).getBytes(),
      sigingKey
    )

    return this.hex(signature)
  }
  
  hex (values) {
    return values.reduce(function(str, chr){
      chr = (chr < 0 ? chr + 256 : chr).toString(16)
      return str + (chr.length == 1 ? '0' : '') + chr
    }, '')
  }
  
  getSignatureKey (key: string, dateStamp: string, regionName: string, serviceName: string) {
    const kDate = Utilities.computeHmacSha256Signature(dateStamp, "AWS4" + key)
    const kRegion = Utilities.computeHmacSha256Signature(Utilities.newBlob(regionName).getBytes(), kDate)
    const kService = Utilities.computeHmacSha256Signature(Utilities.newBlob(serviceName).getBytes(), kRegion)
    const kSigning = Utilities.computeHmacSha256Signature(Utilities.newBlob("aws4_request").getBytes(), kService)
    return kSigning
  }
  
  stringToSign (datetime: string) {
    const parts = []
    parts.push('AWS4-HMAC-SHA256')
    parts.push(datetime)
    parts.push(this.credentialString(datetime))
    parts.push(this.hexEncodedHash(this.canonicalString()))
    return parts.join('\n')
  }
  
  canonicalString () {
    const parts = []
    const [base, search] = this.getUrl().split("?", 2)
    parts.push(this.httpMethod)
    parts.push(this.canonicalUri(base))
    parts.push(this.canonicalQueryString(search))
    parts.push(this.canonicalHeaders() + '\n')
    parts.push(this.signedHeaders())
    parts.push(this.hexEncodedBodyHash())
    return parts.join('\n')
  }
  
  canonicalUri (uri: string) {
    const m = uri.match(/https?:\/\/s3.*\.amazonaws\.com\/(.+)$/)
    const object = m ? m[1] : ""
    return "/" + encodeURIComponent(object).replace(/%2F/ig, '/')
  }
  
  canonicalQueryString (values) {
    if (!values) return ""
    const parts = []
    const items = values.split("&")
    for (const i in items) {
      const [key, value] = items[i].split("=")
      parts.push(encodeURIComponent(key.toLowerCase()) + "=" + encodeURIComponent(value))
    }
    return parts.sort().join("&")
  }
  
  canonicalHeaders () {
    const parts = []
    for (const item in this.headers) {
      const key = item.toLowerCase()
      if (this.isSignableHeader(key)) {
        const header = key + ":" + this.canonicalHeaderValues(this.headers[item].toString())
        parts.push(header)
      }
    }
    return parts.sort().join("\n")
  }
  
  canonicalHeaderValues (values) {
    return values.replace(/\s+/g, " ").trim()
  }
  
  signedHeaders () {
    const keys = []
    for (const key in this.headers) {
      const k = key.toLowerCase()
      if (this.isSignableHeader(k)) {
        keys.push(k)
      }
    }
    return keys.sort().join(';')
  }
  
  credentialString (datetime: string) {
    return [
      datetime.substr(0, 8),
      this.region,
      this.serviceName,
      'aws4_request'
    ].join('/')
  }
  
  hexEncodedHash (s: string) {
    return this.hex(
      Utilities.computeDigest(
        Utilities.DigestAlgorithm.SHA_256,
        s,
        Utilities.Charset.UTF_8
      )
    )
  }
  
  hexEncodedBodyHash () {
    if (this.isPresigned() && !this.content.length) {
      return 'UNSIGNED-PAYLOAD'
    } else if (this.headers['X-Amz-Content-Sha256']) {
      return this.headers['X-Amz-Content-Sha256']
    } else {
      return this.hexEncodedHash(this.content || '')
    }
  }
  
  isSignableHeader (key: string) {
    var lowerKey = key.toLowerCase()
    if (lowerKey.indexOf('x-amz-') === 0) return true
    var unsignableHeaders = [
      'authorization',
      'content-type',
      'content-length',
      'user-agent',
      this.expiresHeader,
      'expect',
      'x-amzn-trace-id'
    ]
    return unsignableHeaders.indexOf(lowerKey) < 0
  }
  
  isPresigned (): boolean {
    return this.headers[this.expiresHeader] ? true : false
  }
}