import datetime, re, time, unicodedata, hashlib, urlparse, types

# [might want to look into language/country stuff at some point] 
# param info here: http://code.google.com/apis/ajaxsearch/documentation/reference.html
#
GOOGLE_JSON_URL = 'http://ajax.googleapis.com/ajax/services/search/web?v=1.0&userip=%s&rsz=large&q=%s'
FREEBASE_URL    = 'http://freebase.plexapp.com'
FREEBASE_BASE   = 'movies'
PLEXMOVIE_URL   = 'http://plexmovie.plexapp.com'
PLEXMOVIE_BASE  = 'movie.12.unicode'

SCORE_THRESHOLD_IGNORE         = 85
SCORE_THRESHOLD_IGNORE_PENALTY = 100 - SCORE_THRESHOLD_IGNORE
SCORE_THRESHOLD_IGNORE_PCT = float(SCORE_THRESHOLD_IGNORE_PENALTY)/100
PERCENTAGE_BONUS_MAX = 10

def Start():
  HTTP.CacheTime = CACHE_1HOUR * 4
  
class PlexMovieAgent(Agent.Movies):
  name = 'Freebase'
  languages = [Locale.Language.English, Locale.Language.Swedish, Locale.Language.French, 
               Locale.Language.Spanish, Locale.Language.Dutch, Locale.Language.German, 
               Locale.Language.Italian]

  def identifierize(self, string):
      string = re.sub( r"\s+", " ", string.strip())
      string = unicodedata.normalize('NFKD', safe_unicode(string))
      string = re.sub(r"['\"!?@#$&%^*\(\)_+\.,;:/]","", string)
      string = re.sub(r"[_ ]+","_", string)
      string = string.strip('_')
      return string.strip().lower()

  def guidize(self, string):
    hash = hashlib.sha1()
    hash.update(string.encode('utf-8'))
    return hash.hexdigest()

  def titleyear_guid(self, title, year):
    if title is None:
      title = ''

    if year == '' or year is None or not year:
      string = "%s" % self.identifierize(title)
    else:
      string = "%s_%s" % (self.identifierize(title).lower(), year)
    return self.guidize("%s" % string)
  
  def getPublicIP(self):
    ip = HTTP.Request('http://plexapp.com/ip.php').content.strip()
    return ip
  
  def getGoogleResults(self, url):
    try:
      jsonObj = JSON.ObjectFromURL(url, sleep=0.5)
      if jsonObj['responseData'] != None:
        jsonObj = jsonObj['responseData']['results']
        if len(jsonObj) > 0:
          return jsonObj
    except:
      Log("Exception obtaining result from Google.")
    
    return []
  
  def search(self, results, media, lang, manual=False):
    
    # Keep track of best name.
    idMap = {}
    bestNameMap = {}
    bestNameDist = 1000
   
    # TODO: create a plex controlled cache for lookup
    # TODO: by imdbid  -> (title,year)
    # See if we're being passed a raw ID.
    findByIdCalled = False
    if media.guid or re.match('t*[0-9]{7}', media.name):
      theGuid = media.guid or media.name 
      if not theGuid.startswith('tt'):
        theGuid = 'tt' + theGuid
      
      # Add a result for the id found in the passed in guid hint.
      findByIdCalled = True
      (title, year) = self.findById(theGuid)
      if title is not None:
        results.Append(MetadataSearchResult(id=theGuid, name=title, year=year, lang=lang, score=100))
        bestNameMap[theGuid] = title
          
    if media.year:
      searchYear = u' (' + safe_unicode(media.year) + u')'
    else:
      searchYear = u''

    # first look in the proxy/cache 
    titleyear_guid = self.titleyear_guid(media.name,media.year)

    bestCacheHitScore = 0
    cacheConsulted = False

    # plexhash search vector
    plexHashes = []
    score = 100
    
    try:
      for item in media.items:
        for part in item.parts:
          if part.plexHash: plexHashes.append(part.plexHash)
    except:
      try: plexHashes.append(media.plexHash)
      except: pass
        
    for ph in plexHashes: 
      try:
        url = '%s/%s/hash/%s/%s.xml' % (PLEXMOVIE_URL, PLEXMOVIE_BASE, ph[0:2], ph)
        Log("checking plexhash search vector: %s" % url)
        res = XML.ElementFromURL(url, cacheTime=60)
        for match in res.xpath('//match'):
          id       = "tt%s" % match.get('guid')
          imdbName = safe_unicode(match.get('title'))
          imdbYear = safe_unicode(match.get('year'))
          count    = int(match.get('count'))
          pct      = float(match.get('percentage',0))/100
          bonus    = - (PERCENTAGE_BONUS_MAX - int(PERCENTAGE_BONUS_MAX*pct))

          distance = Util.LevenshteinDistance(media.name, imdbName.encode('utf-8'))
          Log("distance: %s" % distance)
          if not bestNameMap.has_key(id) or distance < bestNameDist:
            bestNameMap[id] = imdbName
            bestNameDist = distance

          scorePenalty = 0
          scorePenalty += -1*bonus

          # We are going to penalize for distance from name.
          scorePenalty += distance

          if int(imdbYear) > datetime.datetime.now().year:
            Log(imdbName + ' penalizing for future release date')
            scorePenalty += SCORE_THRESHOLD_IGNORE_PENALTY 
  
          # Check to see if the hinted year is different from imdb's year, if so penalize.
          elif media.year and imdbYear and int(media.year) != int(imdbYear):
            Log(imdbName + ' penalizing for hint year and imdb year being different')
            yearDiff = abs(int(media.year)-(int(imdbYear)))
            if yearDiff == 1:
              scorePenalty += 5
            elif yearDiff == 2:
              scorePenalty += 10
            else:
              scorePenalty += 15
          # Bonus (or negatively penalize) for year match.
          elif media.year and imdbYear and int(media.year) != int(imdbYear):
            scorePenalty += -5
  
          Log("score penalty (used to determine if google is needed) = %d" % scorePenalty)

          if (score - scorePenalty) > bestCacheHitScore:
            bestCacheHitScore = score - scorePenalty
  
          cacheConsulted = True
          # score at minimum 85 (threshold) since we trust the cache to be at least moderately good
          results.Append(MetadataSearchResult(id = id, name  = imdbName, year = imdbYear, lang  = lang, score = max([ score-scorePenalty, SCORE_THRESHOLD_IGNORE])))
          score = score - 4
      except Exception, e:
        Log("freebase/proxy plexHash lookup failed: %s" % repr(e))

    score = 100

    # title|year search vector
    url = '%s/%s/guid/%s/%s.xml' % (PLEXMOVIE_URL, PLEXMOVIE_BASE, titleyear_guid[0:2], titleyear_guid)
    Log("checking title|year search vector: %s" % url)
    try:
      res = XML.ElementFromURL(url, cacheTime=60)
      for match in res.xpath('//match'):
        id       = "tt%s" % match.get('guid')

        imdbName = safe_unicode(match.get('title'))
        distance = Util.LevenshteinDistance(media.name, imdbName.encode('utf-8'))
        Log("distance: %s" % distance)
        if not bestNameMap.has_key(id) or distance < bestNameDist:
          bestNameMap[id] = imdbName
          bestNameDist = distance
          
        imdbYear = safe_unicode(match.get('year'))
        count    = int(match.get('count'))
        pct      = float(match.get('percentage',0))/100
        bonus    = - (PERCENTAGE_BONUS_MAX - int(PERCENTAGE_BONUS_MAX*pct))

        scorePenalty = 0
        scorePenalty += -1*bonus

        if int(imdbYear) > datetime.datetime.now().year:
          Log(imdbName + ' penalizing for future release date')
          scorePenalty += SCORE_THRESHOLD_IGNORE_PENALTY 

        # Check to see if the hinted year is different from imdb's year, if so penalize.
        elif media.year and imdbYear and int(media.year) != int(imdbYear):
          Log(imdbName + ' penalizing for hint year and imdb year being different')
          yearDiff = abs(int(media.year)-(int(imdbYear)))
          if yearDiff == 1:
            scorePenalty += 5
          elif yearDiff == 2:
            scorePenalty += 10
          else:
            scorePenalty += 15
        # Bonus (or negatively penalize) for year match.
        elif media.year and imdbYear and int(media.year) != int(imdbYear):
          scorePenalty += -5

        Log("score penalty (used to determine if google is needed) = %d" % scorePenalty)

        if (score - scorePenalty) > bestCacheHitScore:
          bestCacheHitScore = score - scorePenalty

        cacheConsulted = True
        # score at minimum 85 (threshold) since we trust the cache to be at least moderately good
        results.Append(MetadataSearchResult(id = id, name  = imdbName, year = imdbYear, lang  = lang, score = max([ score-scorePenalty, SCORE_THRESHOLD_IGNORE])))
        score = score - 4
    except Exception, e:
      Log("freebase/proxy guid lookup failed: %s" % repr(e))

    doGoogleSearch = False
    if len(results) == 0 or bestCacheHitScore < SCORE_THRESHOLD_IGNORE or manual == True or (bestCacheHitScore < 100 and len(results) == 1):
      doGoogleSearch = True

    Log("PLEXMOVIE INFO RETRIEVAL: FINDBYID: %s CACHE: %s SEARCH_ENGINE: %s" % (findByIdCalled, cacheConsulted, doGoogleSearch))

    if doGoogleSearch:
      normalizedName = String.StripDiacritics(media.name)
      GOOGLE_JSON_QUOTES = GOOGLE_JSON_URL % (self.getPublicIP(), String.Quote('"' + normalizedName + searchYear + '"', usePlus=True)) + '+site:imdb.com'
      GOOGLE_JSON_NOQUOTES = GOOGLE_JSON_URL % (self.getPublicIP(), String.Quote(normalizedName + searchYear, usePlus=True)) + '+site:imdb.com'
      GOOGLE_JSON_NOSITE = GOOGLE_JSON_URL % (self.getPublicIP(), String.Quote(normalizedName + searchYear, usePlus=True)) + '+imdb.com'
      
      subsequentSearchPenalty = 0

      notMovies = {}
      
      for s in [GOOGLE_JSON_QUOTES, GOOGLE_JSON_NOQUOTES]:
        if s == GOOGLE_JSON_QUOTES and (media.name.count(' ') == 0 or media.name.count('&') > 0 or media.name.count(' and ') > 0):
          # no reason to run this test, plus it screwed up some searches
          continue 
          
        subsequentSearchPenalty += 1
  
        # Check to see if we need to bother running the subsequent searches
        Log("We have %d results" % len(results))
        if len(results) < 3 or manual == True:
          score = 99
          
          # Make sure we have results and normalize them.
          jsonObj = self.getGoogleResults(s)
            
          # Now walk through the results and gather information from title/url
          considerations = []
          for r in jsonObj:
            
            # Get data.
            url = safe_unicode(r['unescapedUrl'])
            title = safe_unicode(r['titleNoFormatting'])

            titleInfo = parseIMDBTitle(title,url)
            if titleInfo is None:
              # Doesn't match, let's skip it.
              Log("Skipping strange title: " + title + " with URL " + url)
              continue

            imdbName = titleInfo['title']
            imdbYear = titleInfo['year']
            imdbId   = titleInfo['imdbId']

            if titleInfo['type'] != 'movie':
              notMovies[imdbId] = True
              Log("Title does not look like a movie: " + title + " : " + url)
              continue

            Log("Using [%s (%s)] derived from [%s] (url=%s)" % (imdbName, imdbYear, title, url))
              
            scorePenalty = 0
            url = r['unescapedUrl'].lower().replace('us.vdc','www').replace('title?','title/tt') #massage some of the weird url's google has

            (uscheme, uhost, upath, uparams, uquery, ufragment) = urlparse.urlparse(url)
            # strip trailing and leading slashes
            upath     = re.sub(r"/+$","",upath)
            upath     = re.sub(r"^/+","",upath)
            splitUrl  = upath.split("/")

            if splitUrl[-1] != imdbId:
              # This is the case where it is not just a link to the main imdb title page, but to a subpage. 
              # In some odd cases, google is a bit off so let's include these with lower scores "just in case".
              #
              Log(imdbName + " penalizing for not having imdb at the end of url")
              scorePenalty += 10
              del splitUrl[-1]

            if splitUrl[0] != 'title':
              # if the first part of the url is not the /title/... part, then
              # rank this down (eg www.imdb.com/r/tt_header_moreatpro/title/...)
              Log(imdbName + " penalizing for not starting with title")
              scorePenalty += 10

            if splitUrl[0] == 'r':
              Log(imdbName + " wierd redirect url skipping")
              continue
     
            for urlPart in reversed(splitUrl):  
              if urlPart == imdbId:
                break
              Log(imdbName + " penalizing for not at imdbid in url yet")
              scorePenalty += 5
  
            id = imdbId
            if id.count('+') > 0:
              # Penalizing for abnormal tt link.
              scorePenalty += 10
            try:
              # Keep the closest name around.
              distance = Util.LevenshteinDistance(media.name, imdbName.encode('utf-8'))
              Log("distance: %s" % distance)
              if not bestNameMap.has_key(id) or distance <= bestNameDist:
                bestNameMap[id] = imdbName
                bestNameDist = distance
              
              # Don't process for the same ID more than once.
              if idMap.has_key(id):
                continue
                
              # Check to see if the item's release year is in the future, if so penalize.
              if imdbYear > datetime.datetime.now().year:
                Log(imdbName + ' penalizing for future release date')
                scorePenalty += SCORE_THRESHOLD_IGNORE_PENALTY 
            
              # Check to see if the hinted year is different from imdb's year, if so penalize.
              elif media.year and imdbYear and int(media.year) != int(imdbYear): 
                Log(imdbName + ' penalizing for hint year and imdb year being different')
                yearDiff = abs(int(media.year)-(int(imdbYear)))
                if yearDiff == 1:
                  scorePenalty += 5
                elif yearDiff == 2:
                  scorePenalty += 10
                else:
                  scorePenalty += 15
                  
              # Bonus (or negatively penalize) for year match.
              elif media.year and imdbYear and int(media.year) != int(imdbYear): 
                Log(imdbName + ' bonus for matching year')
                scorePenalty += -5
              
              # Sanity check to make sure we have SOME common substring.
              longestCommonSubstring = len(Util.LongestCommonSubstring(media.name.lower(), imdbName.lower()))
              
              # If we don't have at least 10% in common, then penalize below the 80 point threshold
              if (float(longestCommonSubstring) / len(media.name)) < SCORE_THRESHOLD_IGNORE_PCT: 
                Log(imdbName + ' terrible subtring match. skipping')
                scorePenalty += SCORE_THRESHOLD_IGNORE_PENALTY 
              
              # Finally, add the result.
              idMap[id] = True
              Log("score = %d" % (score - scorePenalty - subsequentSearchPenalty))
              titleInfo['score'] = score - scorePenalty - subsequentSearchPenalty
              considerations.append( titleInfo )
            except:
              Log('Exception processing IMDB Result')
              pass
            
            for c in considerations:
              if notMovies.has_key(c['imdbId']):
                Log("IMDBID %s was marked at one point as not a movie. skipping" % c['imdbId'])
                continue

              results.Append(MetadataSearchResult(id = c['imdbId'], name  = c['title'], year = c['year'], lang  = lang, score = c['score']))
           
            # Each search entry is worth less, but we subtract even if we don't use the entry...might need some thought.
            score = score - 4 
    
    ## end giant google block
      
    results.Sort('score', descending=True)
    
    # Finally, de-dupe the results.
    toWhack = []
    resultMap = {}
    for result in results:
      if not resultMap.has_key(result.id):
        resultMap[result.id] = True
      else:
        toWhack.append(result)
        
    for dupe in toWhack:
      results.Remove(dupe)

    # Make sure we're using the closest names.
    for result in results:
      Log("id=%s score=%s -> Best name being changed from %s to %s" % (result.id, result.score, result.name, bestNameMap[result.id]))
      result.name = bestNameMap[result.id]
      
  def update(self, metadata, media, lang):

    # Set the title. FIXME, this won't work after a queued restart.
    # Only do this once, otherwise we'll pull new names that get edited 
    # out of the database.
    #
    if media and metadata.title is None:
      metadata.title = media.title

    # Hit our repository.
    guid = re.findall('tt([0-9]+)', metadata.guid)[0]
    url = '%s/%s/%s/%s.xml' % (FREEBASE_URL, FREEBASE_BASE, guid[-2:], guid)

    try:
      movie = XML.ElementFromURL(url, cacheTime=3600)

      # Runtime.
      if int(movie.get('runtime')) > 0:
        metadata.duration = int(movie.get('runtime')) * 60 * 1000

      # Genres.
      metadata.genres.clear()
      genreMap = {}
      
      for genre in movie.xpath('genre'):
        id = genre.get('id')
        genreLang = genre.get('lang')
        genreName = genre.get('genre')
        
        if not genreMap.has_key(id) and genreLang in ('en', lang):
          genreMap[id] = [genreLang, genreName]
          
        elif genreMap.has_key(id) and genreLang == lang:
          genreMap[id] = [genreLang, genreName]
        
      keys = genreMap.keys()
      keys.sort()
      for id in keys:
        metadata.genres.add(genreMap[id][1])

      # Directors.
      metadata.directors.clear()
      for director in movie.xpath('director'):
        metadata.directors.add(director.get('name'))
        
      # Writers.
      metadata.writers.clear()
      for writer in movie.xpath('writer'):
        metadata.writers.add(writer.get('name'))
        
      # Actors.
      metadata.roles.clear()
      for movie_role in movie.xpath('actor'):
        role = metadata.roles.new()
        if movie_role.get('role'):
          role.role = movie_role.get('role')
        #role.photo = headshot_url
        role.actor = movie_role.get('name')
          
      # Studio
      if movie.get('company'):
        metadata.studio = movie.get('company')
        
      # Tagline.
      if len(movie.get('tagline')) > 0:
        metadata.tagline = movie.get('tagline')
        
      # Content rating.
      if movie.get('content_rating'):
        metadata.content_rating = movie.get('content_rating')
     
      # Release date.
      if len(movie.get('originally_available_at')) > 0:
        elements = movie.get('originally_available_at').split('-')
        if len(elements) >= 1 and len(elements[0]) == 4:
          metadata.year = int(elements[0])

        if len(elements) == 3:
          metadata.originally_available_at = Datetime.ParseDate(movie.get('originally_available_at')).date()
          
      # Country.
      try:
        metadata.countries.clear()
        if movie.get('country'):
          country = movie.get('country')
          country = country.replace('United States of America', 'USA')
          metadata.countries.add(country)
      except:
        pass
      
    except:
      print "Error obtaining Plex movie data for", guid

    m = re.search('(tt[0-9]+)', metadata.guid)
    if m and not metadata.year:
      id = m.groups(1)[0]
      (title, year) = self.findById(id)
      metadata.year = int(year)


  def findById(self, id):
    jsonObj = self.getGoogleResults(GOOGLE_JSON_URL % (self.getPublicIP(), id))

    try:
      (title, year) = parseIMDBTitle(jsonObj[0]['titleNoFormatting'],jsonObj[0]['unescapedUrl'])
      return (safe_unicode(title), safe_unicode(year))
    except:
      pass
    
    return (None, None)

def parseIMDBTitle(title, url):

  titleLc = title.lower()

  result = {
    'title':  None,
    'year':   None,
    'type':   'movie',
    'imdbId': None,
  }

  try:
    (scheme, host, path, params, query, fragment) = urlparse.urlparse(url)
    path      = re.sub(r"/+$","",path)
    pathParts = path.split("/")
    lastPathPart = pathParts[-1]

    if host.count('imdb.') == 0:
      ## imdb is not in the server.. bail
      return None

    if lastPathPart == 'quotes':
      ## titles on these parse fine but are almost
      ## always wrong
      return None

    if lastPathPart == 'videogallery':
      ## titles on these parse fine but are almost
      ## always wrong
      return None

    # parse the imdbId
    m = re.search('/(tt[0-9]+)/?', path)
    imdbId = m.groups(1)[0]
    result['imdbId'] = imdbId

    ## hints in the title
    if titleLc.count("(tv series") > 0:
      result['type'] = 'tvseries'
    elif titleLc.endswith("episode list"):
      result['type'] = 'tvseries'
    elif titleLc.count("(tv episode") > 0:
      result['type'] = 'tvepisode'
    elif titleLc.count("(vg)") > 0:
      result['type'] = 'videogame'
    elif titleLc.count("(video game") > 0:
      result['type'] = 'videogame'

    # NOTE: it seems that titles of the form
    # (TV 2008) are made for TV movies and not
    # regular TV series... I think we should
    # let these through as "movies" as it includes
    # stand up commedians, concerts, etc

    # NOTE: titles of the form (Video 2009) seem
    # to be straight to video/dvd releases
    # these should also be kept intact
  
    # hints in the url
    if lastPathPart == 'episodes':
      result['type'] = 'tvseries'

    # Parse out title, year, and extra.
    titleRx = '(.*) \(([^0-9]+ )?([0-9]+)(/.*)?.*?\).*'
    m = re.match(titleRx, title)
    if m:
      # A bit more processing for the name.
      result['title'] = cleanupIMDBName(m.groups()[0])
      result['year'] = int(m.groups()[2])
      
    else:
      longTitleRx = '(.*\.\.\.)'
      m = re.match(longTitleRx, title)
      if m:
        result['title'] = cleanupIMDBName(m.groups(1)[0])
        result['year']  = None

    if result['title'] is None:
      return None

    return result
  except:
    return None
 
def cleanupIMDBName(s):
  imdbName = re.sub('^[iI][mM][dD][bB][ ]*:[ ]*', '', s)
  imdbName = re.sub('^details - ', '', imdbName)
  imdbName = re.sub('(.*:: )+', '', imdbName)
  imdbName = HTML.ElementFromString(imdbName).text

  if imdbName:
    if imdbName[0] == '"' and imdbName[-1] == '"':
      imdbName = imdbName[1:-1]
    return imdbName

  return None

def safe_unicode(s,encoding='utf-8'):
  if s is None:
    return None
  if isinstance(s, basestring):
    if isinstance(s, types.UnicodeType):
      return s
    else:
      return s.decode(encoding)
  else:
    return str(s).decode(encoding)
