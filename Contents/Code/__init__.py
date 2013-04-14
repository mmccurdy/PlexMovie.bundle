import re, time, unicodedata, hashlib, urlparse, types, urllib

# [might want to look into language/country stuff at some point] 
# param info here: http://code.google.com/apis/ajaxsearch/documentation/reference.html
#
GOOGLE_JSON_URL = 'http://ajax.googleapis.com/ajax/services/search/web?v=1.0&userip=%s&rsz=large&q=%s'
FREEBASE_URL    = 'http://192.168.1.116'
FREEBASE_BASE   = 'movies'
PLEXMOVIE_URL   = 'http://192.168.1.22'
PLEXMOVIE_BASE  = 'movie'

MPDB_ROOT = 'http://movieposterdb.plexapp.com'
MPDB_JSON = MPDB_ROOT + '/1/request.json?imdb_id=%s&api_key=p13x2&secret=%s&width=720&thumb_width=100'
MPDB_SECRET = 'e3c77873abc4866d9e28277a9114c60c'

# PlexMovie tunables.
INITIAL_SCORE = 100 # Starting value for score before deductions are taken.
PERCENTAGE_PENALTY_MAX = 20 # Maximum amount to penalize matches with low percentages.
COUNT_PENALTY_THRESHOLD = 500 # Items with less than this value are penalized on a scale of 0 to COUNT_PENALTY_MAX.
COUNT_PENALTY_MAX = 10 # Maximum amount to penalize matches with low counts.
FUTURE_RELEASE_DATE_PENALTY = 10 # How much to penalize movies whose release dates are in the future.
YEAR_PENALTY_MAX = 10 # Maximum amount to penalize for mismatched years.
GOOD_SCORE = 98 # Score required to short-circuit matching and stop searching.
SEARCH_RESULT_PERCENTAGE_THRESHOLD = 80 # Minimum 'percentage' value considered credible for PlexMovie results. 

# Google fallback tunables.
SCORE_THRESHOLD_IGNORE = 85
SCORE_THRESHOLD_IGNORE_PENALTY = 100 - SCORE_THRESHOLD_IGNORE
SCORE_THRESHOLD_IGNORE_PCT = float(SCORE_THRESHOLD_IGNORE_PENALTY)/100
PERCENTAGE_BONUS_MAX = 20

def Start():
  HTTP.CacheTime = CACHE_1WEEK
  
class PlexMovieAgent(Agent.Movies):
  name = 'Freebase'
  languages = [Locale.Language.English, Locale.Language.Swedish, Locale.Language.French, 
               Locale.Language.Spanish, Locale.Language.Dutch, Locale.Language.German, 
               Locale.Language.Italian, Locale.Language.Danish]

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
      else:
        if jsonObj['responseStatus'] != 200:
          Log("Something went wrong: %s" % jsonObj['responseDetails'])
    except:
      Log("Exception obtaining result from Google.")
    
    return []


  def getPlexMovieResults(self, media, matches, search_type='hash', plex_hash=''):
    if search_type is 'hash':
      url = '%s/%s/hash/%s/%s.xml' % (PLEXMOVIE_URL, PLEXMOVIE_BASE, plex_hash[0:2], plex_hash)
    else:
      titleyear_guid = self.titleyear_guid(media.name,media.year)
      url = '%s/%s/guid/%s/%s.xml' % (PLEXMOVIE_URL, PLEXMOVIE_BASE, titleyear_guid[0:2], titleyear_guid)

    try:
      Log("checking %s search vector: %s" % (search_type, url))
      res = XML.ElementFromURL(url, cacheTime=CACHE_1WEEK, headers={'Accept-Encoding':'gzip'})
      
      for match in res.xpath('//match'):
        id    = "tt%s" % match.get('guid')
        name  = safe_unicode(match.get('title'))
        year  = safe_unicode(match.get('year'))
        count = int(match.get('count'))
        pct   = int(match.get('percentage', 0))
        dist  = Util.LevenshteinDistance(media.name, name.encode('utf-8'))
        
        # Intialize.
        if not matches.has_key(id):
          matches[id] = [1000, '', None, 0, 0, 0]
          
        # Tally.
        vector = matches[id]
        vector[3] = vector[3] + pct
        vector[4] = vector[4] + count
          
        # See if a better name.
        if dist < vector[0]:
          vector[0] = dist
          vector[1] = name
          vector[2] = year

    except Exception, e:
      Log("freebase/proxy %s lookup failed: %s" % (search_type, str(e)))


  def scoreResults(self, media, matches):

    for key in matches.keys():
      match = matches[key]

      dist = match[0]
      name = match[1]
      year = match[2]
      total_pct = match[3]
      total_cnt = match[4]
      
      # Compute score penalty for percentage/count.
      score_penalty = (100-total_pct) * (PERCENTAGE_PENALTY_MAX/100)
      if total_cnt < COUNT_PENALTY_THRESHOLD:
        score_penalty += (COUNT_PENALTY_THRESHOLD-total_cnt)/COUNT_PENALTY_THRESHOLD * COUNT_PENALTY_MAX
      
      # Year penalty/bonus.
      if int(year) > Datetime.Now().year:
        score_penalty += FUTURE_RELEASE_DATE_PENALTY

      if media.year and year:
        per_year_penalty = int(YEAR_PENALTY_MAX / 3)
        year_delta = abs(int(media.year)-(int(year)))
        if year_delta > 3:
          score_penalty += YEAR_PENALTY_MAX
        else:
          score_penalty += year_delta * per_year_penalty

      # Store the final score in the result vector.
      matches[key][5] = INITIAL_SCORE - dist - score_penalty


  def search(self, results, media, lang, manual=False):
    
    # Keep track of best name.
    lockedNameMap = {}
    idMap = {}
    bestNameMap = {}
    bestNameDist = 1000
    bestHitScore = 0
    continueSearch = True

    # Map GUID to [distance, best name, year, percentage, count, score].
    hash_matches = {}
    title_year_matches = {}    
   
    # TODO: create a plex controlled cache for lookup
    # TODO: by imdbid  -> (title,year)
    
    # See if we're being passed a raw ID.
    findByIdCalled = False
    if media.guid or re.match('t*[0-9]{7}', media.name):
      theGuid = media.guid or media.name 
      if not theGuid.startswith('tt'):
        theGuid = 'tt' + theGuid
      Log('Found an ID, attempting quick match based on: ' + theGuid)
      
      # Add a result for the id found in the passed in guid hint.
      findByIdCalled = True
      (title, year) = self.findById(theGuid)
      if title is not None:
        bestHitScore = 100 # Treat a guid-match as a perfect score
        results.Append(MetadataSearchResult(id=theGuid, name=title, year=year, lang=lang, score=bestHitScore))
        bestNameMap[theGuid] = title
        bestNameDist = Util.LevenshteinDistance(media.name, title)
        continueSearch = False
          
    # Clean up year.
    if media.year:
      searchYear = u' (' + safe_unicode(media.year) + u')'
    else:
      searchYear = u''

    # Grab hash matches first, since a perfect score based on hash is almost certainly correct.
    # Build plex hash list and search each one.
    if manual or continueSearch:
      plexHashes = []
      try:
        for item in media.items:
          for part in item.parts:
            if part.hash: plexHashes.append(part.hash)
      except:
        try: plexHashes.append(media.hash)
        except: pass

      for plex_hash in plexHashes:
        self.getPlexMovieResults(media, hash_matches, search_type='hash', plex_hash=plex_hash)

      self.scoreResults(media, hash_matches)
      
      Log('---- HASH RESULTS MAP ----')
      Log(str(hash_matches))
      
      # Add scored hash results to search results.
      for key in hash_matches.keys():
        match = hash_matches[key]
        if int(match[3]) >= SEARCH_RESULT_PERCENTAGE_THRESHOLD and not manual:
          best_name, year = get_best_name_and_year(key[2:], lang, match[1], match[2], lockedNameMap)
          Log("Adding hash match: %s (%s) score=%d" % (best_name, year, match[5]))
          results.Append(MetadataSearchResult(id = key, name  = best_name, year = year, lang  = lang, score = match[5]))
          if bestHitScore < match[5]:
            bestHitScore = match[5]
        else:
          Log("Skipping hash match (doesn\'t meet percentage threshold): %s (%s) percentage=%d" % (match[1], match[2], match[3]))

      if bestHitScore >= GOOD_SCORE:
        Log('Found perfect match with plex hash query.')
        continueSearch = False

    # Grab title/year matches.
    if manual or continueSearch:
      self.getPlexMovieResults(media, title_year_matches, search_type='title/year')
      self.scoreResults(media, title_year_matches)

      Log('---- TITLE_YEAR RESULTS MAP ----')
      Log(str(title_year_matches))

      # Add scored title year results to search results.
      for key in title_year_matches.keys():
        match = title_year_matches[key]
        if int(match[3]) >= SEARCH_RESULT_PERCENTAGE_THRESHOLD and not manual:
          best_name, year = get_best_name_and_year(key[2:], lang, match[1], match[2], lockedNameMap)
          Log("Adding title_year match: %s (%s) score=%d" % (best_name, year, match[5]))
          results.Append(MetadataSearchResult(id = key, name  = best_name, year = year, lang  = lang, score = match[5]))
          if bestHitScore < match[5]:
            bestHitScore = match[5]
        else:
          Log("Skipping title/year match (doesn\'t meet percentage threshold): %s (%s) percentage=%d" % (match[1], match[2], match[3]))

      if bestHitScore >= GOOD_SCORE:
        Log('Found perfect match with title/year query.')
        continueSearch = False

    # Google fallback search starts here.
    if manual or continueSearch:
      # Try to strip diacriticals, but otherwise use the UTF-8.
      normalizedName = String.StripDiacritics(media.name)
      if len(normalizedName) == 0:
        normalizedName = media.name
        
      GOOGLE_JSON_QUOTES = GOOGLE_JSON_URL % (self.getPublicIP(), String.Quote(('"' + normalizedName + searchYear + '"').encode('utf-8'), usePlus=True)) + '+site:imdb.com'
      GOOGLE_JSON_NOQUOTES = GOOGLE_JSON_URL % (self.getPublicIP(), String.Quote((normalizedName + searchYear).encode('utf-8'), usePlus=True)) + '+site:imdb.com'
      GOOGLE_JSON_NOSITE = GOOGLE_JSON_URL % (self.getPublicIP(), String.Quote((normalizedName + searchYear).encode('utf-8'), usePlus=True)) + '+imdb.com'
      
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
                if distance <= bestNameDist:
                  bestNameDist = distance
              
              # Don't process for the same ID more than once.
              if idMap.has_key(id):
                continue
                
              # Check to see if the item's release year is in the future, if so penalize.
              if imdbYear > Datetime.Now().year:
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
      if not lockedNameMap.has_key(result.id) and bestNameMap.has_key(result.id):
        Log("id=%s score=%s -> Best name being changed from %s to %s" % (result.id, result.score, result.name, bestNameMap[result.id]))
        result.name = bestNameMap[result.id]
        
    # Augment with art.
    if manual == True:
      for result in results[0:3]:
        try: 
          id = re.findall('(tt[0-9]+)', result.id)[0]
          imdb_code = id.lstrip('t0')
          secret = Hash.MD5( ''.join([MPDB_SECRET, imdb_code]))[10:22]
          queryJSON = JSON.ObjectFromURL(MPDB_JSON % (imdb_code, secret), cacheTime=10)
          if not queryJSON.has_key('errors') and queryJSON.has_key('posters'):
            thumb_url = MPDB_ROOT + '/' + queryJSON['posters'][0]['thumbnail_location']
            result.thumb = thumb_url
        except:
          pass
          
      
  def update(self, metadata, media, lang):

    # Set the title. Only do this once, otherwise we'll pull new names 
    # that get edited out of the database.
    #
    setTitle = False
    if media and metadata.title is None:
      setTitle = True
      metadata.title = media.title

    # Hit our repository.
    guid = re.findall('tt([0-9]+)', metadata.guid)[0]
    url = '%s/%s/%s/%s.xml' % (FREEBASE_URL, FREEBASE_BASE, guid[-2:], guid)

    try:
      movie = XML.ElementFromURL(url, cacheTime=CACHE_1WEEK, headers={'Accept-Encoding':'gzip'})

      # Title.
      if not setTitle:
        d = {}
        name,year = get_best_name_and_year(guid, lang, None, None, d)
        if name is not None:
          metadata.title = name

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
      # We already tried Freebase above, so go directly to Google
      (title, year) = self.findById(id, skipFreebase=True)
      if year:
        metadata.year = int(year)


  def findById(self, id, skipFreebase=False):
    title = None
    year = None

    if not skipFreebase:
      # Try Freebase first, since spamming Google will easily get us blocked
      url = '%s/%s/%s/%s.xml' % (FREEBASE_URL, FREEBASE_BASE, id[-2:], id[2:])

      try:
        movie = XML.ElementFromURL(url, cacheTime=CACHE_1WEEK, headers={'Accept-Encoding':'gzip'})

        # Title
        if len(movie.get('title')) > 0:
          title = movie.get('title')

        # Year
        if len(movie.get('originally_available_at')) > 0:
          elements = movie.get('originally_available_at').split('-')
          if len(elements) >= 1 and len(elements[0]) == 4:
            year = int(elements[0])
      except:
        pass

    if not title or not year:
      # No dice, hit up Google
      jsonObj = self.getGoogleResults(GOOGLE_JSON_URL % (self.getPublicIP(), id))

      try:
        titleInfo = parseIMDBTitle(jsonObj[0]['titleNoFormatting'],jsonObj[0]['unescapedUrl'])
        title = titleInfo['title']
        year = titleInfo['year']
      except:
        pass

    if title and year:
      return (safe_unicode(title), safe_unicode(year))
    else:
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
  
def get_best_name_and_year(guid, lang, fallback, fallback_year, best_name_map):
  url = '%s/%s/%s/%s.xml' % (FREEBASE_URL, FREEBASE_BASE, guid[-2:], guid)
  ret = (fallback, fallback_year)
  
  try:
    movie = XML.ElementFromURL(url, cacheTime=CACHE_1WEEK, headers={'Accept-Encoding':'gzip'})
    
    movieEl = movie.xpath('//movie')[0]
    if movieEl.get('originally_available_at'):
      fallback_year = int(movieEl.get('originally_available_at').split('-')[0])

    lang_match = False
    if Prefs['title']:
      for movie in movie.xpath('//title'):
        if lang == movie.get('lang'):
          ret = (movie.get('title'), fallback_year)
          lang_match = True

    # Default to the English title.
    if not lang_match:
      ret = (movieEl.get('title'), fallback_year)
    
    # Note that we returned a pristine name.
    best_name_map['tt'+guid] = True
    return ret
      
  except:
    Log("Error getting best name.")

  return ret