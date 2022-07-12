def full_feature_movies_check(movie_soup):
    #checks if the current movie is full feature movie or not:
    #TV Series, Short, Video Game, Video short, Video, TV Movie, TV Mini-Series, TV Series short, TV Special - not
    #checks if the current movie have been already released
    if '(TV Series' in movie_soup.text:
        return False
    elif '(Short)' in movie_soup.text:
        return False
    elif '(Video Game)' in movie_soup.text:
        return False
    elif '(Video short)' in movie_soup.text:
        return False
    elif '(Video)' in movie_soup.text:
        return False
    elif '(TV Movie)' in movie_soup.text:
        return False
    elif '(TV Mini-Series' in movie_soup.text:
        return False
    elif '(TV Series short)' in movie_soup.text:
        return False
    elif '(TV Special)' in movie_soup.text:
        return False
    elif '(TV Short)' in movie_soup.text:
        return False
    elif '(Documentary' in movie_soup.text:
        return False
    elif movie_soup.find('a', attrs = {'class': 'in_production'}):
        return False

    return True


def get_actor_name_by_url(actor_url):
    response = requests.get(actor_url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text)
        try:
            name = soup.find('span', attrs={'class': 'itemprop'}).text
        except:
            name = None
    return name


async def fetch_sem(session, url, sem):
    async with sem:
        async with session.get(url) as response:
            return await response.text()


def get_soup(url):
    response = requests.get(url)
    return BeautifulSoup(response.text)


def get_actor_name(soup):
    try:
        name = soup.find('span', attrs={'class': 'itemprop'}).text
    except:
        name = None
    return name


async def get_movie_distance_prev_week(actor_start_url, actor_end_url, num_of_actors_limit=None, num_of_movies_limit=None, depth=3):
    actor_start_url = actor_start_url.replace('https://www.', 'https://')
    actor_end_url = actor_end_url.replace('https://www.', 'https://')
    actor_end = get_actor_name_by_url(actor_end_url)
    actor_start_name = get_actor_name_by_url(actor_start_url)
    actors_dict = {}
    distance = 1
    actors_to_check = [(actor_start_name, actor_start_url)]
    while distance < depth:
        print('Distance checking:', distance)
        actors = []
        print('Need to check', len(actors_to_check), 'actors')
        i = 1
        for actor in actors_to_check:
            print(i, 'Checking:', actor)
            i += 1
            one_distance_actors = []
            response = requests.get(actor[1])
            soup = BeautifulSoup(response.text)
            movies = get_movies_by_actor_soup(soup, num_of_movies_limit)
            sem = asyncio.Semaphore(90)
            try:
                async with aiohttp.ClientSession() as session:
                    coroutines = [fetch_sem(session, movie[1], sem) for movie in movies]
                    actor_responses = await asyncio.gather(*coroutines)
            except:
                actor_responses = []
                print('  Error occured while getting one distance connected actors')
            for response in actor_responses:
                soup = BeautifulSoup(response)
                current_movie_actors = get_actors_by_movie_soup(soup, num_of_actors_limit)
                one_distance_actors += current_movie_actors
            current_actors = list(set(one_distance_actors))
            #print('  ', actor[0], 'is connected with', len(current_actors), 'other actors')
            actors_dict.update({_actor[0]: _actor[1] for _actor in current_actors})
            if actor_end in actors_dict:
                return distance
            actors += current_actors
        distance += 1
        actors_to_check = actors

    if actor_end in actors_dict:
        return distance
    else:
        return -1
