from imdb_helper_functions import full_feature_movies_check
from imdb_helper_functions import get_actor_name_by_url
from imdb_helper_functions import fetch_sem
from imdb_helper_functions import get_actor_name
from imdb_helper_functions import get_soup


def get_actors_by_movie_soup(cast_page_soup, num_of_actors_limit=None):
    actors = []
    try:
        table_of_actors = cast_page_soup.find('table', attrs={'class': 'cast_list'}).find_all('tr')
    except:
        table_of_actors = []

    for tr in table_of_actors:
        try:
            td = tr.find_all('td')
            if len(td) > 1:
                name_of_actor = td[1].text.strip()
                url_to_actor_page = 'https://www.imdb.com' + td[1].find('a')['href']
                # url_to_actor_page = urllib.parse.urljoin('https://www.imdb.com/', url_to_actor_page)
                actors.append((name_of_actor, url_to_actor_page))
        except:
            continue

    if num_of_actors_limit:
        return actors[:num_of_actors_limit]
    return actors


def get_movies_by_actor_soup(actor_page_soup, num_of_movies_limit=None):
    movies = []
    try:
        filmography = actor_page_soup.find('div', attrs={'id': 'filmography'})
        table_of_movies_head = filmography.find('div', attrs={'id': 'filmo-head-actor', 'class': 'head'})
        if table_of_movies_head is None:
            table_of_movies_head = filmography.find('div', attrs={'id': 'filmo-head-actress', 'class': 'head'})
        table_of_movies = table_of_movies_head.find_next_sibling().find_all('div')
    except:
        print('Filmography is not finded')
        table_of_movies = []

    for movie in table_of_movies:
        try:
            if 'actor' in movie.attrs.get('id', []) or 'actress' in movie.attrs.get('id', []):
                if full_feature_movies_check(movie):
                    name_of_movie = movie.find('b').find('a').text
                    url_to_movie_page = 'https://www.imdb.com' + movie.find('b').find('a')['href']
                    movies.append((name_of_movie, url_to_movie_page))
        except:
            continue

    if num_of_movies_limit:
        return movies[:num_of_movies_limit]
    return movies


# get_movie_distance function updated
actors_checked = dict()
movies_checked = dict()
async def get_movie_distance(actor_start_url, actor_end_url, num_of_actors_limit=None, num_of_movies_limit=None,
                             depth=3):
    global actors_checked
    global movies_checked

    actor_start_url = actor_start_url.replace('https://www.', 'https://')
    actor_end_url = actor_end_url.replace('https://www.', 'https://')

    actor_end_soup = get_soup(actor_end_url)
    actor_end_name = get_actor_name(actor_end_url)
    actor_end = (actor_end_name, actor_end_url)
    if actor_end not in actors_checked:
        actor_end_movies = get_movies_by_actor_soup(actor_end_soup, num_of_movies_limit)
        actors_checked[actor_end] = actor_end_movies
    else:
        actor_end_movies = actors_checked[actor_end]
    actor_end_movies = set(actor_end_movies)
    distance = 1
    actors_to_check = [(get_actor_name_by_url(actor_start_url), actor_start_url)]

    while distance < depth:
        print(f'Distance checking: {distance}. Need to check {len(actors_to_check)} actors')
        actors = []
        movies = []
        i = 1
        sem = asyncio.Semaphore(100)
        try:
            async with aiohttp.ClientSession() as session:
                for actor in actors_to_check:
                    i += 1
                    if actor not in actors_checked:
                        print(f'{i}. Scraping: {actor}')
                        actor_response = await fetch_sem(session, actor[1], sem)
                        soup = BeautifulSoup(actor_response)
                        current_actor_movies = get_movies_by_actor_soup(soup, num_of_movies_limit)
                        actors_checked[actor] = current_actor_movies
                        print(f'{actor[0]} played in {len(current_actor_movies)} movies')
                        match = actor_end_movies.intersection(set(current_actor_movies))
                        if match:
                            match_movies = [f'{movie[0]} ({movie[1]})' for movie in match]
                            match_movies = ', '.join(match_movies)
                            print(f'MATCH by {actor} via {match_movies}, distance is {distance}')
                            return distance
                    else:
                        current_actor_movies = actors_checked[actor]
                        print(f'{i}. Already scraped: {actor} played in {len(current_actor_movies)} movies')
                        match = actor_end_movies.intersection(set(current_actor_movies))
                        if match:
                            match_movies = [f'{movie[0]} ({movie[1]})' for movie in match]
                            match_movies = ', '.join(match_movies)
                            print(f'MATCH by {actor} via {match_movies}, distance is {distance}')
                            return distance

                    movies += current_actor_movies
                movies = list(set(movies))
                j = 1
            async with aiohttp.ClientSession() as session:
                for movie in movies:
                    j += 1
                    if movie not in movies_checked:
                        print(f'{j} Scraping actors from: {movie}')
                        movie_response = await fetch_sem(session, movie[1], sem)
                        soup = BeautifulSoup(movie_response)
                        current_movie_actors = get_actors_by_movie_soup(soup, num_of_movies_limit)
                        movies_checked[movie] = current_movie_actors
                    else:
                        print(f'{j} Already scraped: {movie}')
                        current_movie_actors = movies_checked[movie]

                    actors += current_movie_actors
        except:
            print('Something goes wrong')
        actors_to_check = list(set(actors))
        distance += 1

    return -1


async def get_movie_descriptions_by_actor_soup(actor_page_soup):
    movies = get_movies_by_actor_soup(actor_page_soup)
    movie_descriptions = []
    async with aiohttp.ClientSession() as session:
        sem = asyncio.Semaphore(90)
        coroutines = [fetch_sem(session, movie[1], sem) for movie in movies]
        responses = await asyncio.gather(*coroutines)
    for response in responses:
        movie_soup = BeautifulSoup(response)
        try:
            summary = movie_soup.find('div', attrs={'class': 'summary_text'})
            if summary:
                movie_description = summary.text.strip()
                try:
                    a = summary.find('a')
                    if a:
                        if a.text == 'See full summary':
                            url = 'https://www.imdb.com' + a['href']
                            soup = get_soup(url)
                            summary_header = soup.find('h4', attrs={'id': 'summaries'})
                            summary = summary_header.find_next_sibling().find('p')
                            movie_description = summary.text
                except:
                    pass
                movie_descriptions.append(movie_description)
        except:
            continue
    return movie_descriptions