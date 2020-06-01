import os
import json
import requests
from py2neo import Graph

scheme = os.environ.get('SCHEME')
host = os.environ.get('HOST')
user = os.environ.get('USER')
password = os.environ.get('PASSWORD')

graph = Graph(
    scheme=scheme,
    host=host,
    port=7687,
    secure=True,
    auth=(
        user,
        password
    )
)

def get_conferences():

    url = "https://api.collegefootballdata.com/conferences"

    payload = {}
    headers = {}

    response = requests.request("GET", url, headers=headers, data=payload)

    conferences = json.loads(response.text)

    return conferences

def get_teams():
    url = "https://api.collegefootballdata.com/teams"

    payload = {}
    headers = {}

    response = requests.request("GET", url, headers=headers, data=payload)

    teams = json.loads(response.text)

    return teams

def get_venues():
    url = "https://api.collegefootballdata.com/venues"

    payload = {}
    headers = {}

    response = requests.request("GET", url, headers=headers, data=payload)

    venues = json.loads(response.text)

    return venues

def get_games():

    url = "https://api.collegefootballdata.com/games?year=2019&seasonType=regular"

    payload = {}
    headers = {}

    response = requests.request("GET", url, headers=headers, data=payload)

    games = json.loads(response.text)

    return games

def get_drives():
    url = "https://api.collegefootballdata.com/drives?year=2019&seasonType=regular"

    payload = {}
    headers = {}

    response = requests.request("GET", url, headers=headers, data=payload)

    drives = json.loads(response.text)

    return drives

def get_plays():
    url = "https://api.collegefootballdata.com/plays?year=2019&seasonType=regular"

    payload = {}
    headers = {}

    response = requests.request("GET", url, headers=headers, data=payload)

    games = json.loads(response.text)

    return games

conferences = get_conferences()
teams = get_teams()
venues = get_venues()
games = get_games()
drives = get_drives()
#plays = get_plays()

graph.run('''
    WITH $json as data
    UNWIND data as c
    MERGE (conference:Conference {conferenceId:c.id})
    SET conference.name = c.name,
        conference.shortName = c.short_name,
        conference.abbreviation = c.abbreviation
''', json=conferences)

graph.run('''
    WITH $json as data
    UNWIND data as t
    MERGE (team:Team {teamId:t.id})
    SET team.school = t.school,
        team.mascot = t.mascot,
        team.abbreviation = t.abbreviation,
        team.altName1 = t.alt_name_1,
        team.altName2 = t.alt_name_2,
        team.altName3 = t.alt_name_3,
        team.conference = t.conference,
        team.division = t.division,
        team.color = t.color,
        team.altColor = t.alt_color
''', json=teams)

graph.run('''
    WITH $json as data
    UNWIND data as v
    MERGE (venue:Venue {venueId: v.id})
    SET venue.name = v.name,
        venue.capacity = v.capacity,
        venue.grass = v.grass,
        venue.city = v.city,
        venue.state = v.state,
        venue.zip = v.zip,
        venue.countryCode = v.country_code,
        venue.elevation = toFloat(v.elevation),
        venue.yearConstructed = v.year_constructed,
        venue.dome = v.dome,
        venue.timezone = v.timezone
''', json=venues)

graph.run('''
    WITH $json as data
    UNWIND data as g
    MERGE (season:Season {seasonId: g.season})
    MERGE (week:Week {weekId: toString(g.season) + toString(g.week)})
    MERGE (game:Game {gameId: g.id})
    SET game.season = g.season,
        game.week = g.week,
        game.weekId = toString(g.season) + toString(g.week),
        game.seasonType = g.season_type,
        game.name = g.awayTeam + ' vs ' + g.homeTeam
        game.startDate = datetime(g.start_date),
        game.startTime = g.start_time_tbd,
        game.neutralSite = g.neutral_site,
        game.conferenceGame = g.conference_game,
        game.attendance = g.attendance,
        game.venueId = g.venue_id,
        game.venue = g.venue,
        game.homeId = g.home_id,
        game.homeTeam = g.home_team,
        game.homeConference = g.home_conference,
        game.homePoints = g.home_points,
        game.homeLineScores = g.home_line_scores,
        game.homePostWinProb = toFloat(g.home_post_win_prob),
        game.awayId = g.away_id,
        game.awayTeam = g.away_team,
        game.awayConference = g.away_conference,
        game.awayPoints = g.away_points,
        game.awayLineScores = g.away_line_scores,
        game.awayPostWinProb = toFloat(g.away_post_win_prob),
        game.excitementIndex = toFloat(g.excitement_index),
        week.weekNbr = g.week,
        week.season = g.season
''', json=games)

graph.run('''
    WITH $json as data
    UNWIND data as d
    MERGE (drive:Drive {driveId: toInteger(d.id)})
    SET drive.offense = d.offense,
        drive.offenseConference = d.offense_conference,
        drive.defense = d.defense,
        drive.defenseConference = d.defense_conference,
        drive.gameId = d.game_id,
        drive.scoring = d.scoring,
        drive.startPeriod = d.start_period,
        drive.startYardline = d.start_yardline,
        drive.startYardsToGoal = d.start_yards_to_goal,
        drive.endPeriod = d.end_period,
        drive.endYardline = d.end_yardline,
        drive.endYardsToGoal = d.end_yards_to_goal,
        drive.plays = d.plays,
        drive.yards = d.yards,
        drive.driveResult = d.drive_result
''', json=drives)

graph.run('''
    MATCH (conference:Conference)
    MATCH (team:Team)
    WHERE conference.name = team.conference
    MERGE (conference)-[:HAS_MEMBER_TEAM]->(team)
''')

graph.run('''
    MATCH (season:Season)
    MATCH (week:Week)
    WHERE season.seasonId = week.season
    MERGE (season)-[:HAS_WEEK]->(week)
''')

graph.run('''
    MATCH (week:Week)
    MATCH (game:Game)
    WHERE week.weekId = game.weekId
    MERGE (week)-[:HAS_GAME]->(game)
''')

graph.run('''
    MATCH (game:Game)
    MATCH (home:Team)
    MATCH (away:Team)
    WHERE game.homeTeam = home.school and game.awayTeam = away.school
    MERGE (away)<-[:HAS_PARTICIPANT_TEAM {homeOrAway: 'away'}]-(game)-[:HAS_PARTICIPANT_TEAM {homeOrAway: 'home'}]->(home)
''')

graph.run('''
    MATCH (game:Game)
    MATCH (drive:Drive)
    WHERE game.gameId = drive.gameId
    MERGE (game)-[:HAS_DRIVE]->(drive)
''')

graph.run('''
    MATCH (venue:Venue)
    MATCH (game:Game)
    WHERE venue.venueId = game.venueId
    MERGE (venue)-[:HOSTED_GAME]->(game)
''')

graph.run('''
    MATCH (drive:Drive)
    MATCH (offense:Team)
    MATCH (defense:Team)
    WHERE drive.offense = offense.school and drive.defense = defense.school
    MERGE (offense)-[:ON_OFFENSE_DRIVE]->(drive)<-[:ON_DEFENSE_DRIVE]-(defense)
''')