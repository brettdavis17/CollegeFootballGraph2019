import os
import json
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

with open('data/conferences.json') as conferences_file:
    conferences = json.load(conferences_file)

with open('data/teams.json') as teams_file:
    teams = json.load(teams_file)

with open('data/games.json') as games_file:
    games = json.load(games_file)

with open('data/drives.json') as drives_file:
    drives = json.load(drives_file)

with open('data/venues.json') as venues_file:
    venues = json.load(venues_file)

conferences_cypher = '''
    WITH $json as data
    UNWIND data as c
    MERGE (conference:Conference {conferenceId:c.id})
    SET conference.name = c.name,
        conference.shortName = c.short_name,
        conference.abbreviation = c.abbreviation
'''

teams_cypher = '''
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
'''

venues_cypher = '''
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
'''

games_cypher = '''
    WITH $json as data
    UNWIND data as g
    MERGE (season:Season {seasonId: g.season})
    MERGE (week:Week {weekId: toString(g.season) + toString(g.week)})
    MERGE (game:Game {gameId: g.id, venueId: g.venue_id, homeTeam: g.home_team, awayTeam: g.away_team})
    MERGE (Season)-[:HAS_WEEK]->(Week)
    MERGE (Week)-[:HAS_GAME]->(Game)
    MERGE (home:Team {school: g.home_team})
    MERGE (away:Team {school: g.away_team})
    MERGE (away)<-[rAway:HAS_PARTICIPANT_TEAM]-(game)-[rHome:HAS_PARTICIPANT_TEAM]->(home)
    SET game.season = g.season,
        game.week = g.week,
        game.weekId = toString(g.season) + toString(g.week),
        game.seasonType = g.season_type,
        game.name = g.away_team + ' vs ' + g.home_team,
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
        week.season = g.season,
        rAway.homeOrAway = 'away',
        rAway.teamScore = g.away_points,
        rAway.opponentScore = g.home_points,
        rHome.homeOrAway = 'home',
        rHome.teamScore = g.home_points,
        rHome.opponentScore = g.away_points
'''

home_stadium_cypher = '''
    MATCH (venue:Venue)-[:HOSTED_GAME]->(game:Game)-[:HAS_PARTICIPANT_TEAM {homeOrAway: 'home'}]->(team:Team)
    WHERE game.isNeutral = false
    MERGE (team)-[:HAS_HOME_STADIUM]->(venue)
'''

drives_cypher = '''
    WITH $json as data
    UNWIND data as d
    MERGE (drive:Drive {driveId: toInteger(d.id), gameId: d.game_id, offense: d.offense, defense: d.defense})
    MERGE (offense:Team {school: d.offense})
    MERGE (defense:Team {school: d.defense})
    MERGE (game:Game {gameId: d.game_id})
    MERGE (game)-[:HAS_DRIVE]->(drive)
    MERGE (offense)-[:ON_OFFENSE_DRIVE]->(drive)<-[:ON_DEFENSE_DRIVE]-(defense)
    SET drive.offenseConference = d.offense_conference,
        drive.defenseConference = d.defense_conference,
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
'''

teams_conferences_cypher = '''
    MATCH (conference:Conference)
    MATCH (team:Team)
    WHERE team.conference = conference.name
    MERGE (conference)-[:HAS_MEMBER_SCHOOL]->(team)
'''

game_venue_cypher = '''
    MATCH (venue:Venue)
    MATCH (game:Game)
    WHERE venue.venueId = game.venueId
    MERGE (venue)-[:HOSTED_GAME]->(game)
'''

graph.run(conferences_cypher, json=conferences)

graph.run(teams_cypher, json=teams)

graph.run(venues_cypher, json=venues)

graph.run(games_cypher, json=games)

graph.run(drives_cypher, json=drives)

graph.run(teams_conferences_cypher)

graph.run(home_stadium_cypher)

graph.run(game_venue_cypher)