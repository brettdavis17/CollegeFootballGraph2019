import os
import json
from py2neo import Graph
import pandas as pd


# Confidential authentication details
scheme = os.environ.get('SCHEME')
host = os.environ.get('HOST')
user = os.environ.get('USER')
password = os.environ.get('PASSWORD')

#Connect to graph
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

#Access data files for load
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

with open('data/plays/week1.json') as week_1_file:
    week_1_plays = json.load(week_1_file)

with open('data/plays/week2.json') as week_2_file:
    week_2_plays = json.load(week_2_file)

with open('data/plays/week3.json') as week_3_file:
    week_3_plays = json.load(week_3_file)

with open('data/plays/week4.json') as week_4_file:
    week_4_plays = json.load(week_4_file)

with open('data/plays/week5.json') as week_5_file:
    week_5_plays = json.load(week_5_file)

with open('data/plays/week6.json') as week_6_file:
    week_6_plays = json.load(week_6_file)

with open('data/plays/week7.json') as week_7_file:
    week_7_plays = json.load(week_7_file)

with open('data/plays/week8.json') as week_8_file:
    week_8_plays = json.load(week_8_file)

with open('data/plays/week9.json') as week_9_file:
    week_9_plays = json.load(week_9_file)

with open('data/plays/week10.json') as week_10_file:
    week_10_plays = json.load(week_10_file)

with open('data/plays/week11.json') as week_11_file:
    week_11_plays = json.load(week_11_file)

with open('data/plays/week12.json') as week_12_file:
    week_12_plays = json.load(week_12_file)

with open('data/plays/week13.json') as week_13_file:
    week_13_plays = json.load(week_13_file)

with open('data/plays/week14.json') as week_14_file:
    week_14_plays = json.load(week_14_file)

with open('data/plays/week15.json') as week_15_file:
    week_15_plays = json.load(week_15_file)

#There are too many plays, so I'm loading them in weekly batches
plays_list = [
    week_1_plays,
    week_2_plays,
    week_3_plays,
    week_4_plays,
    week_5_plays,
    week_6_plays,
    week_7_plays,
    week_8_plays,
    week_9_plays,
    week_10_plays,
    week_11_plays,
    week_12_plays,
    week_13_plays,
    week_14_plays,
    week_15_plays
]

#varable for loading conference nodes
conferences_cypher = '''
    WITH $json as data
    UNWIND data as c
    MERGE (conference:Conference {conferenceId:c.id})
    SET conference.name = c.name,
        conference.shortName = c.short_name,
        conference.abbreviation = c.abbreviation
'''

#variable for loading team nodes
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

#variable for loading venues
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

#variable for loading games, season, and week.  Also establishes relationships.
games_cypher = '''
    WITH $json as data
    UNWIND data as g
    MERGE (season:Season {seasonId: g.season})
    MERGE (week:Week {weekId: toString(g.season) + toString(g.week)})
    MERGE (game:Game {gameId: g.id, venueId: g.venue_id, homeTeam: g.home_team, awayTeam: g.away_team})
    MERGE (season)-[:HAS_WEEK]->(week)
    MERGE (week)-[:HAS_GAME]->(game)
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

#variable for loading drives and establish relationships
drives_cypher = '''
    WITH $json as data
    UNWIND data as d
    MERGE (drive:Drive {driveId: d.id, gameId: d.game_id, offense: d.offense, defense: d.defense})
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

#variable for loading plays and establishing relationships
plays_cypher = '''
    WITH $json as data
    UNWIND data as p
    MERGE (play:Play {
        playId:p.id,
        offense:p.offense,
        defense:p.defense,
        index:p.index,
        driveId:p.drive_id
        }
    )
    MERGE (offense:Team {school:p.offense})
    MERGE (defense:Team {school:p.defense})
    MERGE (drive:Drive {driveId:p.drive_id})
    MERGE (drive)-[:HAS_PLAY]->(play)
    MERGE (offense)-[:ON_OFFENSE_PLAY]->(play)<-[:ON_DEFENSE_PLAY]-(defense)
    SET play.offenseConference = p.offense_conference,
        play.offenseScore = p.offense_score,
        play.defenseConference = p.defense_conference,
        play.defenseScore = p.defense_score,
        play.home = p.home,
        play.away = p.away,
        play.period = p.period,
        play.clockSec = p.clockSec,
        play.clockDisplay = p.clockDisplay,
        play.gameTimeLeftSec = p.gameTimeLeftSec,
        play.yardLine = p.yard_line,
        play.yardsToGoal = p.yards_to_goal,
        play.down = p.down,
        play.distance = p.distance,
        play.yardsGained = p.yards_gained,
        play.playType = p.play_type,
        play.playText = p.play_text,
        play.ppa = p.ppa
'''

#variable establishing relationships between teams and their conference
teams_conferences_cypher = '''
    MATCH (conference:Conference)
    MATCH (team:Team)
    WHERE team.conference = conference.name
    MERGE (conference)-[:HAS_MEMBER_SCHOOL]->(team)
'''

#variable establishing relationship between a game and the venue hosting it
game_venue_cypher = '''
    MATCH (venue:Venue)
    MATCH (game:Game)
    WHERE venue.venueId = game.venueId
    MERGE (venue)-[:HOSTED_GAME]->(game)
'''

#variable establishing relationship between a team and the venue it uses as home stadium
home_stadium_cypher = '''
    MATCH (venue:Venue)-[:HOSTED_GAME]->(game:Game)-[r:HAS_PARTICIPANT_TEAM]->(team:Team)
    WHERE game.neutralSite = false AND r.homeOrAway = 'home'
    MERGE (team)-[:HAS_HOME_STADIUM]->(venue)
'''

#variable establishing relationship of plays in sequence in a drive
plays_index_cypher = '''
    MATCH (play1:Play)
    MATCH (play2:Play)
    WHERE play1.index = play2.index - 1 AND play1.driveId = play2.driveId
    MERGE (play1)-[:FOLLOWED_BY_PLAY]->(play2)
'''

#load conferences with conferences cypher variable
graph.run(conferences_cypher, json=conferences)

#load teams with teams cypher variable
graph.run(teams_cypher, json=teams)

#load venues with venues cypher variable
graph.run(venues_cypher, json=venues)

#load games with games cypher variable
graph.run(games_cypher, json=games)

#load drives with drives cypher variable
graph.run(drives_cypher, json=drives)

#load teams to conferences relationship with cypher variable
graph.run(teams_conferences_cypher)

#load games to venue relationship with cypher variable
graph.run(game_venue_cypher)

#load teams to home stadium relationship with cypher variable
graph.run(home_stadium_cypher)

#create for loop to execute batch loads of plays
for p in plays_list:

    graph.run(
        plays_cypher,
        json=p
    )

#load followed-by-play relationships with cypher variable
graph.run(plays_index_cypher)
