# CollegeFootballGraph2019
A Neo4j database for the 2019 College Football Season

Data is pulled from a college football data api https://api.collegefootballdata.com/api/docs/?url=/api-docs.json.

The database centers around the Season node for 2019 and from there there are orbiting layers. Week nodes orbit the season node, game nodes orbit the week nodes and drive nodes orbit the game nodes.

There is another network of nodes with teams orbiting conferences.

The team nodes have relationships to the games they participate in as well as the drives they participate in.  A property on their relationship with the drive explains whether they were on offense or defense.
