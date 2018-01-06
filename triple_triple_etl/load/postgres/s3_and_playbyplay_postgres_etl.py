from triple_triple_etl.load.postgres.s3_postgres_etl import S3PostgresETL
from triple_triple_etl.load.postgres.nbastats_postgres_etl import NBAStatsPostgresETL
from triple_triple_etl.constants import BASE_URL_PLAY


class S3andPlaybyPlayPostgresETL(object):
    def __init__(
        self,
        s3_etl,
        playbyplay_etl
    ):
        self.s3_etl = s3_etl
        self.playbyplay_etl = playbyplay_etl

    def run(self):
        self.s3_etl.run()
       
        # update game_id and season from s3 data
        # for inputs to playbyplay_etl
        self.playbyplay_etl.params['game_id'] = self.s3_etl.game_id
        self.playbyplay_etl.params['season'] = self.s3_etl.season
        self.playbyplay_etl.run()

 
