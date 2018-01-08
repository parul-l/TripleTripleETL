class S3andNBAStatsPostgresETL(object):
    def __init__(
        self,
        s3_etl,
        playbyplay_etl,
        boxscore_traditional_etl,
        boxscore_player_tracking_etl
    ):
        self.s3_etl = s3_etl
        self.playbyplay_etl = playbyplay_etl
        self.boxscore_traditional_etl = boxscore_traditional_etl
        self.boxscore_player_tracking_etl = boxscore_player_tracking_etl

    def run(self):
        self.s3_etl.run()
       
        # update game_id and season from s3 data
        # for inputs to nbastats data
        nbastats_etls = [
            self.playbyplay_etl,
            self.boxscore_traditional_etl,
            self.boxscore_player_tracking_etl
        ]

        for etl in nbastats_etls:
            etl.params['GameID'] = self.s3_etl.game_id
            etl.params['season'] = self.s3_etl.season

        self.playbyplay_etl.run()
        self.boxscore_traditional_etl.run()
        self.boxscore_player_tracking_etl.run()

 
