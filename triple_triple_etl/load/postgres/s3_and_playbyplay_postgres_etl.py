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
        self.playbyplay_etl.params['GameID'] = self.s3_etl.game_id
        self.playbyplay_etl.params['season'] = self.s3_etl.season
        self.playbyplay_etl.run()

 
