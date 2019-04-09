from triple_triple_etl.load.postgres.gamepositions2gamepossession_etl import (
    PossessionPostgresETL
)

if __name__ == '__main__':
    game_id = '0021500492'

    etl = PossessionPostgresETL(game_id=game_id)
    etl.run()
