import datetime
import pandas as pd
import pyarrow as pa

from triple_triple_etl.log import get_logger


logger = get_logger()


def get_play_by_play(play_data: dict, season: str='2015-16'):
    logger.info('Get play by play data')

    headers = [x.lower() for x in play_data['resultSets'][0]['headers']]
    df = pd.DataFrame(
        data=play_data['resultSets'][0]['rowSet'],
        columns=headers
    )

    # convert timestamp
    df.loc[:, 'wctimestring'] = df.wctimestring.apply(
        lambda x: datetime.datetime.strptime(x, '%I:%M %p').strftime('%H:%M:%S')
    )
    # add season
    df.loc[:, 'season'] = season

    # enforce datatypes
    dtype = {    
        'game_id':'object',
        'eventnum':'int64',
        'eventmsgtype':'int64',
        'eventmsgactiontype':'int64',
        'period':'int64',
        'wctimestring':'object',
        'pctimestring':'object',
        'homedescription':'object',
        'neutraldescription':'object',
        'visitordescription':'object',
        'score':'object',
        'scoremargin':'object',
        'person1type':'float64',
        'player1_id':'int64',
        'player1_name':'object',
        'player1_team_id':'float64',
        'player1_team_city':'object',
        'player1_team_nickname':'object',
        'player1_team_abbreviation':'object',
        'person2type':'int64',
        'player2_id':'int64',
        'player2_name':'object',
        'player2_team_id':'float64',
        'player2_team_city':'object',
        'player2_team_nickname':'object',
        'player2_team_abbreviation':'object',
        'person3type':'int64',
        'player3_id':'int64',
        'player3_name':'object',
        'player3_team_id':'float64',
        'player3_team_city':'object',
        'player3_team_nickname':'object',
        'player3_team_abbreviation':'object'
    }
    df = df.astype(dtype)
    # convert to pyarrow table
    return pa.Table.from_pandas(df, preserve_index=False)
